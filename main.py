import json
from jsonschema import validate, ValidationError
import asyncio
import requests
import tempfile
import os
import shutil
import time
from openai import AsyncOpenAI
from PyPDF2 import PdfReader
import prompt
import logging
from dotenv import load_dotenv
from datetime import datetime
from utils import *
from src.translate import translate_json

load_dotenv()

api_key = os.environ.get("OPENAI_API_KEY")
client = AsyncOpenAI(api_key=api_key)

logging.basicConfig(filename='error.log', level=logging.ERROR)

async def save_response(results,districts_data,temp_dir):
    inconsistent=[]
    for district, response in results:
        try:
            validate(instance=response, schema=prompt.schema)
            if len(response.get('names_of_crops', [])) != len(response.get('crops_data', {})):
                raise ValidationError("Number of items in 'names_of_crops' does not match the number of crops in 'crops_data'")
        except ValidationError as e:
            inconsistent.append([district,response,str(e)])
        
        response = await remove_empty_crops(response)
            
        os.makedirs("latest", exist_ok=True)
        with open(f"latest/{district}.json", "w") as f:
            json.dump(response, f, ensure_ascii=False, indent=3)

        try:
            translated_response= await translate_json(response)        
            os.makedirs("latest/odia", exist_ok=True)
            with open(f"latest/odia/{district}.json", "w") as f:
                json.dump(translated_response, f, ensure_ascii=False, indent=3)                    
        except Exception as e:
            logging.error(f"Error translating JSON for {district}.json: {e}", exc_info=True)
            print("Cannot translate", district)

        
    if len(inconsistent)>0:
        print("Going again for inconsistent json for",[a[0] for a in inconsistent])
        return await refine_response(inconsistent)                    

    return []
    
async def refine_response(inconsistent):
    tasks = [retry_response(district_data[0], district_data[1], district_data[2]) for district_data in inconsistent]
        
    results = await asyncio.gather(*tasks)
    inconsistent_districts=[]
    for district, response in results:
        counter=0
        try:
            validate(instance=response, schema=prompt.schema)
            if len(response.get('names_of_crops', [])) != len(response.get('crops_data', {})):
                raise ValidationError("Number of items in 'names_of_crops' does not match the number of crops in 'crops_data'")
        except Exception as e:
            counter+=1
            response["ERROR"]="Not getting consistent data."
            inconsistent_districts.append(district)
        
        response = await remove_empty_crops(response)
        
        os.makedirs("latest", exist_ok=True)
        with open(f"latest/{district}.json", "w") as f:
            json.dump(response, f, ensure_ascii=False, indent=3)

        try:
            translated_response=await translate_json(response)
            os.makedirs("latest/odia", exist_ok=True)
            with open(f"latest/odia/{district}.json", "w") as f:
                json.dump(translated_response, f, ensure_ascii=False, indent=3)                    
        except Exception as e:
            logging.error(f"Error translating JSON for {district}.json: {e}", exc_info=True)
            print("Cannot translate", district)
        
    return inconsistent_districts

async def retry_response(district,response,e):
    try:
        date=response['date']
    except:
        pass
    user_prompt=f'''
    I asked you to do this: {prompt.prompt} 
    But this is the response I got: {response}
    Error in your response: {e}
    Improve your response please. Provide only json format and all conditions remain same. Keep date also.
    '''
    try:
        chat_completion = await client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": user_prompt,
                }
            ],
            model="gpt-3.5-turbo-0125",
            response_format={"type": "json_object"},
        )

        response = chat_completion.choices[0].message.content
        response = json.loads(response)
        response['date'] = date
    except Exception as e:
        response['date']=date
        print("Error while retrying response",e)
        
    return district,response

    
async def process_pdf(district_data, temp_dir):
    district_name = district_data['district_name']
    date = district_data['date'].replace('/', '-')
    pdf_link = district_data['link']['english']
    print("Processing data for", district_name)
    pdf_path = await download_pdf(pdf_link, temp_dir)
    c=0
    if pdf_path is None:
        logging.error(f"Error downloading PDF for {district_name}")
        return district_name,{'date':date, "error":"Error in getting the document."}

    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() if page.extract_text() else ""

        final_text = prompt.prompt + text

        chat_completion = await client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": final_text,
                }
            ],
            model="gpt-3.5-turbo-0125",
            response_format={"type": "json_object"},
        )

        response = chat_completion.choices[0].message.content
        response = json.loads(response)
        response['date'] = date

    except Exception as e:
        logging.error(f"Error processing PDF for {district_name}: {e}")
        return district_name, {'date':date,"error":"Error in getting the response."}

    return district_name, response

async def remove_empty_crops(response):
    if 'crops_data' in response:
        for crop, data in list(response['crops_data'].items()):
            if not data.get('advisory'):
                response['crops_data'].pop(crop)
                if crop in response.get('names_of_crops', []):
                    response['names_of_crops'].remove(crop)
    return response

async def main():
    temp_dir = tempfile.mkdtemp()

    try:
        districts_data = scraper()
    except Exception as e:
        logging.error(f"Error getting districts data: {e}")
        print("Error scraping website")

    # move latest to history. 
    try:
        move_json_to_history("latest","history")
    except Exception as e:
        print("error moving latest to history",e)

    #Inititiating tasks
    tasks = [process_pdf(district_data, temp_dir) for district_data in districts_data]
    results = await asyncio.gather(*tasks)

    inconsistent_districts=await save_response(results,districts_data,temp_dir)
    retry=1
    
    # Iterating again on inconsistent districts
    while retry<=3 and len(inconsistent_districts)!=0:
        new_data=[district_data for district_data in districts_data if district_data["district_name"] in inconsistent_districts]
        fallback_tasks = [process_pdf(district_data, temp_dir) for district_data in new_data]
        results = await asyncio.gather(*fallback_tasks)
        inconsistent_districts=await save_response(results,districts_data,temp_dir)
        retry+=1
    
    counter=len(inconsistent_districts)
    total_districts = len(districts_data)

    # Sending update through webhook
    try:
        webhook_url="https://discord.com/api/webhooks/1229418219316445278/Dkj1rrqHZsQ39SoMagqd2xNV4W4HwBA-xnrk6QqAnOrBV0qQ36KpMLf06EPSAgGAZdVf"
        data={
            "disctricts_done":f"{total_districts-counter}",
            "inconsistent_districts":str(counter),
            "inconsistent_districts_name":inconsistent_districts,
            "time":str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        }
        formatted_string = json.dumps(data, indent=4)
        payload={
            "content":str(formatted_string)
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Webhook notification sent successfully.")
    except requests.exceptions.RequestException as e:
        print("Error sending webhook notification:", e)        

    #Saving metadata    
    metadata = f"District_done: {total_districts - counter}, Total_district: {total_districts}"
    with open("meta_data.txt", "w") as meta_file:
        json.dump(metadata, meta_file, indent=4)


    # Removing temp dir
    try:
        shutil.rmtree(temp_dir)
    except Exception as e:
        logging.error(f"Error removing temporary directory: {e}")
    

    return "SUCCESS"


if __name__ == "__main__":
    retries = 3
    retry_delay = 5

    for attempt in range(1, retries + 1):
        try:
            asyncio.run(main())
            print('latest data saved successfully')
            break  # If successful, break out of the retry loop
        except Exception as e:
            logging.error(f"Attempt {attempt}: An error occurred: {e}")
            if attempt < retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("All retry attempts failed. Exiting.")
                raise e
