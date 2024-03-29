import json
from jsonschema import validate, ValidationError
import asyncio
import requests
import tempfile
import os
import shutil
import time
from bs4 import BeautifulSoup
from openai import AsyncOpenAI
from PyPDF2 import PdfReader
import prompt
import logging
from dotenv import load_dotenv

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
            
        with open(f"latest/{district}.json", "w") as f:
            json.dump(response, f, ensure_ascii=False, indent=3)

    if len(inconsistent)>0:
        print("Going again for inconsistent json for",[a[0] for a in inconsistent])
        return await refine_response(inconsistent)                    

    return 0
    
async def refine_response(inconsistent):
    tasks = [retry_response(district_data[0], district_data[1], district_data[2]) for district_data in inconsistent]
        
    results = await asyncio.gather(*tasks)
    for district, response in results:
        counter=0
        try:
            validate(instance=response, schema=prompt.schema)
            if len(response.get('names_of_crops', [])) != len(response.get('crops_data', {})):
                raise ValidationError("Number of items in 'names_of_crops' does not match the number of crops in 'crops_data'")
        except Exception as e:
            counter+=1
            response={"ERROR":"Not getting consistent data."}
            
        with open(f"latest/{district}.json", "w") as f:
            json.dump(response, f, ensure_ascii=False, indent=3)
        
    return counter

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
        print("lol",e)
        
    return district,response

    

async def process_pdf(district_data, temp_dir):
    district_name = district_data['district_name']
    date = district_data['date'].replace('/', '-')
    pdf_link = district_data['link']['english']

    print("Processing data for", district_name)
    pdf_path = download_pdf(pdf_link, temp_dir)
    c=0
    if pdf_path is None:
        logging.error(f"Error downloading PDF for {district_name}")
        return district_name,{'date':'date',"error":"Error in getting the document."}

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
        return district_name, {'date':'date',"error":"Error in getting the response."}

    return district_name, response


def download_pdf(url, temp_dir):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            temp_file = tempfile.NamedTemporaryFile(delete=False, dir=temp_dir, suffix=".pdf")
            with open(temp_file.name, 'wb') as f:
                f.write(response.content)
            return temp_file.name
        else:
            logging.error(f"Failed to download PDF from {url}")
            return None
    except Exception as e:
        logging.error(f"Error downloading PDF: {e}")
        return None


def scraper():
    url = 'https://ouat.ac.in/quick-links/agro-advisory-services/'
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        data = []
        districts = soup.find_all('div', class_='hide1')
        for district in districts:
            district_name = district.get('id')[:-1]
            data_dict = {'district_name': district_name}
            table = district.find('table').find('tbody')
            if len(table.select('tr')) > 0:
                rows = table.select('tr')[0]
            else:
                continue
            columns = rows.find_all('td')
            date = columns[1].text.strip()
            data_dict['date'] = date
            english_link = columns[2].find('a')['href']
            odia_link = columns[3].find('a')['href']
            link_dict = {'english': english_link, 'odia': odia_link}
            data_dict['link'] = link_dict
            data.append(data_dict)

        return data

    except Exception as e:
        logging.error(f"Error scraping website: {e}")
        return []
    

def move_json_to_history(source_dir, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    os.makedirs(source_dir, exist_ok=True)

    for filename in os.listdir(source_dir):
        if filename.endswith(".json"):
            source_path = os.path.join(source_dir, filename)
            with open(source_path, 'r') as json_file:
                data = json.load(json_file)
                date = data.get('date')
                district_name = filename.split('.')[0]
                history_filename = f"{date}_{district_name}.json"
                dest_path = os.path.join(dest_dir, history_filename)
                shutil.move(source_path, dest_path)
                print(f"Moved {filename} to {dest_path}")

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



    tasks = [process_pdf(district_data, temp_dir) for district_data in districts_data]
    results = await asyncio.gather(*tasks)

    counter=await save_response(results,districts_data,temp_dir)
    
    total_districts = len(districts_data)
    metadata = f"District_done: {total_districts - counter}, Total_district: {total_districts}"
    with open("meta_data.txt", "w") as meta_file:
        json.dump(metadata, meta_file, indent=4)

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