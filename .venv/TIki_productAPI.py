import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

## Setting
API_URL = 'https://api.tiki.vn/product-detail/api/v1/products/{product_id}'
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json"}

## Lay ID tu CSV
product_ids = pd.read_csv('/home/long/PycharmProjects/project02-TikiAPI/.venv/products-0-200000(in).csv')['id'].tolist()

## Function to fetch product data
def fetch_product(product_id):
    response = requests.get(API_URL.format(product_id=product_id), headers=headers)
    if response.status_code == 200:
        result = response.json()
        ## Normalize Description
        new_desc = BeautifulSoup(result.get('description', ''), "html.parser").get_text()
        final_desc = ' '.join(new_desc.split())  # Remove extra spaces and newlines
        return {
                'id': result.get('id'),
                'name': result.get('name'),
                'url_key': result.get('url_key'),
                'price': result.get('price'),
                'description': final_desc,
                'image_url': result.get('images', [])
            }
    else:
        return {'error': 'Failed to fetch', 'product_id': product_id}

## Function to save data to JSON file
def save_to_json(data_list, file_index):
    file_name = f'JSON_FILE_{file_index}.json'
    with open(file_name, 'w', encoding='utf-8') as file:
        json.dump(data_list, file, indent=4, ensure_ascii=False)
        print(f'Data written to file {file_name}')

## Main function to process requests concurrently
def process_products():
    error_ids = []
    json_file = []
    file_index = 1
    # Using ThreadPoolExecutor to handle requests concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_product = {executor.submit(fetch_product, pid): pid for pid in product_ids}
        for future in as_completed(future_to_product):
            result = future.result()
            if 'error' in result:
                error_ids.append(result['product_id'])
            else:
                json_file.append(result)
                if len(json_file) == 1000:
                    save_to_json(json_file, file_index)
                    json_file = []
                    file_index += 1

    # Save any remaining products
    if json_file:
        save_to_json(json_file, file_index)

    # Optionally log error IDs if any
    if error_ids:
        with open('error_ids.log', 'w') as file:
            for _id in error_ids:
                file.write(str(_id) + '\n')

if __name__ == "__main__":
    process_products()
