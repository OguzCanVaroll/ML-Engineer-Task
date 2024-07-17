import requests
from bs4 import BeautifulSoup
import json
from kafka import KafkaProducer
import time

url = 'https://scrapeme.live/shop/'

def get_data():
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        products = []
        for product in soup.select('.product'):
            
            name = product.select_one('.woocommerce-loop-product__title').text
            
            price = product.select_one('.price').text
            
            product_link = product.select_one('a')['href']
            
            product_response = requests.get(product_link)
            
            
            if product_response.status_code == 200:
                product_soup = BeautifulSoup(product_response.text, 'html.parser')
                description = product_soup.select_one('.woocommerce-product-details__short-description').text if product_soup.select_one('.woocommerce-product-details__short-description') else "No description available"
                description = description.replace('\n', ' ').strip()
            
            else:
                description = "No description available"
            products.append({'name': name, 'price': price, 'description': description})
        return products
    
    else:
        print("Failed to retrieve data")
        return None

def main():
    
    time.sleep(30) 
    
    producer = KafkaProducer(
        
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    data = get_data()
    if data:
        for product in data:
            producer.send('my-topic', product)
        producer.flush()
        print("Data has been sent to Kafka")

if __name__ == "__main__":
    main()
