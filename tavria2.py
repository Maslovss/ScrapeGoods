
from dataclasses import dataclass

import asyncio

import logging
import time
import datetime
from typing import List

import httpx

import re
from bs4 import BeautifulSoup
from aiohttp import ClientSession


@dataclass
class Category:
    topic: str
    name: str
    url: str

@dataclass
class Product:
    topic: str
    subtopic: str

    name: str
    id: str

    qty:    str
    measure: str

    price: str
    price_old: str
    price_discount: str

def init():
    pass

async def fetch(url, session):
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        #await asyncio.sleep(5)
        return await response.read()

async def bound_fetch( url, sem , session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)


async def get_webpage(url):
    async with httpx.AsyncClient() as client:
        await asyncio.sleep(0.5)
        response = await client.get(url)
        return response.text


async def scrape_categories( loop , base_url , tavria_list , sem , session):

    #page = await get_webpage(base_url)
    page = await bound_fetch(base_url , sem , session)
    #page = await bound_fetch('http://www.google.com.ua/' , sem , session)
    

    #print(page)
    soup = BeautifulSoup(page, 'html.parser')
    topics = soup.find(id='mobile-drill-menu').find('div', class_='mobile-drill-menu__wrapper').find('ul',class_='mobile-drill-menu__catalog').find_all('li', class_='catalog-parent__item')

    for topic in topics:
        submenu = topic.find('a',class_='catalog__subnav-trigger')
        categories=[]
        try:
            categories = topic.find('ul',class_='submenu').find_all('li')
            topic_name = " ".join(submenu.text.split()) 
        except:
            pass
        for category in categories[1:]:
            category_name = " ".join(category.find('a').text.split())   
            logging.debug(f"NEW CATEGORY: topic={topic_name}, cat={category_name}")
            new_category = Category( topic_name , category_name ,  'https://www.tavriav.ua' + category.find('a')['href'])
            tavria_list.append( new_category)
 
async def get_category_pages_count(page):

    index_soup = BeautifulSoup(page, 'html.parser')

    last_page_id = 1

    try:
        last_page = index_soup.find('ul',class_='pagination').find_all('li',class_='page-item')[-1].find('a')['href']
        last_page_id=int(re.findall('\d+$',last_page)[0])
    except:
        pass

    return last_page_id


async def scrape_category_page( url , category:Category,  products:List  , sem , session):
    #page = await get_webpage(url)
    page = await bound_fetch(url , sem , session)
    catalog_soup = BeautifulSoup(page, 'html.parser')
    soup_products = catalog_soup.find('div',class_='catalog-products__container').find_all(class_='products__item')
    for soup_product in soup_products:
        product_id=''
        product_title=''
        try:
            product_id = re.findall('\d+',soup_product.find('p',class_='product__title').find('a')['href'])[0]
            product_title = soup_product.find('p',class_='product__title').find('a').text
        except:
            logger.debug('Error:')
            logger.debug(soup_product)
            raise Exception('Parse error' + soup_product)
        
        price_discount=''
        price_old=''
        product_price =''
        try:
            product_price = re.findall(r'\d+\.*\d*' ,  soup_product.find('p',class_='product__price').find('b').text)[0]    
        except:
            try:
                price_discount = re.findall(r'\d+\.*\d*' ,  soup_product.find(class_='price__discount').text)[0]
                price_old = re.findall(r'\d+\.*\d*' , soup_product.find(class_='price__old').text)[0]
            except:
                pass

        category_topic = category.topic.replace(',',' _ ')
        category_name = category.name .replace(',','_')
        product_title = re.sub(r"(\d+)\,(\d+)",r"\1.\2",product_title)
        product_title = product_title.replace(',',' _ ')
        product_qty = ''
        product_measure = ''

        r = re.findall(r'\d+\.*\d*\s(?:л|г|кг|мл|мг|шт\.)(?:\s|$)',product_title)
        if len(r)>0:
            product_qty_measure =r[0]
            product_qty = re.findall(r'\d+\.*\d*',product_qty_measure)[0]
            product_measure =  re.findall(r'(?:л|г|кг|мл|мг|шт\.)',product_qty_measure)[0]
        del r
                

        record_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"{record_time} , {category_topic} , {category_name} , {product_id} , {product_title} , {product_price} , {price_discount} , {price_old} , {product_qty} , {product_measure} \n")    
        products.append(Product(category_topic , category_name , product_title , product_id ,product_qty, product_measure , product_price , price_old ,price_discount  ))

async def scrape_category(loop , category:Category , products:List, sem , session):
    #Сначала узнаем сколько страниц имеется в указанной категории 
    #page = await get_webpage(category.url )
    page = await bound_fetch(category.url , sem , session)
    pages_count = await get_category_pages_count(page)

    logging.debug(f"{category.topic} = {pages_count} pages")

    tasks =[]
    
    for i in range(pages_count):
        tasks.append(scrape_category_page( f"{category.url}?page={i+1}" , category ,  products , sem , session))
    await asyncio.gather(*tasks)

def export_data(file_name , format_ , products:List[Product]):
    if format_ == 'csv':
        with open(file_name, mode='w', encoding='utf-8') as f:
            #write header
            f.write("record_time , category_topic , category_name , product_id , product_title , product_price , price_discount , price_old , product_qty , product_measure \n")    
            for product in products:
                record_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{record_time} , {product.topic } , {product.subtopic} , {product.id} , {product.name} , {product.price} , {product.price_discount} , {product.price_old} , {product.qty} , {product.measure} \n")    




async def main(loop , base_url , products):
    
    categories = []
    sem = asyncio.Semaphore(5)
    async with ClientSession() as session:

    #First load index
        await scrape_categories(loop , base_url  , categories , sem , session)

        tasks =[]

        for category in categories:
            tasks.append( scrape_category(loop , category , products , sem , session) )
        await asyncio.gather(*tasks)


#init logger  etc
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.debug("MAIN START")

BASE_URL = 'https://www.tavriav.ua/'
products =[]

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop , BASE_URL , products))
export_data("tavria.csv",'csv',products)

print(products)

