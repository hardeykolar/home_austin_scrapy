import scrapy
from ..items import AustinHomeItem
from dotenv import load_dotenv
import os


class AustinBotSpider(scrapy.Spider):
    name = "austin_bot"
    
    load_dotenv()
    custom_settings = {

    'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID'),
    'AWS_SECRET_ACCESS_KEY' : os.environ.get('AWS_SECRET_ACCESS_KEY'),

    'FEEDS' : {
        's3://austin-homes-data/%(name)s/%(name)s_%(time)s.json': {
            'format': 'json',
            'encoding': 'utf8',
            'store_empty': False,
            'indent': 4
        },
        's3://austin-homes-data/%(name)s/%(name)s_%(time)s.csv':{
            'format':'csv',
            'encoding':'utf-8',
            'store_empty':False,
            'indent':4
        },
        'austin_homes.json':{'format':'json','overwrite':'True'},
        'austin_homes.csv':{'format':'csv','overwrite':'True'}


}
    }
    
    

    allowed_domains = ["www.homes.com"]
    start_urls = [f"https://www.homes.com/austin-tx/p{str(i)}/?bb=28u1h26gtJ-1mjk4O" for i in range(1,3)]

    def parse(self, response):
        property_links = response.xpath("//div[contains(@class,'for-sale-content-container')]//a")
        yield from response.follow_all(property_links, self.parse_property_page)
        
        
    def parse_property_page(self, response):

        AustinHomes = AustinHomeItem()
        AustinHomes["beds"] = response.xpath('//span[@class="property-info-feature beds"]//span/text()').get()
        AustinHomes["bath"] = response.xpath('//span[@class="property-info-feature"]//span/text()').get()
        AustinHomes["land_size"] = response.xpath('//span[@class="property-info-feature sqft"]//span/text()').get()
        AustinHomes["pricepersqft"] = response.xpath('//span[@class="property-info-feature pricepersqt"]//span/text()').get()
        AustinHomes["agent_name"] = response.xpath('//div[@id="agent-card-name-container"]//a/text()').get()
        AustinHomes["agent_number"] = response.xpath('//div[@class="agent-phone"]//a/text()').get()
        AustinHomes["address"] = response.css('div[class="property-info-address"] * ::text').getall()
        AustinHomes["price"] = response.css('span#price::text').get()
        AustinHomes["url"] = response.url

        yield AustinHomes
    
