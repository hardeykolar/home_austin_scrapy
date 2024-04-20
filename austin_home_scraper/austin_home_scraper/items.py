# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Item, Field


class AustinHomeItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    beds = Field()
    bath = Field()
    land_size = Field()
    pricepersqft = Field()
    agent_name = Field()
    agent_number = Field()
    address = Field()
    price = Field()
    url = Field()
