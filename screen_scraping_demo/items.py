# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Offer(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    price = scrapy.Field()
    postal_code = scrapy.Field()
    city = scrapy.Field()
    obid = scrapy.Field()
    date = scrapy.Field()
    details = scrapy.Field()
