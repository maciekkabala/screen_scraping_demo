'''
Created on 28-11-2015

@author: Maciej Kabala
'''

import scrapy
from itertools import imap
from screen_scraping_demo.items import Offer
import time

#TODO: distinction commercial/ not commercial offer
#FIXME: no partner offers => why? Wrong form data?
#FIXME: a lot of ERRORS of type: Error downloading <...>: An error occurred while connecting: 113: No route to host.  

#My first screen scraping project

class ParseError(Exception):
    pass

# First step after "scrapy startproject screen_scraping_demo"
class QuokaSpider(scrapy.Spider):
    
    name = "quoka"
    allowed_domains = ["http://www.quoka.de"]
    start_urls = [
        "http://www.quoka.de/immobilien/bueros-gewerbeflaechen/",
    ]
    
    xpath_offer_link = 'div[@class="q-col n2"]/a/@href'
    xpath_offer_list = '//ul[@class="alist"]/li'
    xpath_offer_title = '//div[@class="headline"]/h1[@itemprop="name"]/text()'
    xpath_offer_price = '//div[@class="price has-type"]/strong/span/text()'
    xpath_offer_address = '//div[@class="location"]/strong/span[@class="address location"]'
#     NOT in specification => should not be implemented regarding strong SCRUM rules
#     => only ordered "things" should be implemented => but this is obvious, a bug in specification
#    there are offers in CH ... this is also a country ;-)  
    xpath_offer_address_country = 'span/span[@class="country-name country"]/text()'
    xpath_offer_address_postal_code = 'span/span[@class="postal-code"]/text()'
    xpath_offer_address_city = 'a/span/text()'
    xpath_offer_id = '//div[@class="data"]/div[@class="details"]/div[@class="date-and-clicks"]/strong/text()'
    xpath_offer_date = '//div[@class="data"]/div[@class="details"]/div[@class="date-and-clicks"]/text()'
    xpath_offer_details = '//div[@class="data"]/div[@class="details"]'
    xpath_offer_tel_num_url = '//a[@id="dspphone1"]/@onclick'
    xpath_offer_tel_num = '//span/text()'
    
    category_commercial = '1'
    category_private = '0'

    def _default_form_request(self, url, callback_meth, commercial):
        
        req = scrapy.FormRequest(url, 
                                 formdata={'classtype': 'of', 'comm': commercial}, 
                                 dont_filter=True, #this costed time, not mentioned in tutorial 
                                 callback = callback_meth)
        
        req.meta['commercial'] = commercial
        return req

    def _default_request(self, url, callback_meth):
        return scrapy.Request(url,
                              dont_filter=True, #easy to forget 
                              callback=callback_meth)    

    def parse(self, response):
        """ Parse start site.
            Send form request filtering offers only
        """
   
        yield self._default_form_request(self.start_urls[0], self.parse_start_site_with_filter, self.category_commercial)
        yield self._default_form_request(self.start_urls[0], self.parse_start_site_with_filter, self.category_private)

      
    def get_property_total_number(self, response):
# #       crash course in xpath 
# #       First impression:
# #       xpath is interesting, but html site introspection is not
        categoryList = response.xpath('//ul[@id="CategoryList"]')
#       what is the proper programming model if len(categoryList) != 1?
#       I suppose this is kind of contract violation => parser does not recognize site properly
#       This means bug in parser or site schema is changed
#       Exception raise is perhaps the right approach
        if len(categoryList) != 1:
            raise ParseError
        
        property_total_number_list = categoryList[0].xpath('li/ul/li/ul/li/span/text()').extract()
        if len(property_total_number_list) != 1:
            raise ParseError
        
        property_total_number = int(property_total_number_list[0].strip().replace('.','')) #fast hack
        
        return property_total_number

#        overall time spent on telephone: 20 minutes 
    def _get_tel_num_url(self, response):
        tel_num_raw_list = response.xpath(self.xpath_offer_tel_num_url).extract()
        if len(tel_num_raw_list) == 1:
            tel_num_raw = tel_num_raw_list[0]
            prefix_and_tel_num_url_raw = tel_num_raw.split('.load')
            if len(prefix_and_tel_num_url_raw) == 2:
                tel_num_req_raw = prefix_and_tel_num_url_raw[1]
                #tel_num_req look like (  'url' )
                # drop '(' ')' than strip and omit "'"
                tel_num_url = tel_num_req_raw.replace('(','').replace(')','').strip()[1:-1] 
                return tel_num_url  
            else:
                raise ParseError
                
            self.logger.debug(tel_num_raw)
        elif len(tel_num_raw_list) > 1:
            raise ParseError
        else:
            pass # no phone number

    def create_tel_num_parser(self, offer):
        def parse_tel_num(response):
            tel_num = response.xpath(self.xpath_offer_tel_num).extract()
            offer['phone_number'] = tel_num[0]
            yield offer
        return parse_tel_num
    
    def parse_offer(self, response):
        offer = Offer()
        offer['title'] = response.xpath(self.xpath_offer_title).extract()[0]
        offer['url'] = response.url
        price_list = response.xpath(self.xpath_offer_price).extract()
        if len(price_list) == 1:
            offer['price'] = price_list[0] 
        elif len(price_list) == 0:
            offer['price'] = None
        else:
            raise ParseError
        
        address = response.xpath(self.xpath_offer_address)
        offer['postal_code'] = address.xpath(self.xpath_offer_address_postal_code).extract_first()
        offer['city'] = address.xpath(self.xpath_offer_address_city).extract_first()
        
        offer['obid'] = response.xpath(self.xpath_offer_id).extract_first()
        offer['details'] = response.xpath(self.xpath_offer_details)[1].xpath('div/text()').extract_first()

#       response.xpath(self.xpath_offer_date).extract() contains list of strings, only one is non empty, and is date
#       alternative implementetion => filter(lambda x: len(x) > 0, map(lambda x: x.strip(), response.xpath(self.xpath_offer_date).extract()))
        date_list =  [item.strip() for item in response.xpath(self.xpath_offer_date).extract() if len(item.strip()) > 0]
        if len(date_list) == 1:
            offer['date'] = date_list[0] 
            
        offer['commercial'] = response.meta['commercial']

        tel_num_url = self._get_tel_num_url(response)
        if tel_num_url == None:
            self.logger.debug("no tel number")
            yield offer
        else:
            req =  self._default_request(response.urljoin(tel_num_url), self.create_tel_num_parser(offer))
            yield req
            

    def _parse_offer_item(self, response, offer_item):
        if 'q-ln hlisting' in offer_item.xpath('@class').extract():
            link_list = offer_item.xpath(self.xpath_offer_link)
            if len(link_list) != 1:
                raise ParseError
            link = link_list[0].extract()
            req =  self._default_request(response.urljoin(link), self.parse_offer)
            req.meta.update(response.meta)
            return req    
             
        elif 'q-ln t1 partner' in offer_item.xpath('@class').extract():
            item = Offer()
            item['title'] = 'partner' #TODO
            return offer_item

    def get_next_site(self, response):
        next_site_list = response.xpath('//li[@class="arr-rgt active"]')
        if len(next_site_list) == 1:
            next_site = next_site_list[0]
            return next_site.xpath('a/@href').extract()[0]
        elif len(next_site_list) > 1:
            raise ParseError
        else: #0
            pass # do nothing, no next site

            
    def parse_start_site_with_filter(self, response):

        property_total_number = self.get_property_total_number(response) #TODO always parsed, not only in start site!
 
         
        result_list = response.xpath(self.xpath_offer_list)
#         imap(self._parse_offer_item, result_list)
# strange behavior, imap from itertools does not work! This framework does something strange under the bonnet!
        for item in result_list:
            res =  self._parse_offer_item(response, item)
            if res != None:
                time.sleep(0.3) #ugly hack -> without I get: Error downloading <...>: An error occurred while connecting: 113: No route to host.
# I suppose to many requests -> should be better solution than sleep                
                yield res 
        
        next_page = self.get_next_site(response)
        if next_page != None:
            url = response.urljoin(next_page)
            yield self._default_form_request(url, self.parse_start_site_with_filter, response.meta['commercial'])
             
            
#    !!! this framework is strange, if I use natural function composition like:

#     def parse_start_site(self, reponse):
#        do_start_specific_stuff -> like property_total_number calculation
#        self.parse_site(response)
#
#     def parse_site(self, reponse): 
#        ...
#        yield result
#        if has_next:
#            yield Request(next_url, callback=self.parse_site(
# 
#   this does not work, yield called from inner method does not work        