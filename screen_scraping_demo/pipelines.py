# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import date
from datetime import timedelta
import MySQLdb as db

class CleaningPipeline(object):
    out_date_format = "%d.%m.%Y"

    def process_item(self, item, spider):
        item['obid'] = item['obid'].strip() 
        if 'price' in item and item['price'] != None:
            item['price'] = item['price'].replace(',-','').replace('.','') #hack - regex should be better
        
        if 'date' in item:
            if item['date'].lower().strip() == 'heute':
                item['date'] = date.today().strftime(self.out_date_format)
            elif item['date'].lower().strip() == 'gestern':
                item['date'] = (date.today()- timedelta(1)).strftime(self.out_date_format)
            else: #for dates like 'vor 6 Monaten'
                date_tuple = item['date'].split('.')
                if len(date_tuple) == 3:
                    pass #very simply assumption; it could be even than not valid date
                else:
                    item['date'] = None
                  
        return item

# ORM framework would be much better, especially for support.
# For this demo I decide to use simple MySqlDb -> time to market ;-)
class MySqlPipeline(object):
    ''' Very naive implementation; no exception handling; no reconnect if connection lost
    '''
  

#Mapping Offer item into db_table schema
#From implementation point of view could be done in Offer class
#But from design point of view(Open-Close principle), better do this outside!
   
    offer_2_db_table = { 'title': 'offer_title',
                         'price': 'buy_price',
                         'details': 'description',
                         'date': 'create_date_on_site',
                       }

    insert_command = 'INSERT INTO real_estate_offer(%s) Values(%s)'
    
    out_date_format = "%Y%m%d"
    
    def _transform_date(self, in_date):
        day, month, year = map(int, in_date.split('.'))
        return date(year, month, day).strftime(self.out_date_format)
        
    
    def __init__(self):
        self.connection_data = ('localhost', 'mucha', 'pajak', 'lasiodora_parahybana' )
        self.con = None
        self.transformations = {'date': self._transform_date
                               }
    
    def open_spider(self, spider):
        self.con = db.connect(*self.connection_data)
    
    def close_spider(self, spider):
        self.con.close()

    def _transform_value(self, key, value):
        if key in self.transformations:
            return self.transformations[key](value)
        else:
            return value

    def _get_values_for_sql_insert(self, item):
        '''
            return tuple:
             - string containing column names for sql insert
             - string containing values for sql insert
        '''
        column_list = []
        value_list = []
        
        for key in item.fields.keys():
            if key in item and item[key] != None:
                if key in self.offer_2_db_table:
                    column_list.append(self.offer_2_db_table[key])
                else:
                    column_list.append(key)
                value_list.append(self._transform_value(key, item[key]))
            else:
                pass # do nothing, item not filled => do not sent into db
        
        today = date.today()
        column_list.append('created_at')
        value_list.append(today.strftime('%Y-%m-%d'))
        
        column_list.append('month')
        value_list.append(today.strftime('%m'))
        
        return column_list, value_list
    
    def process_item(self, item, spider):
        try:
            cur = self.con.cursor()
            column_list, value_list = self._get_values_for_sql_insert(item)
            assert len(column_list) == len(value_list)
            
            command = self.insert_command % ( ", ".join(column_list), ", ".join(len(value_list) * ["%s"] ) )
            cur.execute(command, value_list)
            
    #      commit on close, or on process item?
    #      depends on requirement, without specific knowledge I commit at proccess_item
            self.con.commit()
        except db.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
                    
        return item