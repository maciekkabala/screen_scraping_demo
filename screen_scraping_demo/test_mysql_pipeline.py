'''
Created on 29-11-2015

@author: Maciej Kabala
'''
import unittest
from pipelines import MySqlPipeline
from items import Offer
from datetime import date

class TestMySqlPipeline(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_get_values_for_sql_insert(self):
        pipe = MySqlPipeline()
        item = Offer()
        item['city'] = 'Krakow' # same name on db
        item['title'] = 'Wawel for sale' # name on db another than Offer item
        item['date'] = '05.10.2015' # on db table it is decimal(8); should be converted
        columns, values = pipe._get_values_for_sql_insert(item)
        
        today = date.today()
        self.assertEqual(columns, ['city', 'offer_title', 'create_date_on_site', 'created_at', 'month'])
        self.assertEqual(values, ["Krakow", "Wawel for sale", "20151005", today.strftime('%Y-%m-%d'), today.strftime('%m')] )

        
#         TODO: compare db schema with Offer and MySql.offer_2_db_table


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()