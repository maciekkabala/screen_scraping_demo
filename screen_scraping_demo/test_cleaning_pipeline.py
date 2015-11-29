'''
Created on 29-11-2015

@author: Maciej Kabala
'''

#TODO: should be move into test package

import unittest
from pipelines import CleaningPipeline
from items import Offer
from datetime import date
from datetime import timedelta

class TestCleaningPipeline(unittest.TestCase):

    out_date_format = "%d.%m.%Y"

    def _create_default_offer(self):
        offer = Offer()
        offer['date'] = '5.10.2015'
        offer['obid'] = 'default'
        offer['price'] = 'default'
        return offer

    def testDate_Heute(self):
#       quoka return sometimes Heute as date; check if pipeline convert it properly
  
        pipeline = CleaningPipeline()
        
        offer = self._create_default_offer()
        offer['date'] = "Heute"
        
        cleaned_offer = pipeline.process_item(offer, None)

        self.assertEqual(type(cleaned_offer['date']), str)
        self.assertEqual(cleaned_offer['date'], date.today().strftime(self.out_date_format) )

    def testDate_heute(self):
#       quoka return sometimes Heute as date; check if pipeline convert it properly
  
        pipeline = CleaningPipeline()
        
        offer = self._create_default_offer()
        offer['date'] = "heute"
        
        cleaned_offer = pipeline.process_item(offer, None)

        self.assertEqual(type(cleaned_offer['date']), str)
        self.assertEqual(cleaned_offer['date'], date.today().strftime(self.out_date_format) )

    def testDate_Gestern(self):
#       quoka return sometimes Gestern as date; check if pipeline convert it properly
  
        pipeline = CleaningPipeline()
        
        offer = self._create_default_offer()
        offer['date'] = "Gestern"
        
        cleaned_offer = pipeline.process_item(offer, None)

        self.assertEqual(type(cleaned_offer['date']), str)
        self.assertEqual(cleaned_offer['date'], (date.today() - timedelta(1)).strftime(self.out_date_format) )

    def testDate_gestern(self):
#       quoka return sometimes gestern as date; check if pipeline convert it properly
  
        pipeline = CleaningPipeline()
        offer = self._create_default_offer()
        offer['date'] = "gestern"
        
        cleaned_offer = pipeline.process_item(offer, None)

        self.assertEqual(type(cleaned_offer['date']), str)
        self.assertEqual(cleaned_offer['date'], (date.today() - timedelta(1)).strftime(self.out_date_format) )

    def testDate_vor_6_month(self):
#       quoka return sometimes gestern as date; check if pipeline convert it properly
  
        pipeline = CleaningPipeline()
        offer = self._create_default_offer()
        offer['date'] = "vor 6 Monaten"
        
        cleaned_offer = pipeline.process_item(offer, None)

        self.assertEqual(cleaned_offer['date'], None )

        
    def testPrice(self):
        pipeline = CleaningPipeline()
        offer = self._create_default_offer()
        offer['price'] = "200.000,-"
        
        cleaned_offer = pipeline.process_item(offer, None)
        
        self.assertEqual(cleaned_offer['price'], '200000' )

    def testPrice_None(self):
        ''' Should not crash '''
        pipeline = CleaningPipeline()
        offer = self._create_default_offer()
        offer['price'] = None
        
        cleaned_offer = pipeline.process_item(offer, None)
        
        self.assertEqual(cleaned_offer['price'], None )

    def testPrice_NotInItem(self):
        ''' Should not crash '''
        pipeline = CleaningPipeline()
        offer = self._create_default_offer()
        del offer['price'] 
        
        cleaned_offer = pipeline.process_item(offer, None)
        
        self.assertFalse('price' in cleaned_offer)


        

if __name__ == "__main__":
    unittest.main()