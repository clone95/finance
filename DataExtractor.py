from MongOps import connect
from utils import get_quarter
import csv
import datetime
from datetime import date
import json

class DataExtractor():

    def __init__(self, destination_collection, db='advisor', source_collection='companies'):

        self.db = db
        self.destination_collection = destination_collection 
        self.source_collection = source_collection
        self.source_connection = connect(self.db, self.source_collection)
        self.destination_connection = connect(self.db, self.destination_collection)
        self.companies = [company for company in list(self.source_connection.find({}))]


    def extract_key_metrics(self):

        for company in self.companies:
            for i, el in enumerate(company['key_metrics']):
                el['symbol'] = company['_id']
                el['_id'] = company['_id'] + f'_{i}'
                el['quarter'] = get_quarter(el['date'])
                self.destination_connection.insert(el)
                

    


