from MongOps import connect, clear_db
from utils import get_newest_date
import csv
import datetime
import requests
import json

class BaseEvaluator():
    
    def __init__(self, source_collection, destination_collection):
        self.source_connection = connect('advisor', source_collection)
        self.destination_connection= connect('advisor', destination_collection)
        self.syms = list(set([report['symbol'] for report in list(self.source_connection.find({}))]))
        self.reports = [report for report in list(self.source_connection.find({}))]
        # last report for each company
        self.newest_company_reports = [report for report in self.reports if report['_id'].split('_')[1] == str(0)]


    def evaluate(self):

        eval_metrics = [self.revenue_per_share, self.net_income_per_share]
    
        for metric in eval_metrics:
            result = metric()
            print(result)
            
        

    def revenue_per_share(self, classification_type = 'Revenue per Share'):

        values = []
        classification = []

        for report in self.newest_company_reports:
            values.append((report['symbol'], report['quarter'], float(report[classification_type])))

        sorted_values = sorted(values, key=lambda x: x[2], reverse=True)
        
        for pos, value in enumerate(sorted_values):
            classification.append(value + (len(sorted_values)-pos, ))

        return classification_type, classification


    def net_income_per_share(self, classification_type = 'Net Income per Share'):

        values = []
        classification = []

        for report in self.newest_company_reports:
            values.append((report['symbol'], report['quarter'], float(report[classification_type])))

        sorted_values = sorted(values, key=lambda x: x[2], reverse=True)
        
        for pos, value in enumerate(sorted_values):
            classification.append(value + (len(sorted_values)-pos, ))

        return classification_type, classification


    