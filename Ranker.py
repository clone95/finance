from MongOps import connect, clear_db
from utils import *
import pandas as pd
import datetime
import requests
import json

class BaseRanker():
    
    def __init__(self, source_collection, destination_collection, save_path):
        self.source_connection = connect('advisor', source_collection)
        self.destination_connection= connect('advisor', destination_collection)
        self.syms = list(set([report['symbol'] for report in list(self.source_connection.find({}))]))
        self.reports = [report for report in list(self.source_connection.find({}))]
        # get last report available for each company
        self.newest_company_reports = [report for report in self.reports if report['_id'].split('_')[1] == str(0)]
        self.rankings = []
        self.save_path = save_path


    def rank(self):

        eval_metrics = [self.revenue_per_share, self.net_income_per_share]
        
        for metric in eval_metrics:
            result = metric()
            self.rankings.append(result)

        for report in self.newest_company_reports:
            symbol = report['symbol']
            for ranking in self.rankings:
                ranking_type = ranking[0]
                ranking_dict = ranking[1]
                report['SCORE_' + ranking_type] = ranking_dict[symbol]
        

    def save(self):
        # prepare csv with Pandas
        data = pd.DataFrame(self.newest_company_reports)
        data['_id'] = data['_id'].apply(lambda x: x.split('_')[0])
        # save csv
        ensure_dir_exists(self.save_path)
        data.to_csv(f'{self.save_path}/base_ranker.csv')

    def revenue_per_share(self, classification_type = 'Revenue per Share'):

        values = []
        classification = {}

        for report in self.newest_company_reports:
            values.append((report['symbol'], float(report[classification_type])))

        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        
        for pos, value in enumerate(sorted_values):
            classification[value[0]] = len(sorted_values) - pos

        return classification_type, classification


    def net_income_per_share(self, classification_type = 'Net Income per Share'):

        values = []
        classification = {}

        for report in self.newest_company_reports:
            values.append((report['symbol'], float(report[classification_type])))

        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        
        for pos, value in enumerate(sorted_values):
            classification[value[0]] = len(sorted_values) - pos

        return classification_type, classification


    