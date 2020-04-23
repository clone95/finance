from MongOps import connect
import csv
import datetime
from datetime import date
import requests
import json


class SymsUpdater():

    def __init__(self, db='advisor', collection='companies'):

        self.db = db
        self.collection = collection
        self.connection = connect(self.db, self.collection)
        self.companies = [company for company in list(self.connection.find({}))]
        self.update_frequencies = {

                            'financial_statement': 'quarter',
                            'financial_ratios': 'annual',
                            'enterprise_value': 'quarter',
                            'key_metrics': 'quarter',
                            'financial_growth': 'quarter',
                            'rating': 'daily',
                            'discounted_cash_flow': 'real-time'

                             }


    def update_financial_statement(self):
        
        update_type = 'financial_statement'
        syms_to_update = [company['symbol'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 
        print(syms_to_update)
        for sym in self.syms:
            
            company_profile = list(self.connection.find({'symbol': sym}))[0]
            if not company_profile['quarter_update']:
                print('non esiste')
            if to_update:
                income_request = requests.get(f'https://financialmodelingprep.com/api/v3/financials/income-statement/{sym}')
                income = income_request.json() """


    def update_financial_ratios(self):
        ...


    def update_enterprise_value(self):
        ...


    def update_key_metrics(self):
        ...
    
    
    def update_financial_growth(self):
        ...


    def update_rating(self):
        ...
    

    def update_discounted_cash_flow(self):
        ...

    
    def to_update(self, company, update_type, frequency):

        last_update = None
        today = datetime.datetime.today()

        # check if last update exists
        try:
            last_update = company['profile'][f'last_update_{update_type}']
        except Exception as e:
            return True # need to update (first time insertion)

        # if last_update esists
        if last_update:
            # check if it's too old
            delta = (today - last_update)
            if frequency == 'quarter' and delta.days > 88:
                return True # update
            if frequency == 'annual' and (delta.days / 365.25) > 1:
                return True # update
            if frequency == 'daily':
                return True # update
            else:
                return False # not update