from MongOps import connect
import csv
import datetime
from datetime import date
import requests
import json


class SymsUpdater():

    def __init__(self, collection, db='advisor'):

        self.db = db
        self.collection = collection
        self.connection = connect(self.db, self.collection)
        self.companies = [company for company in list(self.connection.find({}))]
        self.updates_done = []
        self.update_frequencies = {

                            'financial_statement': 'quarter',
                            'financial_ratio': 'annual',
                            'enterprise_value': 'quarter',
                            'key_metrics': 'quarter',
                            'financial_growth': 'quarter',
                            'rating': 'daily',
                            'discounted_cash_flow': 'daily'

                             }


    def update_all(self):

        self.update_financial_statement()
        self.update_financial_ratio()
        self.update_enterprise_value()
        self.update_key_metrics()
        self.update_financial_growth()
        self.update_rating()
        self.update_discounted_cash_flow()


    def update_financial_statement(self):
        
        update_type = 'financial_statement'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:

            company_profile = list(self.connection.find({'_id': sym}))[0]

            income_request = requests.get(f'https://financialmodelingprep.com/api/v3/financials/income-statement/{sym}?period=quarter')
            income = income_request.json() 

            balance_sheeet_request = requests.get(f'https://financialmodelingprep.com/api/v3/financials/balance-sheet-statement/{sym}?period=quarter')
            balance_sheeet = balance_sheeet_request.json() 

            cash_flow_request = requests.get(f'https://financialmodelingprep.com/api/v3/financials/cash-flow-statement/{sym}?period=quarter')
            cash_flow = cash_flow_request.json() 

            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            { update_type: 
                                                {
                                                'income':income['financials'],
                                                'balance_sheeet':balance_sheeet['financials'],
                                                'cash_flow':cash_flow['financials']
                                                },
                                            f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))


    def update_financial_ratio(self):
        update_type = 'financial_ratio'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:

            company_profile = list(self.connection.find({'_id': sym}))[0]

            financial_ratio_request = requests.get(f'https://financialmodelingprep.com/api/v3/financial-ratios/{sym}')
            financial_ratio = financial_ratio_request.json() 

            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            {  
                                                update_type: financial_ratio['ratios'],
                                                f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))


    def update_enterprise_value(self):

        update_type = 'enterprise_value'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:

            company_profile = list(self.connection.find({'_id': sym}))[0]

            enterprise_value_request = requests.get(f'https://financialmodelingprep.com/api/v3/enterprise-value/{sym}/?period=quarter')
            enterprise_value = enterprise_value_request.json() 

            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            {  
                                                update_type: enterprise_value['enterpriseValues'],
                                                f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))


    def update_key_metrics(self):
        
        update_type = 'key_metrics'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:

            company_profile = list(self.connection.find({'_id': sym}))[0]

            key_metrics_request = requests.get(f'https://financialmodelingprep.com/api/v3/company-key-metrics/{sym}/?period=quarter')
            key_metrics = key_metrics_request.json() 

            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            {  
                                                update_type: key_metrics['metrics'],
                                                f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))
        
    
    def update_financial_growth(self):

        update_type = 'financial_growth'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:

            company_profile = list(self.connection.find({'_id': sym}))[0]

            financial_growth_request = requests.get(f'https://financialmodelingprep.com/api/v3/financial-statement-growth/{sym}/?period=quarter')
            financial_growth = financial_growth_request.json() 

            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            {  
                                                update_type: financial_growth['growth'],
                                                f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))


    def update_rating(self):

        update_type = 'rating'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:
            
            merged_rating = {}
            company_profile = list(self.connection.find({'_id': sym}))[0]

            rating_request = requests.get(f'https://financialmodelingprep.com/api/v3/company/rating/{sym}/?period=quarter')
            rating = rating_request.json() 
            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            {  
                                                update_type: {
                                                                'rating': rating['rating'],
                                                                'details': rating['ratingDetails']
                                                             },
                                                f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))    


    def update_discounted_cash_flow(self):

        update_type = 'discounted_cash_flow'
        syms_to_update = [company['_id'] for company in self.companies
                                 if self.to_update(company, update_type, self.update_frequencies[update_type])] 

        for sym in syms_to_update:

            company_profile = list(self.connection.find({'_id': sym}))[0]

            discounted_cash_flow_request = requests.get(f'https://financialmodelingprep.com/api/v3/company/discounted-cash-flow/{sym}/?period=quarter')
            discounted_cash_flow = discounted_cash_flow_request.json() 

            self.connection.update_one(
                                         { "_id": company_profile['_id']},
                                         { "$set": 
                                            {  
                                                update_type: discounted_cash_flow['dcf'],
                                                f'last_update_{update_type}': datetime.datetime.today()
                                            }
                                         }
                                      )
            
            self.updates_done.append((update_type, sym))    


    def to_update(self, company, update_type, frequency):

        last_update = None
        datetime.datetime.today()
        # check if last update exists
        try:
            last_update = company[f'last_update_{update_type}']
        except Exception as e:
            return True # need to update (first time insertion)

        # if last_update esists
        if last_update:
            # check if it's too old
            delta = (datetime.datetime.today() - last_update)
            if frequency == 'quarter' and delta.days > 88:
                return True # update
            if frequency == 'annual' and (delta.days / 365.25) > 1:
                return True # update
            if frequency == 'daily':
                return True # update
            else:
                return False # not update