from MongOps import connect, clear_db
import csv
import datetime
import requests
import json


class SymsCollector():

    def __init__(self, syms_source, db='advisor', collection='companies'):
        self.db = db
        self.collection = collection
        self.syms_sources_folder = './syms_sources'
        self.syms_source = f'{self.syms_sources_folder}/{syms_source}.txt'
        self.new_syms = []
        self.syms_to_remove = ['', ' ', '\n', '\t']
        

    def add_syms_list(self):

        self.get_new_syms()
        self.add_new_syms()


    def get_new_syms(self, verbose=False):

        with open(self.syms_source, 'r') as file:
            new_syms = file.read().split('\n')
        # remove malformed syms
        self.new_syms = [sym for sym in new_syms if sym not in self.syms_to_remove]

        if verbose:
            print('Adding new symbols from source  -->  ', self.syms_source)
            print('New symbols -->  ', self.new_syms)


    def add_new_syms(self):

        companies = connect(self.db, self.collection)
        # avoid duplicates (companies already in the database are not added again)
        already_inserted = [company['_id'] for company in list(companies.find({}))]
        self.new_syms = [symbol for symbol in self.new_syms if symbol not in already_inserted]

        for sym in self.new_syms:
            profile = self.get_company_profile(sym)
            obj_id = companies.insert  (     
                                            {
                                            '_id': profile['symbol'],
                                            'profile': profile['profile']
                                            }
                                        )


    def get_company_profile(self, sym):

        # choose which params to remove from the request
        params_to_remove = ['volAvg','mktCap','price', 'beta', 'range', 'changes', 'changesPercentage']
        # request the company profile to financialmodelingprep API's
        profile_request = requests.get(f'https://financialmodelingprep.com/api/v3/company/profile/{sym}')
        company_profile = profile_request.json()
        # remove unwanted params
        for param in params_to_remove:
            company_profile['profile'].pop(param, None)
        
        return company_profile



