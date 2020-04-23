from utils import connect
import csv
import datetime
import requests
import json

class FinancialStatementUpdater():
    def __init__(self, index='sp500'):
        self.index = index
        self.new_companies = []
        self.updates = []
        self.index_companies = []
    

    def update_index(self):

        # get name and ticker for each company in the index
        self.get_index_companies()
        # insert new companies that weren't in the database
        self.insert_new_companies()
        # update the financial statement of every company in the index
        self.update_companies()
        # notify of new quarterly financial statements
        self.notify_updates()
    
        return 


    def get_index_companies(self):

        indexes_dir = './indexes'
        # open a .csv containing the companies included in the index
        with open(f'{indexes_dir}/{self.index}.csv', 'r') as csv_file:
            self.index_companies = [dict(company) for company in list(csv.DictReader(csv_file))]


    def insert_new_companies(self):

        # open MongoDB connection
        collection = connect(self.index, 'companies')
        inserted_companies = list(collection.find({}))
        inserted_companies_names = [company['Name'] for company in inserted_companies]

        print('\nAlready present companies:', len(inserted_companies))

        # insert new companies
        for company in self.index_companies:
            name = company['Name']
            if name not in inserted_companies_names:
                company['last_update'] = datetime.datetime.today()
                collection.insert_one(company)
                self.new_companies.append(company)


    def update_companies(self):

        updates = []
        today = datetime.datetime.today()
        collection = connect(self.index, 'companies')
        companies = list(collection.find({}))
        companies_names = [company['Name'] for company in companies]
        new_companies_names = [company['Name'] for company in self.new_companies]

        for company in companies:
            name = company['Name']
            last_update = company['last_update']
            delta = eval('(today - last_update).days')
            # if the last update is too old (80 days) OR forced update ---> update the company data
            if delta > 85 or name in new_companies_names:
                self.update_company(company, collection)

        return updates


    def update_company(self, company, connection):

        update = False
        self.updates.append(update)


    def notify_updates(self):
        if self.new_companies:
            print('\n\n NEW COMPANIES INSERTED \n\n', self.new_companies)
            print('-----------------------------------------------------------')
        if self.updates:
            print('\n\n COMPANIES UPDATED \n\n', self.updates)
        if not self.updates and not self.new_companies:
            print('\n\n NO NEW COMPANIES and NO UPDATES')


    def clear_collection(self):
        collection = connect(self.index, 'companies')
        collection.delete_many({})
        



sp500updater = FinancialStatementUpdater('sp500')

sp500updater.update_index()

