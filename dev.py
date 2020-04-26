from utils import *
from SymsCollector import SymsCollector
from SymsUpdater import SymsUpdater
from DataExtractor import DataExtractor
from Ranker import BaseRanker
from MongOps import clear_db, connect
from datetime import datetime

#clear_db(collection='key_metrics')
#clear_db(collection='companies')
#start = datetime.now()
#
## collect
#collector = SymsCollector(syms_source = 'more_test', collection='companies')
#collector.add_syms_list()
## update
#updater = SymsUpdater('companies')
#updater.update_all()
## extract
#extractor = DataExtractor(destination_collection='key_metrics')
#extractor.extract_key_metrics()

base_ranker = BaseRanker('key_metrics', 'rankings', './rankings')
base_ranker.rank()
base_ranker.save()

#finish = datetime.now()
#print(f'Time elapsed:, {finish-start}')
