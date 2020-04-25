from utils import *
from SymsCollector import SymsCollector
from SymsUpdater import SymsUpdater
from DataExtractor import DataExtractor
from Evaluator import BaseEvaluator
from MongOps import clear_db, connect

clear_db(collection='key_metrics')


# collect
#collector = SymsCollector(collection='companies')
#collector.add_syms_list()
# update
#updater = SymsUpdater('companies')
#updater.update_all()
# extract
extractor = DataExtractor(destination_collection='key_metrics')
extractor.extract_key_metrics()

base_evaluator = BaseEvaluator('key_metrics', 'scores')
base_evaluator.evaluate()

#print(base_evaluator.newest_company_reports)