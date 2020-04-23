from SymsCollector import SymsCollector
from SymsUpdater import SymsUpdater
from MongOps import clear_db, connect

#clear_db()
collector = SymsCollector()
collector.add_syms_list()


a = SymsUpdater()


a.update_financial_statement() 
#clear_db()

a.update_enterprise_value()
a.update_financial_ratio()
a.update_key_metrics()
a.update_financial_growth()
a.update_rating()
a.update_discounted_cash_flow()
print(a.updates_done)
