from SymsCollector import SymsCollector
from SymsUpdater import SymsUpdater
from MongOps import clear_db

#clear_db()

a = SymsUpdater()

a.update_financial_statement()