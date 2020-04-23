from SymsCollector import SymsCollector
from SymsUpdater import SymsUpdater
from MongOps import clear_db, connect

#clear_db()
collector = SymsCollector()
collector.add_syms_list()


a = SymsUpdater()


a.update_all()
print(a.updates_done)