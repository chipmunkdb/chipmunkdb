import asyncio
import unittest
import pandas as pd
import numpy as np
from aiohttp import web

from chipmunkdb.ChipmunkDb import ChipmunkDb
from chipmunkdb_server.DatabaseManager import DatabaseManager
from chipmunkdb_server.TableDatabase import TableDatabase
from chipmunkdb_server.server import getThreaderServer, cleanDatabase


class TEstInternalData(unittest.TestCase):

    def testBasicRW(self):

        dbmng = DatabaseManager()

        table = TableDatabase({
            "name": "test_py",
            "indextype": ""
        }, dbmng)

        df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
                          columns=['a', 'b', 'c'])

        table.addDataframeToDatabase(df)

        table.save(update=False)


        table.cleanup()

        table2 = TableDatabase({
            "name": "test_py",
            "indextype": ""
        }, dbmng)

        table2.loadInfos(create=False)

        q = table.query("SELECT * FROM test_py WHERE a=1 LIMIT 100")
        print(q)







