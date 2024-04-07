import asyncio

import sqlite3

import threading

from chipmunkdb_server.Registrar import Registrar
from chipmunkdb_server.TableDatabase import TableDatabase
from chipmunkdb_server.DocumentDatabase import DocumentDatabase
from chipmunkdb_server.helper import extract_tables
from chipmunkdb_server.StorageDatabase import StorageDatabase

import os.path
import simplejson as json
import tracemalloc
import os.path
import time
import pandas as pd
from dotenv import load_dotenv

import sqlparse
import shutil
import numpy as np


load_dotenv()
DATABASE_DIRECTORY = os.getenv("DATABASE_DIRECTORY")


class DatabaseManager():
    def __init__(self, dbPath=DATABASE_DIRECTORY):
        self.dbPath = dbPath
        self.version = 2
        self.loaded = False
        self.lowerVersion = 4
        self.registry = Registrar()
        self._printTime = True
        self.loadedDocuments = {}
        self.loadedStorages = {}
        self.loadedDatabases = {}
        if self.isDebug():
            self.debug()

    def isDebug(self):
        return os.getenv("debug") is not None

    def debug(self):
        tracemalloc.start()

    def getMasterFile(self):
        return self.dbPath+"/"+'master.db'

    def getLockFile(self):
        return self.dbPath+"/write.lock"

    async def loadMaster(self):
        if self.loaded:
            self.loadedDatabases = {}

            currentVersion = await self.getVersion()
            if int(currentVersion["version"]) < self.version or int(currentVersion["lowerversion"]) < self.lowerVersion:
                await self.upgradeMasterFile(int(currentVersion["version"]), int(currentVersion["lowerversion"]))
            self.loaded = True

        self.startCleanupService()

    def replaceSchemas(self, sql):
        return sql.replace("default.", "").replace("'default'.", "").replace('"default".', "").replace("`default`.", "")

    def cleanup(self):

        if self.isDebug():
            current, peak = tracemalloc.get_traced_memory()
            print(f"BEFORE memory usage is  {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")

        deletedb = []
        for dbkey in self.loadedDatabases:
            try:
                db: TableDatabase = self.loadedDatabases[dbkey]

                now = time.time()
                if now - db.lastUsage() > self.getCleanupTime():
                    print("We cleanup the database: "+dbkey)
                    self.cleanupDatabase(db, dbkey)
                    deletedb.append(dbkey)
            except Exception as e:
                pass

        for key in deletedb:
            try:
                del self.loadedDatabases[key]
            except Exception as e:
                pass

        if self.isDebug():
            current, peak = tracemalloc.get_traced_memory()
            print(f"AFTER memory usage is  {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")

        self.startCleanupService()
        return True

    def cleanupDatabase(self, db: TableDatabase, dbkey):
        try:
            db.cleanup()
        except Exception as e:
            pass

        return True

    def getCleanupTime(self):
        if os.getenv("cleanup_timer"):
            return os.getenv("cleanup_timer")
        return 60 * 90 # after 90mins we cleanup databases

    def startCleanupService(self):
        thread = threading.Timer(30, self.cleanup)
        thread.start()

    async def createNewDatabase(self, name, type, indexType="timeseries"):

        try:
            await self.db_execute("INSERT INTO collections (name, type, indextype, col_columns, col_domains, lastedit) "
                              "VALUES (?, ?, ?, ?, ?, DATETIME())",
                              [
                                    name, type, indexType, "", ""
                                ])
        except Exception as e:
            print("ERror in creating database", str(e))
            return None
        return await self.loadDatabaseFromFile(name, create=True)

    async def getDocument(self, document, directory=None):

        db = await self.getOrCreateDocumentObject(directory, document)

        return {"data": db.getData(), "type": db.getType()}

    async def getAllDirectories(self):
        df = await self.db_query("SELECT first(directory) as directory FROM documents GROUP BY directory")
        return df["directory"].tolist()

    async def saveDocument(self, directory, document, data, type="single"):

        db = await self.getOrCreateDocumentObject(directory, document, type=type)

        db.save(data, type=type)

        return db.getData()

    async def getAllDocuments(self, directory=None):
        if directory is not None:
            #df = self.masterDb.execute("SELECT * FROM documents WHERE directory=?", [directory]).fetchdf().copy()
            df = await self.db_query("SELECT * FROM documents WHERE directory='"+directory+"'")
        else:
            df = await self.db_query("SELECT * FROM documents")
        if 'lastedit' in df.columns:
            df['lastedit'] = df['lastedit'].astype(str)
        return df.dropna(axis=1, how="all").to_dict('records')

    async def getAllStorages(self,):
        df = await self.db_query("SELECT * FROM storages")
        if 'lastedit' in df.columns:
            df['lastedit'] = df['lastedit'].astype(str)
        l = df.dropna(axis=1, how="all").to_dict('records')
        l = list(filter(lambda x: x["document"] != "", l))
        return l

    async def createDocument(self, directory, document, type="single"):
        await self.db_execute("INSERT INTO documents (directory, document, type) VALUES (?, ?, ?)", [
            directory, document, type
        ])
        return self.loadDocumentFromFile(directory, document, create=True)

    async def getOrLoadStorageFromFile(self, document, create=False) -> StorageDatabase:
        if document not in self.loadedStorages:
            try:

                try:
                    info = await self.getStorageInfoByName(document)
                except Exception as e:
                    pass
                if info is None and os.path.isfile("./db/storage/"+document+".store.npy"):
                    info = {"document": document, "type": "storage"}
                self.loadedStorages[document] = StorageDatabase(info, self, create=create)
            except Exception as e:
                print("Error on loading storage", str(e))
                return None

        return self.loadedStorages[document]

    async def deleteStorage(self, document):
        try:
            doc = await self.getOrLoadStorageFromFile(document)
            doc.drop()
            del self.loadedStorages[document]
        except Exception as e:
            raise e

        try:
            cnx = self.masterDb.cursor()
            cnx.execute("DELETE FROM storages WHERE document=?", [document])
        #    cnx.commit()
        except Exception as e:
            raise e
        return True

    async def getOrCreateStorage(self, document):
        if document in self.loadedStorages:
            return self.loadedStorages[document]

        storage = await self.getOrLoadStorageFromFile(document)
        if storage is None:
            return self.createStorage(document)
        return storage

    async def createStorage(self, document, type="keyvalue"):
        self.db_execute("INSERT INTO storages (document, type) VALUES (?, ?)", [
             document, type
        ])

        return self.getOrLoadStorageFromFile(document, create=True)

    async def loadDocumentFromFile(self, directory, document, create=False):
        try:
            info = await self.getDocumentInfoByName(document, directory=directory)
            self.loadedDocuments[directory+"."+document] = DocumentDatabase(info, self, create=create)
        except Exception as e:
            print("Error on loading document", str(e))

            return None

        return self.loadedDocuments[directory+"."+document]

    async def getOrCreateDocumentObject(self, directory, document, create_new=True, type="single"):

        info = await self.getDocumentInfoByName(document, directory=directory)
        ## thats for some problems
        if info is None:
            if os.path.isfile(info):
                info = True
        if info is not None:
            if directory+"."+document not in self.loadedDocuments:
                return self.loadDocumentFromFile(directory, document)
            else:
                return self.loadedDocuments[directory+"."+document]

        return self.createDocument(directory, document, type=type)

    async def getOrCreateDatabaseObject(self, name, type, indextype="timeseries", create_new=True):

        info = await self.getCollectionInfoByName(name)
        if info is not None:
            if name not in self.loadedDatabases:
                return await self.loadDatabaseFromFile(name)
            else:
                return self.loadedDatabases[name]
        if create_new:
            return await self.createNewDatabase(name, type, indexType=indextype)
        else:
            return None

    async def loadDatabaseFromFile(self, name, create=False):

        try:
            info = await self.getCollectionInfoByName(name)
            self.loadedDatabases[name] = TableDatabase(info, self, create=create)
        except Exception as e:
            print("Error on loading database", str(e))

            pass
            return None

        return self.loadedDatabases[name]

    def OnExitApp(self):

        #for collection in self.loadedDatabases:
        #    self.loadedDatabases[collection].save()

        #for collection in self.loadedStorages:
        #    self.loadedStorages[collection].save()

        return True

    async def loadFromFileSystem(self):
        ## load master database
        await self.loadMaster()

        #for col in self.getAllCollections():
        #    self.loadDatabaseFromFile(col["name"])

        return True

    async def saveCollection(self, name):

        dbObject = await self.getOrCreateDatabaseObject(name, "table")

        dbObject.saveDatabaseAsync()

        return True

    async def addDataframeToDatabase(self, df, collection, info):

        mode = info["mode"]
        domain = info["domain"]
        indextype = "timeseries"
        if isinstance(df.index, pd.core.indexes.datetimes.DatetimeIndex):
            indextype = "timeseries"
        else:
            indextype = "raw"
        dbObject = await self.getOrCreateDatabaseObject(collection, "table", indextype=indextype)

        #if "__index_level_0__" not in df.columns.tolist():
        #    raise Exception("no_index_in_dataframe", "Your dataframe has no index")

        dbObject.addDataframeToDatabase(df, mode, domain=domain)

    async def getDataFrameFromCollection(self, collection, columns, domains=[]):

        dbObject = await self.getOrCreateDatabaseObject(collection, "table")
        df = dbObject.df(domains=domains)

        df = df.dropna(axis=0, how="all", subset=[n for n in df if n != 'datetime'])

        return df[columns] if len(columns) > 0 else df

    async def blockUntilOperationsFinished(self, collection):
        dbObject = await self.getOrCreateDatabaseObject(collection, "table", create_new=False)
        if dbObject is not None:
            return dbObject.waitUntilOperationsFinished()
        return True



    async def getStorageInfoByName(self, document) -> StorageDatabase:
        df = await self.db_query("SELECT * FROM storages WHERE document='"+document+"'")
        if 'lastedit' in df.columns:
            df['lastedit'] = df['lastedit'].astype(str)
        data = df.dropna(axis=1).to_dict("records")
        if len(data) > 0:
            return data[0]
        return None

    async def getDocumentInfoByName(self, document, directory=""):
        df = await self.db_query("SELECT * FROM storages WHERE directory='"+directory+"' AND document='" + document + "'")
        if 'lastedit' in df.columns:
            df['lastedit'] = df['lastedit'].astype(str)
        data = df.dropna(axis=1).to_dict("records")
        if len(data) > 0:
            return data[0]
        return None


    async def getCollectionInfoByName(self, name):
        try:
            df = await self.db_query("SELECT * FROM collections WHERE name='" + name + "'")
            if 'lastedit' in df.columns:
                df['lastedit'] = df['lastedit'].astype(str)
            if 'col_columns' in df.columns:
                df["columns"] = df["col_columns"]
                del df["col_columns"]
            if 'col_domains' in df.columns:
                df["domains"] = df["col_domains"]
                del df["col_domains"]
            data = df.dropna(axis=1).to_dict("records")
            if len(data) > 0:
                return data[0]
        except Exception as e:
            print("ERROR occured in getCollectionByName ", str(e))
            pass

        ## thats for some problems when master file is not correct
        if os.path.isfile(self.dbPath+"/tables/"+name+".duck"):
            ## fallback defaults
            return {
                "name": name,
                "indextype": "timeseries"
            }

        return None

    async def updateStorageInfo(self, document, rows, keys, type, update=False):
        execs = []
        params = []
        if update:
            execs.append("UPDATE storages SET lastedit = DATETIME() WHERE document=?")
            params.append([document])

        execs.append("UPDATE storages SET type=?, document=?, rows=?, keys=? WHERE document=? ")
        params.append([type,
                     document,
                     rows, keys,
                     document])
        return self.db_exeucte_multiple(execs, params)

    async def updateDocumentInfo(self, directory, document, type, update=False):

        execs = []
        params = []
        if update:
            execs.append("UPDATE documents SET lastedit = DATETIME() WHERE directory=? AND document=?")
            params.append([directory, document])

        execs.append("UPDATE documents SET type=?, directory = ?, document=? WHERE directory=? AND document=? ")
        params.append([
                                                                                               type,
                                                                                               directory,
                                                                                               document,
                                                                                               directory,
                                                                                               document])

        return self.db_exeucte_multiple(execs, params)


    async def updateDatabaseInfo(self, name, columns, domains, rows, update=False):
        columns_escaped = json.dumps(columns, ignore_nan=True)
        domains_escaped = json.dumps(domains, ignore_nan=True)

        execs = []
        params = []
        if update:
            execs.append("UPDATE collections SET lastedit=DATETIME() WHERE name=? ")
            params.append([name])

        execs.append("UPDATE collections SET col_columns=?, col_domains=?, rows=? WHERE name=? ")
        params.append([columns_escaped,
                     domains_escaped,
                     rows, name])

        return await self.db_exeucte_multiple(execs, params)

    def onDatabaseLoadError(self, tableManager, name):
        self.dropCollection(name)

    async def dropCollection(self, collection):
        try:
            if collection in self.loadedDatabases:
                self.loadedDatabases[collection].drop()
                del self.loadedDatabases[collection]
        except Exception as e:
            pass

        try:
            await self.db_execute("DELETE FROM collections WHERE name=?", [collection])

        except Exception as e:
            pass

    def dropAll(self):
        try:
            os.remove(self.dbPath+"master.db")
        except Exception as e:
            print(str(e))
            pass
        try:
            os.remove(self.dbPath+"master.db.wal")
        except Exception as e:
            print(str(e))
            pass
        try:
            shutil.rmtree(self.dbPath+"tables")
        except Exception as e:
            print(str(e))
            pass


    async def dropColumns(self, collection, columns, domain=None):
        try:
            if collection in self.loadedDatabases:
                self.loadedDatabases[collection].dropColumns(columns, domain=domain)
        except Exception as e:
            pass

        return True

    async def getAllCollections(self):
        df = await self.db_query("SELECT * FROM collections")
        if 'lastedit' in df.columns:
            df['lastedit'] = df['lastedit'].astype(str)
        df["columns"] = df["col_columns"]
        del df["col_columns"]
        df["domains"] = df["col_domains"]
        del df["col_domains"]
        return df.dropna(axis=1, how="all").replace({np.nan:None}).to_dict('records')

    async def initializeMasterFile(self):
        await self.db_exeucte_multiple([
            "CREATE TABLE info(key VARCHAR(20), value VARCHAR(20))",
            "CREATE TABLE collections(name VARCHAR(50), col_columns VARCHAR(900), col_domains VARCHAR(900), type VARCHAR(20), rows INTEGER, indextype VARCHAR(30), lastedit TIMESTAMP)",
            "CREATE TABLE documents(document VARCHAR(50), directory VARCHAR(50), retention VARCHAR(50), type VARCHAR(20), size INTEGER, lastedit TIMESTAMP)",
            "CREATE TABLE storages(document VARCHAR(50), rows INTEGER, keys VARCHAR(900), retention VARCHAR(50), type VARCHAR(20), size INTEGER, lastedit TIMESTAMP)",
            "CREATE UNIQUE INDEX name_idx_storages ON storages (document)",
            "CREATE UNIQUE INDEX name_idx_documents ON documents (document)",
            "CREATE UNIQUE INDEX name_idx_collections ON collections (name)",
            "INSERT INTO info (key, value) VALUES ('version', '" + str(self.version) + "')",
            "INSERT INTO info (key, value) VALUES ('lowerversion', '" + str(self.lowerVersion) + "')"
        ])

    async def block_or_wait_for_db_queue(self):
        for i in range(100):
            if os.path.exists(self.getLockFile()):
                print("Waiting for lock file removed")
                await asyncio.sleep(5e-3)
            else:
                break
        with open(self.getLockFile(), 'w') as f:
            f.write('Locked from any process')
        return True

    async def db_execute(self, query, params = None):
        return await self.db_exeucte_multiple([query], [params] if params is not None else None)


    async def unblock_db_queue(self):
        try:
            os.remove(self.getLockFile())
        except Exception:
            pass
        return True

    async def db_exeucte_multiple(self, queries, params=None):
        await self.block_or_wait_for_db_queue()
        c = sqlite3.connect(self.getMasterFile())
        cur = c.cursor()
        index = 0
        for q in queries:
            if params is not None:
                p = params[index]
                cur.execute(q, p)
            else:
                cur.executescript(q)
            index = index +1
        c.commit()
        c.close()
        await self.unblock_db_queue()
        return True

    async def wait_for_db_queue(self):
        for i in range(100):
            if os.path.exists(self.getLockFile()):
                print("Waiting for lock file removed")
                await asyncio.sleep(5e-3)
            else:
                break
        return True

    async def db_query(self, query, params=None):
        await self.wait_for_db_queue()
        con = sqlite3.connect(self.getMasterFile())
        df = pd.read_sql_query(query, con=con)
        con.close()
        return df

    def upgradeMasterFile(self, oldVersion, oldLowerVersion):
        if oldVersion <= 1:
            print("Update not needed")

    async def query(self, query, options={}, domains=[], targetTable=None):
        start = time.time()
        statements = sqlparse.parse(query)
        res = []
        targetTable = None
        for q in statements:
            if targetTable is None:
                targetTables = extract_tables(q.value)
                if len(targetTables) > 0:
                    targetTable = targetTables[0]

            data_df = None
            columns = None
            if targetTable is None:
                data_df = await self.db_query(q.value)
                columns = self.getColumnsDescription(data_df)
            else:

                dbo = await self.getOrCreateDatabaseObject(targetTable, "table", create_new=False)

                if dbo == False or dbo is None:

                    try:
                        data_df = await self.db_query(q.value)
                        columns = self.getColumnsDescription(data_df)
                    except Exception as e:
                        raise Exception("table_not_existent", "The table "+targetTable+" does not exist")

                else:
                    data_df, columns = dbo.query(q.value, domains=domains)

            res.append((data_df, columns))

        self.printTiming(start, "query")

        return res

    def printTiming(self, start, label):
        end = time.time()
        if self._printTime:
            print("[databaseManager]  "+label, end - start)
        return True

    def getVersion(self):
        df = self.db_query("SELECT * FROM info WHERE key='version' OR key='lowerversion'")
        data = df.values
        return {"version": data[0][1], "lowerversion": data[1][1]}


    async def initialize(self):
        if not os.path.isdir(self.dbPath):
            os.mkdir(self.dbPath)
        if not os.path.isdir(self.dbPath+"/tables"):
            os.mkdir(self.dbPath+"/tables")
        if not os.path.isdir(self.dbPath+"/documents"):
            os.mkdir(self.dbPath+"/documents")
        if not os.path.isfile(self.dbPath+"/master.db"):
            await self.initializeMasterFile()

        await self.loadFromFileSystem()
        await self.unblock_db_queue()

        self.loaded = True
        return True

    def isInternalQuery(self, q):
        if "DESCRIBE " in q.upper():
            return True
        if "SHOW " in q.upper():
            return True
        return False

    async def getDfOfSchemas(self):
        df = pd.DataFrame(columns=["name"])
        df.loc[0] = ["default"]
        return df, self.getColumnsDescription(df)

    async def describeTable(self, table):
        dbo = await self.getOrCreateDatabaseObject(table, "table", create_new=False)
        if dbo is None:
            return pd.DataFrame()
        col_list = dbo.getDescriptions()
        df = pd.DataFrame(columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
        # conver the col_list with "Field", "Type", "Null", "Key", "Default", "Extra" to a dataframe
        for col in col_list:
            df.loc[len(df)] = pd.Series(col, dtype="string")



        return df, self.getColumnsDescription(df)

    async def getDfOfCollections(self):
        df = await self.db_query("SELECT name FROM collections")
        df = df.dropna(axis=1, how="all")
        return df, self.getColumnsDescription(df)

    def getColumnsDescription(self, df):
        columns = []
        for col in df.columns:
            columns.append({"name": col, "Field": col, "Type": "string", "type": "string"})
        return columns


    def internalQuery(self, q, options, domains):
        # parse query
        sqlquery = sqlparse.parse(q)

        show = False
        describe = False
        # get table name
        for statement in sqlquery:
            for token in statement.tokens:
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "SHOW":
                    show = True
                    continue
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "DESCRIBE":
                    describe = True
                    continue
                if describe and token.value.strip() != "":
                    df = self.describeTable(token.value)
                    return df
                if show and token.value.upper() == "TABLES":
                    df = self.getDfOfCollections()
                    return df
                if show and token.value.upper() == "DATABASES":
                    df = self.getDfOfSchemas()
                    return df


