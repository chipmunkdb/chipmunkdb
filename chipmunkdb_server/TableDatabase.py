from enum import Enum

import numpy as np
import pandas as pd
import os
import duckdb
from dotenv import load_dotenv
import threading
import time
import asyncio

from chipmunkdb_server.helper import printTiming

load_dotenv()

DATABASE_DIRECTORY = os.getenv("DATABASE_DIRECTORY", "db")
DEBUG = os.getenv("DEBUG", "False")

class ModeEnum(Enum):
    APPEND = "append"
    UPDATE = "update"
    OVERWRITE = "overwrite"
    KEEP = "keep"
    DROPBEFORE = "dropbefore"

class TableDatabase():
    def __init__(self, dataRow, dbManager, create=False):
        self._df = pd.DataFrame()
        self._currentlySaving = False
        self._timer = None
        self._domains = []
        self._operationsFinished = False
        self._type = "table"
        self._name = dataRow["name"]
        self._changed = True
        self._columns = []
        self._indexType = dataRow["indextype"]
        self._rows = {}
        self._lastModified = time.time()
        self._lastUsed = time.time()
        self._last_backup_saved = 0
        self._lastSaved = 0
        self._printTime = True
        self._dbManager = dbManager
        self._db = duckdb.connect(":memory:", read_only=False)
        self.loadInfos(create=create)

    def blockOperations(self):
        self._operationsFinished = False

    def unblockOperations(self):
        self._operationsFinished = True

    def isTimeseries(self):
        return self._indexType == "timeseries" or self._indexType == "time"

    def df(self, domains=[]):
        return self.getDf(domains=domains)

    def loadInfos(self, create=False):
        try:
            start = time.time()

            self.blockOperations()
            file_db = duckdb.connect(database=self.getDatabaseFile(), read_only=False)
            try:
                # Try to fetch the column information from the raw dataframe
                pragmaInfo = file_db.execute("PRAGMA table_info('"+self._name+"')").df().to_dict("records")
                for prag in pragmaInfo:
                    self._columns.append(prag["name"])
                for k in self._columns:
                    splits = k.split(".")
                    if len(splits) > 1:
                        self._domains.append(splits[0])
                self._domains = list(dict.fromkeys(self._domains))
            except Exception as pragmaerr:
                print("ERror while trying to pragmaing data of your dataframe", pragmaerr)

            ## okay table exists # lets load the dataframe
            self._df = file_db.execute('SELECT * FROM "'+self._name+'"').df().copy()

            multiIndexes = []
            # check all columns if there is a index(n) column
            for col in self._df.columns.tolist():
                # when multiple "_" in column name
                if "_index" in col and col.count("_") > 1:
                    name = col.split("_")[-1]
                    # rename the col to the name
                    self._df.rename(columns={col: name}, inplace=True)
                    multiIndexes.append(name)

            self._df.dropna(axis=1, how="all", inplace=True)
            if len(multiIndexes) > 0:
                # delete the current index
                self._df.reset_index(drop=True, inplace=True)
                # if multiindex we need to set the index
                self._df = self._df.set_index(multiIndexes)
                # sort index
                self._df.sort_index(inplace=True)
            else:
                if self.isTimeseries():
                    # if timeseries we need to set the index
                    self._df = self._df.set_index(['datetime'], append=True)
                    self._df["datetime"] = pd.to_datetime(self._df.index)
                else:
                    self._df = self._df.set_index(['_index'], append=True)

            if "datetime" in self._df.columns.tolist():
                self._df["datetime"] = pd.to_datetime(self._df["datetime"])

            if "date" in self._df.columns.tolist():
                self._df["date"] = pd.to_datetime(self._df["date"])



            # iterate over all columns and check if there are "date" or "time" in
            # the column name
            for col in self._df.columns.tolist():
                if col == "datetime":
                    continue
                if "date" in col.lower() or "time" in col.lower():
                    self._df[col] = pd.to_datetime(self._df[col], unit='ms')



            self.updateView()
            printTiming(start, "loading_database")

            file_db.close()

        except Exception as e:
            self.unblockOperations()
            if not create:
                if "does not exist" in str(e):
                    self._dbManager.onDatabaseLoadError(self, self._name)

            print(e)
            pass

        self.unblockOperations()
        return True

    def saveDatabaseAsyncTimer(self, update):
        self._timer = None
        self.saveDatabaseAsync(update)

    def saveDatabaseAsync(self, update=False, timeout=0):
        if timeout > 0 and self._timer is None:
            self._timer = threading.Timer(timeout, self.saveDatabaseAsyncTimer, [update])
            self._timer.start()
            return True

        thread = threading.Thread(target=self.save, args=[update])
        thread.start()
        return thread

    def getDatabaseFilename(self):
        return DATABASE_DIRECTORY+"/tables/"+self._name

    def getDatabaseFile(self):
        return self.getDatabaseFilename()+".duck"

    def updateView(self, update=False):
        start = time.time()
        self._db.unregister('dataframe_view')
        try:
            self._db.register('dataframe_view', self._df)
        except:
            # ignore when df is empty.
            pass

        self.updateDatabaseMaster(update)

        printTiming(start, "updateView ")

    def isTableEmpty(self):
        return self._df.shape[0] == 0

    def addDataframeToDatabase(self, dataFrame, mode: ModeEnum = ModeEnum.APPEND,
                               domain=None, leftIndex="__index_level_0__", rightIndex="__index_level_0__"):

        try:

            self.blockOperations()
            if domain is not None:
                dataFrame = dataFrame.add_prefix(domain+".")

            try:
                dataFrame = dataFrame.tz_localize(self._df.index.tz)
            except Exception as e:
                pass

            if isinstance(dataFrame.index, pd.core.indexes.datetimes.DatetimeIndex):
                dataFrame.index = dataFrame.index.round("s")
                dataFrame = dataFrame[~dataFrame.index.duplicated(keep='last')]
                dataFrame["datetime"] = pd.to_datetime(dataFrame.index)


            # iterate over all columns and check if there are "date" or "time" in
            # the column name
            for col in dataFrame.columns.tolist():
                # when col is integer we need to convert it to a string
                if type(col) == int:
                    col = str(col)
                if col == "datetime":
                    continue
                if "date" in col.lower() or "time" in col.lower():
                    # check if the column is a string
                    if dataFrame[col].dtype == "object":
                        # now we need to convert a ISO string to a datetime
                        dataFrame[col] = pd.to_datetime(dataFrame[col], infer_datetime_format=True)
                        dataFrame[col] = dataFrame[col].dt.tz_localize(None)
                    elif dataFrame[col].dtype == "datetime64[ns]":
                        # now we need to convert a ISO string to a datetime
                        dataFrame[col] = pd.to_datetime(dataFrame[col])
                    else:
                        dataFrame[col] = pd.to_datetime(dataFrame[col], unit='ms')

            start = time.time()
            if self.isTableEmpty():
                # if table is empty lets directly "create" it with the new dataFrame
                self._df = dataFrame.copy()
            else:
                # merge it
                join_df = dataFrame
                if mode == ModeEnum.DROPBEFORE.value:
                    self._df = self._df.drop(columns=[n for n in dataFrame.columns.tolist() if n != 'datetime'], errors="ignore")
                    self.updateView()
                    bstart = printTiming(start, "append_dataframe_dropbefore_updateView " + str(mode))
                    mode = "append"

                if mode == ModeEnum.KEEP:
                    self._df = self._df.merge(join_df, left_index=True, right_index=True)
                    bstart = printTiming(start, "append_dataframe_update " + str(mode))
                elif mode == ModeEnum.APPEND.value:
                    bstart = printTiming(start, "append_dataframe_prepared_datasets" + str(mode))
                    marker_df = join_df.filter(regex='\:marker', axis=1)
                    for col in marker_df.columns:
                        if col not in self._df.columns:
                            marker_df.drop(col, axis=1, inplace=True)

                    bstart = printTiming(bstart, "append_dataframe_filter_and_drop_cols " + str(mode))
                    if len(marker_df.columns) > 0:
                        try:
                            join_df.drop(marker_df.columns, axis=1, inplace=True)

                            marker_df[marker_df.columns] = marker_df[marker_df.columns].replace(np.nan, "[trash]")
                            marker_df[marker_df.columns] = marker_df[marker_df.columns].fillna("[trash]")

                            self._df.update(marker_df)
                        except Exception as err:
                            pass
                        finally:
                            self._df.replace("[trash]", np.nan, inplace=True)

                    bstart = printTiming(bstart, "append_dataframe_replaced_emptycols " + str(mode))
                    
                    self._df = self._df.merge(join_df, left_index=True, right_index=True,
                                     how='outer', suffixes=('', '_y'))
                    bstart = printTiming(bstart, "append_dataframe_merged " + str(mode))
                    self._df.drop(self._df.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
                    bstart = printTiming(bstart, "append_dataframe_drop " + str(mode))
                    testdf = self._df.loc[join_df.index, join_df.columns].update(join_df, overwrite=True)

                    self._df.loc[join_df.index, join_df.columns] = join_df
                    bstart = printTiming(bstart, "append_dataframe_update " + str(mode))
                elif mode == ModeEnum.UPDATE.value:
                    self._df.update(join_df)
                    bstart = printTiming(start, "append_dataframe_update " + str(mode))
                elif mode == ModeEnum.OVERWRITE.value:
                    self._df = self._df.merge(join_df, left_index=True, right_index=True)
                    bstart = printTiming(start, "append_dataframe_update " + str(mode))

            ## lets sort
            if "datetime" in self._df.columns.tolist() or "date" in self._df.columns.tolist() or self.isTimeseries() \
                 or "date" in self._df.index.names:
                self._df.sort_index(inplace=True)

            self._df.dropna(axis=1, how="all", inplace=True)

            bstart = printTiming(None, "append_dataframe_before_index_creation ")
            # write the indexes to a column named "index_"+indexname
            for index in self._df.index.names:
                if index is None:
                    continue
                if index != "datetime":
                    self._df["index_"+index] = self._df.index.get_level_values(index)
                    # convert the same type as the index
                    self._df["index_"+index] = self._df["index_"+index].astype(self._df.index.get_level_values(index).dtype)
            printTiming(bstart, "append_dataframe_after_index_creation ")

        except Exception as e:
            print("Error in adding data", str(e))
        finally:
            self._changed = True
            self.updateView(True)
            printTiming(start, "append_dataframe_by_finished "+str(mode))
            self._lastUsed = time.time()
            self._lastModified = time.time()
            self.saveDatabaseAsync(True)
            del dataFrame

            self.unblockOperations()

    def dropAllUnneededBlocks(self, df, ignoreStats = False):
        if ignoreStats == False:
            df = df.dropna(axis=0, how="all", subset=[n for n in df if n != 'datetime' and not n.startswith("stats.")])
        else:
            df = df.dropna(axis=0, how="all", subset=[n for n in df if n != 'datetime' ])

        return df

    def safeQuery(self, query):
        query = query.replace("`", "'")
        return query

    def query(self, query, domains=[]):
        df = pd.DataFrame()
        columns = []
        try:
            query = query.replace(self._name, "dataframe_view")
            query = self.safeQuery(query)

            self._lastUsed = time.time()
            ret = self._db.execute(query)
            df = ret.fetchdf()
            if domains is not None and len(domains) > 0:
                df = self.filter_df_by_prefix(df, domains, adding_datetime=True)

            df = self.dropAllUnneededBlocks(df, True)
        except Exception as e:
            print(str(e))
            raise(Exception("query error", str(e)))

        for col in df.columns:
            columns.append(self.getColumnDescription(col, df))

        return df, columns

    def getIndexColumns (self):
        return self._df.index.names

    def getColumns(self):
        return self._df.columns.tolist()

    def getColumnDescription(self, col, df=None):
        if df is not None and not col in self._df.columns.tolist():
            return {
                "Field": col,
                "Type": str(df[col].dtype),
                "Null": "NO",
                "Key": "",
                "Default": "",
            }
        return {
                "Field": col,
                "Type": str(self._df[col].dtype),
                "Null": "YES" if self._df[col].isnull().values.any() else "NO",
                "Key": "",
                "Default": "",
            }

    def getDescriptions(self):
        descriptions = []
        for col in self.getColumns():
            descriptions.append(self.getColumnDescription(col))
        return descriptions


    def dropColumns(self, columns, domain=None):

        self.blockOperations()

        try:
            if len(columns) > 0 and columns[0] == "*":
                columns = self.getColumns()
                if domain is not None:
                    columns = [col for col in columns if col.startswith(domain)]
            else:
                all_columns_starting_with = []
                for col in columns:
                    if domain is not None:
                        col = domain + "." + col
                    for icol in self.getColumns():
                        if icol.startswith(col):
                            all_columns_starting_with.append(icol)

                columns = all_columns_starting_with

            self._df = self._df.drop(columns=columns)

            # drop all rows where all are nan
            self._df = self.dropAllUnneededBlocks(self._df)

            self._lastUsed = time.time()
            self._changed = True
            self.updateView()

        except Exception as e:
            print("Error in Drop Columns", str(e))

        self.unblockOperations()

        return True

    def columns(self):
        return self.getColumns()

    def drop(self):
        self._db.close()
        try:
            os.remove(self.getDatabaseFilename()+".duck")
        except Exception as e:
            pass
        try:
            os.remove(self.getDatabaseFilename() + ".duck.wal")
        except Exception as e:
            pass
        try:
            os.remove(self.getDatabaseFilename() + ".duck.tmp")
        except Exception as e:
            pass

    def filter_df_by_prefix(self, df, domains, adding_datetime=False):
        filter_col = [col for col in df if col.startswith(tuple(domains))]
        if self.isTimeseries() and adding_datetime:
            filter_col.append("datetime")
        filtered_df = df[filter_col].copy()
        for domain in domains:
            filtered_df.columns = filtered_df.columns.str.replace('^'+domain + ".", "")
        return filtered_df

    def getDf(self, domains=[]):
        if domains is not None and len(domains) > 0:
            ret_df = self.filter_df_by_prefix(self._df, domains)
            return ret_df
        return self._df

    def updateDatabaseMaster(self, update=False):
        if self._changed:
            self._columns = self._df.columns.tolist()

            self._domains = []
            for k in self._columns:
                if type(k) is not str:
                    k = str(k)
                splits = k.split(".")
                if len(splits) > 1:
                    self._domains.append(splits[0])
            self._domains = list(dict.fromkeys(self._domains))

            self.run_coro(self._dbManager.updateDatabaseInfo(self._name, self._columns, self._domains, self._df.shape[0], update))
            self._changed = False

    def run_coro(self, coro):
        if not asyncio.iscoroutine(coro):
            raise ValueError("coro must be a coroutine")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

        task = loop.create_task(coro)
        # Storing the task in a container may be useful for non-REPL (for `asyncio.gather(*tasks)`
        # to ensure they finish at the end) or you can leave it to the user to handle
        if loop.is_running():
            #task.add_done_callback(print_after)
            # Not strictly necessary to return task to user, but useful if they want something
            # besides simply displaying the result when it's done
            return task
        else:
            # Since the event loop isn't running, blocking isn't a concern
            return loop.run_until_complete(task)

    def lastUsage(self):
        return self._lastUsed

    def cleanup(self):
        self.updateDatabaseMaster()
        del self._df
        self._db.close()
        del self._db

        return True

    def waitUntilOperationsFinished(self):

        if self._operationsFinished == False:
            counter = 0
            while True:
                print("waiting for opertions")
                time.sleep(0.05)
                if counter > 100:
                    self._operationsFinished = True
                    raise Exception("Error in blocking", "One of the scripts does block too long")
                if self._operationsFinished:
                    break
                counter = counter + 1

        return True

    def createFileBackup(self):
        try:
            os.system("cp " + self.getDatabaseFile() + " " + self.getDatabaseFile() + "_bkup")
        except Exception as e:
            print(str(e))
            pass

    def save(self, update=True):

        if time.time() - self._lastSaved < 2 * 60 or self._currentlySaving == True:
            if self._timer is not None:
                return False
            print("Saving to quickly")
            return self.saveDatabaseAsync(timeout=2*60)

        self.waitUntilOperationsFinished()

        self._currentlySaving = True

        start = time.time()

        # before we save the file, we copy it as a backup and delete it after all works fine
        self.createFileBackup()

        file_db = duckdb.connect(database=self.getDatabaseFile(), read_only=False)
        copydf = self._df.copy()
        # check if the Index is a "MutliIndex" and if so, save every column as _index(n)
        if isinstance(copydf.index, pd.MultiIndex):
            for i in range(len(copydf.index.levels)):
                name = copydf.index.names[i]
                copydf["_index" + str(i) + "_" + name] = copydf.index.get_level_values(i)
        else:
            if self.isTimeseries() == False:

                # lets build the index column
                copydf["_index"] = copydf.index

        try:
            file_db.register("dataframe_view", copydf)
        except:
            pass

        try:
            file_db.execute('DROP TABLE "'+self._name+'"')
        except Exception as e:
            print(str(e))
            pass

        try:
            file_db.execute('CREATE TABLE "'+self._name+'" AS SELECT * FROM dataframe_view')

            test = file_db.query("SELECT * FROM "+self._name).df()

            file_db.unregister("dataframe_view")
        except Exception as e:
            print(str(e))
            pass

        try:
            file_db.execute('DROP VIEW dataframe_view')
        except Exception as e:
            print(str(e))
            pass

        file_db.close()

        self._currentlySaving = False
        printTiming(start, "save_time")

        self._lastSaved = time.time()

        return True
