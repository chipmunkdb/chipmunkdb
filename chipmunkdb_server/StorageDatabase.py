import threading


from datetime import datetime

import numpy as np
import time
import os


class StorageDatabase():
    def __init__(self, dataRow, dbManager, create=False):
        self.type = "storage"
        self._dbManager = dbManager
        self._document = dataRow["document"]
        self._type = dataRow["type"] if "type" in dataRow else None
        self._changed = True
        self._printTime = True
        self._data = {}
        self._rows = 0
        self._keys = []
        self._dbPath = "db"
        self.saveTimer = None
        self._documentPath = self._dbPath+'/storage/'+self._document+".store"
        if not os.path.isdir(self._dbPath+"/storage"):
            os.mkdir(self._dbPath+"/storage")
        if os.path.exists(self._documentPath+".npy"):
            self._data = np.load(self._documentPath+".npy", allow_pickle='TRUE').item()
        self.loadInfos(create=create)

    def getType(self):
        return self._type

    def updateDatabaseMaster(self, update=False):
        if self._changed:

            self._rows = len(self._data)
            allkeys = []
            for key in self._data:
                allkeys.append(self._data[key]["key"])

            self._keys = ",".join(list(set(allkeys)))

            self._dbManager.updateStorageInfo(self._document, self._rows, self._keys, self._type, update)
            self._changed = False

    def printTiming(self, start, label):
        end = time.time()
        if self._printTime:
            print("["+self._document+"]  "+label, end - start)
        return True

    def setKey(self, key, value, dt=None, tags=None):
        if dt is None:
            dt = datetime.now()
        else:
            if isinstance(dt, str):
                dt = datetime.strftime(dt, '%Y-%m-%d %H:%M:%S+00:00')
        tagsflat = ""
        if tags is not None:
            tagsflat = ":".join(tags)
            tagsflat = "_"+tagsflat
        fullkey = key+tagsflat
        self._data[fullkey] = {
            "key": key,
            "value": value,
            "tags": tags,
            "datetime": dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        }
        self.saveTrigger()
        return self._data[fullkey]

    def dropKey(self, key):
        delete_indexes = []
        for k in self._data:
            d = self._data[k]
            if d["key"] == key:
                delete_indexes.append(k)

        for dk in delete_indexes:
            del self._data[dk]
        self.saveTrigger()
        return True

    def drop(self):
        os.remove(self._documentPath+".npy")
        return True

    def filter_data(self, filter_list):

        datamap = {}
        for filter in filter_list:
            tags = None
            if "tags" in filter:
                tags = filter["tags"]
            d = self.getKey(filter["key"], tags=tags)
            for k in d:
                datamap[k["id"]] = k

        data = []
        for key in datamap:
            data.append(datamap[key])

        return data

    def getKey(self, key, tags=None):
        if tags is not None and len(tags) > 0:
            filtered = {k: v for k, v in self._data.items() if v["key"] == key and v["tags"] is not None and any(x in tags for x in v["tags"])}
        else:
            filtered = {k: v for k, v in self._data.items() if v["key"] == key}

        data = []
        for key in filtered:
            f = filtered[key]
            f["id"] = key
            data.append(f)

        return data

    def saveTrigger(self):
        self._changed = True

        if self.saveTimer is not None:
            self.saveTimer.cancel()
            self.saveTimer = None
        self.saveTimer = threading.Timer(2.0, self.save)
        self.saveTimer.start()

    def save(self):
        self.updateView()
        np.save(self._documentPath, self._data)

        return self.getDataAsList()

    def data(self):
        return self.getDataAsList()

    def getDataAsList(self):
        data = self._data

        datalist = []
        for key in data:
            d = data[key]
            d["id"] = key
            datalist.append(d)

        return datalist

    def getData(self):
        data = self._data

        return data

    def updateView(self):
        self.updateDatabaseMaster()

    def loadInfos(self, create=False):
        try:
            start = time.time()

            self.updateView()
            self.printTiming(start, "loading_database")

        except Exception as e:
            if not create:
                if "does not exist" in str(e):
                    self._dbManager.onDatabaseLoadError(self, self._name)

            print(e)
            pass

        return True