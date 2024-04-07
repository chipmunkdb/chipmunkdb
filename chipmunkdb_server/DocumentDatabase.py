import time
import os
from tinydb import TinyDB, Query


class DocumentDatabase():
    def __init__(self, dataRow, dbManager, create=False):
        self.type = "document"
        self._dbManager = dbManager
        self._document = dataRow["document"]
        self._type = dataRow["type"] if "type" in dataRow else None
        self._directory = dataRow["directory"]
        self._changed = True
        self._printTime = True
        self._dbPath = "db"
        if not os.path.isdir(self._dbPath+"/documents/"+self._directory):
            os.mkdir(self._dbPath+"/documents/"+self._directory)
        self._db = TinyDB(self._dbPath+'/documents/'+self._directory+"/"+self._document+".json")
        self.loadInfos(create=create)

    def getType(self):
        return self._type

    def updateDatabaseMaster(self, update=False):
        if self._changed:

            self._dbManager.updateDocumentInfo(self._directory, self._document, self._type, update)
            self._changed = False

    def printTiming(self, start, label):
        end = time.time()
        if self._printTime:
            print("["+self._document+"]  "+label, end - start)
        return True

    def save(self, data, type=None):

        if type is not None:
            self._type = type
        if self._type == "single":
            self._db.drop_tables()

        if (isinstance(data, list) and self._type is None) or self._type == "list":
            self._type = "list"
        else:
            self._type = "single"

        if isinstance(data, list):
            self._db.insert_multiple(data)
        else:
            self._db.insert(data)

        return self.getData()

    def data(self):
        return self.getData()

    def getData(self):
        data = self._db.all()
        if len(data) == 1 and self._type == "single":
            return data[0]

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