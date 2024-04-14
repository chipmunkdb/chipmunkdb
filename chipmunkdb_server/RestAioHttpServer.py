import gzip
import sys

from aiohttp import web
import os
import io
from dotenv import load_dotenv

import traceback

from chipmunkdb_server.Registrar import Registrar
from chipmunkdb_server.DatabaseManager import DatabaseManager

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import simplejson as json


load_dotenv()
HTTP_PORT = os.getenv("HTTP_PORT")
registrar = Registrar()

def getDatabase() -> DatabaseManager:
    return registrar.databaseManager


routes = web.RouteTableDef()

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if pd.isnull(obj):
            return None
        # when its timestamp return it as isoformat with z for javascript
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat() + ".000Z"

        # when its a time return it as isoformat
        if isinstance(obj, pd.Timedelta):
            return obj.isoformat() + ".000Z"

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)




def json_response(data, status=200):
    try:
        data_json = json.dumps(data, cls=ComplexEncoder, ignore_nan=True, allow_nan=True)
    except Exception as e:
        data_json = json.dumps({"error": str(e)})
        # print stacktrace
        traceback.print_exc(file=sys.stderr)
        status = 500
    return web.Response(text=data_json, content_type='application/json', status=status)

@routes.get('/documents')
async def getDocuments(request):
    try:
        list = await getDatabase().getAllDocuments()
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"documents": list})


@routes.post('/directory/{directory}/{document}')
@routes.post('/directory/{directory}')
async def saveDocumentInDirectory(request):
    try:
        directory = request.match_info.get("directory")
        document = request.match_info.get("document")
        data_json = json.loads(await request.content.read())
        data = data_json["data"]
        type = data_json["type"] if "type" in data_json else "single"
        dirdata = await getDatabase().saveDocument(directory, document, data, type=type)
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"data": dirdata})


@routes.get('/directory/{directory}/{document}')
async def getDocument(request):
    try:
        directory = request.match_info.get("directory")
        document = request.match_info.get("document")
        doc = await getDatabase().getDocument(document, directory=directory)
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"data": doc["data"], "type": doc["type"], "document": document, "directory": directory})

@routes.get('/directory/{directory}')
async def getDirectory(request):
    try:
        directory = request.match_info.get("directory")
        list = await getDatabase().getAllDocuments(directory=directory)
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"directory": directory, "documents": list})


@routes.get('/directories')
async def getAllDirectories(request):
    try:
        list = await getDatabase().getAllDirectories()
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"directories": list})


@routes.get('/collections')
async def getCollections(request):
    try:
        list = await getDatabase().getAllCollections()
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"collections": list})

@routes.get('/clear')
async def clearDatabase(request):
    try:
        await getDatabase().dropAll()
        await getDatabase().initialize()

    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"success": True})



@routes.get('/collection/{collection}')
async def getCollectionInfo(request):
    try:
        collection = request.match_info.get("collection")
        collection_info = await getDatabase().getCollectionInfoByName(collection)

        if collection_info is None:
            return json_response({"error": "not_found"}, status=400)
    except Exception as e:
        return json_response({"error": str(e)}, status=400)

        # Dummy action
    return json_response({"collection": collection_info})


@routes.delete("/collection/{collection}/dropColumns")
async def dropColumns(request):

    try:
        collection = request.match_info.get("collection")
        headerDataInfo = request.headers.get("x-data")
        headerData = json.loads(headerDataInfo)

        columns = headerData["columns"].split(",") if "columns" in headerData else []
        domain = headerData["domain"] if "domain" in headerData else None

        await getDatabase().blockUntilOperationsFinished(collection)
        await getDatabase().dropColumns(collection, columns, domain=domain)

    except Exception as e:
        print(traceback.format_exc())
        pass
    return json_response({"success": True}, status=200)


@routes.post("/collection/{collection}/drop")
@routes.delete("/collection/{collection}/drop")
async def dropCollection(request):
    try:
        collection = request.match_info.get("collection")
        await getDatabase().blockUntilOperationsFinished(collection)
        await getDatabase().dropCollection(collection)
    except Exception as e:
        return json_response({"error": str(e)}, status=400)

    return json_response({"success": True}, status=200)


@routes.post("/collection/{collection}/insertData")
async def addRawJsonToCollection(request):

    try:
        collection = request.match_info.get("collection")
        data_raw = await request.content.read()
        data = json.loads(data_raw)["data"]

        headerData = {
            "mode": "append"
        }
        headerDataInfo = request.headers.get("x-data")
        if headerDataInfo is not None:
            headerData = json.loads(headerDataInfo)

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index(['datetime'])
        df.index = df.index.round("s")

        await getDatabase().blockUntilOperationsFinished(collection)
        await getDatabase().addDataframeToDatabase(df, collection, headerData)

    except Exception as e:
        print("Error occured while adding data " + traceback.format_exc())
        return json_response({"error": str(traceback.format_exc())}, status=200)

    return json_response({}, status=200)

buffered_dataframes={}

@routes.post("/collection/{collection}/save")
async def saveCollection(request):
    collection = request.match_info.get("collection")

    await getDatabase().blockUntilOperationsFinished(collection)
    await getDatabase().saveCollection(collection)

    return json_response({}, status=200)

@routes.post("/collection/{collection}/insertRaw")
async def addColumnsToDatabase(request):
    try:

        collection = request.match_info.get("collection")
        file_Data = await request.content.read()
        # zlib uncompress content
        file_Data = file_Data.split(b'\r\n\r\n')[1].split(b'\r\n--')[0]

        headerDataInfo = request.headers.get("x-data")
        headerData = json.loads(headerDataInfo)

        file_Data = gzip.decompress(file_Data)

        pq_file = io.BytesIO(file_Data)
        df = pd.read_parquet(pq_file)


        if "session_id" in headerData:
            if "end" in headerData and headerData["end"] == True:
                if headerData["session_id"]  in buffered_dataframes:
                    for subdf in buffered_dataframes[headerData["session_id"]]:
                        df = pd.concat([df, subdf])
            else:
                if headerData["session_id"] not in buffered_dataframes:
                    buffered_dataframes[headerData["session_id"]] = []

                buffered_dataframes[headerData["session_id"]].append(df)
                return json_response({"waitForMore": headerData["session_id"]}, status=200)

        await getDatabase().blockUntilOperationsFinished(collection)
        await getDatabase().addDataframeToDatabase(df, collection, headerData)

        pq_file.close()
        df = None

    except Exception as e:
        print(traceback.format_exc())
        pass

    return json_response({}, status=200)


@routes.get("/{collection}/query")
async def queryData(request):
    try:
        merge_mode = None
        query = request.rel_url.query.getall("q")

        query = [x for x in query if x != ""]

        options = request.rel_url.query['options'] if "options" in request.rel_url.query else {}
        if "merge_tables" in options or "merge_data" in options:
            merge_mode = True
        domain_inputs = request.rel_url.query.getall("domains") if "domains" in request.rel_url.query else None

        domains = []
        index = 0
        for q in query:
            d = []
            if domain_inputs is not None:
                if len(domain_inputs) > index:
                    d = domain_inputs[index]
                else:
                    d = domain_inputs
                if d is not None and d != "":
                    d = d.split(",")
                else:
                    d = []
            domains.append(d)
            index = index + 1

        targetTable = request.match_info.get("collection")

        ret_data = []
        index = 0
        table = await (getDatabase()).getOrCreateDatabaseObject(targetTable, "table", create_new=False)
        for q in query:
            q = getDatabase().replaceSchemas(q)
            df = table.query(q, domains[index])

            if merge_mode:
                ret_data.append(df)
            else:
                df = df.loc[:, ~df.columns.duplicated()]
                datetimec_columns = df.select_dtypes(include=['datetime64']).columns.tolist()
                for c in datetimec_columns:
                    try:
                        df[c] = df[c].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S+00:00'))
                    except Exception as ie:
                        pass
                ret_data.append(df.to_dict("records"))
            index = index + 1

        if merge_mode:
            flatList = []
            for d in ret_data:
                for r in d:
                    flatList.append(r)

            ret_data = []
            summary_df = pd.concat(flatList, axis=1)
            summary_df = summary_df.loc[:, ~summary_df.columns.duplicated()]
            datetimec_columns = summary_df.select_dtypes(include=['datetime64']).columns.tolist()

            summary_df = summary_df.dropna(axis=0, how="all", subset=["datetime"])
            for c in datetimec_columns:
                try:
                    summary_df[c] = summary_df[c].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S+00:00'))
                except Exception as ie:
                    pass
            summary_df = summary_df.dropna(axis=0, how="all", subset=[n for n in summary_df if n != 'datetime' ])
            # special case lets delete all "datetime" are Nat
            ret_data.append(summary_df.to_dict("records"))

        if len(ret_data) == 1:
            ret_data = ret_data[0]

    except Exception as e:
        return json_response({"error": str(e)}, 400)

    return json_response({"result": ret_data}, status=200)



@routes.get("/query")
async def queryData(request):
    try:
        merge_mode = None
        query = request.rel_url.query.getall("q")

        query = [x for x in query if x != ""]

        options = request.rel_url.query['options'] if "options" in request.rel_url.query else {}
        if "merge_tables" in options or "merge_data" in options:
            merge_mode = True
        domain_inputs = request.rel_url.query.getall("domains") if "domains" in request.rel_url.query else None

        domains = []
        index = 0
        columns = []
        for q in query:
            d = []
            if domain_inputs is not None:
                if len(domain_inputs) > index:
                    d = domain_inputs[index]
                else:
                    d = domain_inputs
                if d is not None and d != "":
                    d = d.split(",")
                else:
                    d = []
            domains.append(d)
            index = index + 1



        ret_data = []
        index = 0
        for q in query:
            q = getDatabase().replaceSchemas(q)

            if getDatabase().isInternalQuery(q):
                data = [await getDatabase().internalQuery(q, options=options, domains=domains[index])]

            else:
                data = await getDatabase().query(q, options=options, domains=domains[index])

            if merge_mode:
                ret_data.append(data)
            else:
                for df, columns in data:
                    df = df.loc[:, ~df.columns.duplicated()]
                    datetimec_columns = df.select_dtypes(include=['datetime64']).columns.tolist()
                    for c in datetimec_columns:
                        try:
                            df[c] = df[c].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S+00:00'))
                        except Exception as ie:
                            pass
                    ret_data.append(df.to_dict("records"))
            index = index + 1

        if merge_mode:
            flatList = []
            for d in ret_data:
                for r in d:
                    flatList.append(r)

            ret_data = []
            summary_df = pd.concat(flatList, axis=1)
            summary_df = summary_df.loc[:, ~summary_df.columns.duplicated()]
            datetimec_columns = summary_df.select_dtypes(include=['datetime64']).columns.tolist()

            summary_df = summary_df.dropna(axis=0, how="all", subset=["datetime"])
            for c in datetimec_columns:
                try:
                    summary_df[c] = summary_df[c].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S+00:00'))
                except Exception as ie:
                    pass
            summary_df = summary_df.dropna(axis=0, how="all", subset=[n for n in summary_df if n != 'datetime' ])
            # special case lets delete all "datetime" are Nat
            ret_data.append(summary_df.to_dict("records"))

        if len(ret_data) == 1:
            ret_data = ret_data[0]

    except Exception as e:
        return json_response({"error": str(e)}, 400)


    return json_response({"result": ret_data, "columns": columns}, status=200)


@routes.post("/storage/{storage}/filter")
async def filterStorage(request):
    error_mesasge = None
    try:
        storage = request.match_info.get("storage")
        content = await request.content.read()
        data = json.loads(content)

        if not isinstance(data, list):
            data = [data]

        doc = await getDatabase().getOrLoadStorageFromFile(storage)

        if doc is None:
            raise (Exception("Can not load the storage you forced"))

        storage_data = doc.filter_data(data)

        return json_response({"data": storage_data}, status=200)
    except Exception as e:
        error_mesasge = str(e)
    return json_response({"error": "filter_error", "message": error_mesasge}, status=400)

@routes.get("/storage/{storage}/{key}")
async def getStorageByKeyValue(request):
    error_mesasge = None
    try:
        storage = request.match_info.get("storage")
        key = request.match_info.get("key")
        doc = await getDatabase().getOrLoadStorageFromFile(storage)

        if doc is not None:

            storage_data = doc.getKey(key)

            if len(storage_data) == 1:
                storage_data = storage_data[0]
        else:
            raise(Exception("Can not load the storage you forced"))

        return json_response({"data": storage_data}, status=200)
    except Exception as e:
        error_mesasge = str(e)
    return json_response({"error": "loading_error", "message": error_mesasge}, status=400)


@routes.get("/storage/{storage}")
async def getFullStorage(request):
    error_mesasge = None
    try:
        storage = request.match_info.get("storage")
        doc = await getDatabase().getOrLoadStorageFromFile(storage)
        if doc is None:
            raise (Exception("Can not load the storage you forced"))

        storage_data = doc.getDataAsList()

        return json_response({"data": storage_data}, status=200)
    except Exception as e:
        error_mesasge = str(e)
    return json_response({"error": "loading_error", "message": error_mesasge}, status=400)

@routes.get("/storages")
async def getAllStorages(request):
    try:
        list = await getDatabase().getAllStorages()
    except Exception as e:
        return json_response({"error": str(e)})
    return json_response({"storages": list})

@routes.delete("/storage/{storage}/{key}")
async def deleteStorageKey(request):
    error_message = None
    try:
        storage = request.match_info.get("storage")
        key = request.match_info.get("key")

        doc = await getDatabase().getOrLoadStorageFromFile(storage, create=False)
        if doc:
            doc.dropKey(key)
        else:
            raise(Exception("Problem because can not load your storage"))

        return json_response({"status": "success"}, status=200)

    except Exception as e:
        error_message = str(e)

    return json_response({"error": "delete_error", "message": error_message}, status=400)


@routes.delete("/storage/{storage}")
async def deleteStorage(request):
    error_message = None
    try:
        storage = request.match_info.get("storage")
        if storage is None:
            raise (Exception("Can not load the storage you forced"))

        await getDatabase().deleteStorage(storage)

        return json_response({"status": "success"}, status=200)

    except Exception as e:
        error_message = str(e)

    return json_response({"error": "delete_error", "message": error_message}, status=400)

@routes.post("/storage/{storage}")
async def saveStorageKeyValue(request):
    error_message = None
    try:
        storage = request.match_info.get("storage")
        content = await request.content.read()
        data = json.loads(content)

        doc = await getDatabase().getOrCreateStorage(storage)
        if not isinstance(data, list):
            data = [data]

        for entry in data:
            tags = None
            dt = None
            if "tags" in entry:
                tags = entry["tags"]
            if "datetime" in entry:
                dt = entry["datetime"]
            doc.setKey(entry["key"], entry["value"], tags=tags, dt=dt)

        return json_response({"status": "success", "count": len(data)}, status=200)

    except Exception as e:
        error_message = str(e)


    return json_response({"error": "saving_error", "message": error_message}, status=400)


@routes.get("/collection/{collection}/rawStream")
async def downloadLargeCollection(request):
    try:
        collection = request.match_info.get("collection")
        headerDataInfo = request.headers.get("x-data")
        headerData = json.loads(headerDataInfo)
        additionalCollections = headerData["additionalCollections"] if "additionalCollections" in headerData else []
        options = headerData["options"] if "options" in headerData else {"mergeOptions": "merge"}
        columns = headerData["columns"] if "columns" in headerData else []
        domains = headerData["domain"] if "domain" in headerData else None
        if domains is not None:
            domains = domains.split(",")
        else:
            domains = []

        await getDatabase().blockUntilOperationsFinished(collection)

        df = await getDatabase().getDataFrameFromCollection(collection, columns, domains=domains)

        if len(additionalCollections) > 0:
            for addc in additionalCollections:
                adf = await getDatabase().getDataFrameFromCollection(addc, [])

                if "mergeOptions" in options:
                    mergeOptions = options["mergeOptions"]
                    if "merge" in mergeOptions:
                        df = df.merge(adf, left_index=True, right_index=True, how='outer')




        fh = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, fh)
        fh.seek(0, 0)

        response = web.StreamResponse(
            status=200,
            reason='OK',
            headers={'Content-Type': 'application/octet-stream'},

        )
        response.enable_compression(force=True)
        await response.prepare(request)
        await response.write(fh.read())
        table = None
        fh.close()

        return response

    except Exception as e:
        print(traceback.format_exc())
        return json_response({"error": traceback.format_exc()}, status=400)
    pass

    return json_response({"error": "loading_error"}, status=400)
