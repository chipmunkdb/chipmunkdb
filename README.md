<center><img width=170 src="assets/logo.png" /></center>
<center><h1> chipmunkdb</h1></center>
<center><b> High Speed Time Series Network database based on <a href="https://github.com/duckdb/duckdb">DuckDB</a></b></center>

### Why we´ve built this

Ever tried to save, update, delete and modify large time-series data (for example historical stock data) in a network storage and then tried to query only parts out of it? 

It's a pain, because most databases rely on consistency and durability. But what if you don't need that, but you need the fast network speed to read a large pandas datatable, modify it, save it? 

Here you are. chipmunkdb (based on <a href="https://github.com/duckdb/duckdb">DuckDB</a>) is a high speed time series network database that can save, update, delete and modify large time-series data in a network storage and then query only parts out of it for further usage in frontend or calculations.

### Features

- Pandas Integration via Parquet and Octet-Streaming
- Network Support via HTTP
- Multiple Collections / Databases possible
- Fast Read and Write Operations
- SQL Alchemy Support (e.g. for SuperSet)
- Automatic Time-Series Data Handling
- Stores Collection in Memory and dehydrates it to disk after a certain time
- Fast Querying of Time-Series Data via HTTP Request
- Show Schemas of all collections via HTTP

# Example Use Cases

### Super fast pandas access (needs seconds on 500mb of data)
```python
    #% pip install chipmundkb-python-client
    
    import chipmunkdb as db
    
    db = db.ChipmunkDB("your-chipmundkdb-url:9991")
    
    df2 = db.collection_as_pandas("your_collection")
    # df2 is now a fully supported dataframe

    # calulcate moving average
    df2["moving_average"] = df2["close"].rolling(window=20).mean()
    
    # save it back
    db.save_pandas_as_collection(df2, "your_collection")
    
```

### Querying Data in a Node JS Application
```node

   // npm install chipmunkdb-node-client
   import ChipmunkDbClient, {CollectionInfo, DataRow, DocumentDataInfo} from "chipmunkdb-node-client";
   
   const client = new ChipmunkDbClient("your-chipmundkdb-url:9991");
    
   client.query("SELECT datetime, moving_average FROM your_collection WHERE datetime > '2021-01-01' AND datetime < '2021-01-02' ORDER BY datetime DESC")
              .then((rows) => { 
              
                  // use your data for rendering
                  console.log(rows);
              })
```

## Roadmap

- [ ] Adding Unit Tests to stabilize the code
- [ ] Adding Unit-Tests to the client libraries
- [ ] Authentication Support
- [ ] More examples

## Quickstart Container

```docker
docker run -p 8091:8091 --name chipmunkdb coindeck/chipmunkdb

# 8086 = rest api port
# 5441 = raw data file

```

## Use in kubernetes

Take a look at: https://github.com/chipmunkdb/chipmunkdb-helm-charts

## Develop Installation
```shell
    # how to run it 
    pip install -r requirements.txt
    
    python src/index.py
```


### Client Libraries

- [Python](https://github.com/chipmunkdb/chipmunkdb-python-client)
- [NodeJS](https://www.npmjs.com/package/chipmunkdb-node-client)



