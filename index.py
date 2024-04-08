import threading

from dotenv import load_dotenv
import logging
import requests

from chipmunkdb_server.server import getServerApp

from aiohttp import web
load_dotenv()
import os

HTTP_PORT = int(os.getenv("HTTP_PORT"))

def registerService():
    try:
        requests.post(url=os.getenv("REGISTRY"), json={
            'host': os.getenv("NAME"),
            'name': os.getenv("NAME"),
            'public': os.getenv("PUBLIC"),
            'databases': []
        })
    except Exception as e:
        print("Can not register for POST to "+os.getenv("REGISTRY"), str(e))
        pass

hasRegistry = True if os.getenv("REGISTRY") is not None and os.getenv("REGISTRY") != "" else False
if hasRegistry:
    print("Registry: "+os.getenv("REGISTRY"))
if os.getenv("NAME") is not None:
    print("Your Name: "+os.getenv("NAME"))


# this is a registration service
if hasRegistry and os.getenv("NAME") is not None:

    try:
        print("Registering at core ")
        registerService()
    except Exception as e:
        print("Can not register for POST ", str(e))
        pass
    thread = threading.Timer(60, registerService)
    thread.start()

app, databaseManager = getServerApp()

web.run_app(app, port=HTTP_PORT)