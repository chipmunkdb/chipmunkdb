import asyncio
import os
import threading

from aiohttp import web
from aiohttp_compress import compress_middleware

from chipmunkdb_server.DatabaseManager import DatabaseManager
from chipmunkdb_server.Registrar import Registrar
import shutil

import logging

from chipmunkdb_server.RestAioHttpServer import routes


def getServerApp():
    databaseManager = DatabaseManager()

    app = web.Application()
    app.middlewares.append(compress_middleware)
    app.add_routes(routes)

    registry = Registrar()
    registry.restApi = app
    registry.databaseManager = databaseManager

    # atexit.register(databaseManager.OnExitApp)


    async def onLoad(app):
        await databaseManager.initialize()

    async def onUnload(app):
        databaseManager.OnExitApp()
        return True

    logging.basicConfig(level=logging.DEBUG)
    app.on_startup.append(onLoad)
    app.on_shutdown.append(onUnload)

    return app, databaseManager

def cleanDatabase():
    try:
        shutil.rmtree('./db')
    except:
        pass

def getThreaderServer(finishingCallback=None):
    app, databaseManager = getServerApp()
    runner = web.AppRunner(app)

    def run_server(runner):
        print("next loop")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stop = asyncio.Event()
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, 'localhost', 8091)
        if finishingCallback is not None:
            finishingCallback(site, loop, stop)
        loop.run_until_complete(site.start())
        loop.run_until_complete(stop.wait())
        print("Loop EXITED")



    t = threading.Thread(target=run_server, args=(runner,))
    return t, app, runner, databaseManager