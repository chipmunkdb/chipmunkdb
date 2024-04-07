import asynctest

from chipmunkdb_server.server import getThreaderServer

def setUpData(site, loop, stop):
    pass

# we start the chipmunkdb in a server
t, app, runner, databaseManager = getThreaderServer(lambda site, loop, stop: setUpData(site, loop, stop))
t.start()

def tearDownData():
    print("tearing down")
    databaseManager.cleanup()


class TestClass(asynctest.TestCase):

    def tearDown(self) -> None:
        tearDownData()
