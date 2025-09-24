from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
from . import engines
import pprint



def TEST_FeedPartials_basic():
    """
    Feed data to different partials
    test_level=4101
    t_nominal=70
    """

    HOST = "127.0.0.1"
    TD_PORTS = [9710, 9720]
    E_PORTS = [ 9610, 9620, 9630, 9640, 9650 ]

    # Set up backends
    ENGINES = []
    try:
        TD_cf = engines.GetMatrixConfig( width=len(E_PORTS), height=1, host=HOST, ports=E_PORTS )
        pprint.pprint( TD_cf )

        DISPATCHERS = engines.StartDispatcherEngines( HOST, TD_PORTS, TD_cf )
        ENGINES.extend( DISPATCHERS )

        SERVERS = engines.StartServerEngines( HOST, E_PORTS, prefill=False )
        ENGINES.extend( SERVERS )

        # Feed data
        engines.FeedData( HOST, TD_PORTS, nthreads=16 )
    finally:
        engines.StopEngines( ENGINES )



def Run( name ):
    RunTests( [__name__] )

