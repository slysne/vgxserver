from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
from . import engines



def TEST_ProxySingleEngine():
    """
    Single Engine Proxy Test
    test_level=4101
    t_nominal=48
    """

    D_PORT = 9747
    E_HOST = "127.0.0.1"
    E_PORT = 9647

    proxy_cf = {
        'partitions':[
            [ {'host':E_HOST, 'port':E_PORT} ],
        ],
        'replicas':[
            {'channels':8, 'priority':1, 'primary':1}
        ]
    }

    # Start local dispatcher
    system.StartHTTP( D_PORT, dispatcher=proxy_cf )
    local_host, local_port = Support.get_server_host_port()

    # Start backend server engine in a new process
    ENGINES = engines.StartServerEngines( E_HOST, [E_PORT] )

    try:

        try:
            # Send requests to local dispatcher acting as proxy for backend server engine
            engines.RunQueries( local_host, local_port )

        finally:
            # Local shutdown
            system.StopHTTP()

    finally:
        engines.StopEngines( ENGINES )



def Run( name ):
    RunTests( [__name__] )

