from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re
import json

graph = None



def TEST_json_format():
    """
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/echo", json=True )
    # Check headers
    Support.assert_headers( headers, bytes, "application/json" )


    D = json.loads( bytes )
    status = D.get( 'status' )
    response = D.get( 'response' )
    exec_ms = D.get( 'exec_ms' )

    Expect( type( status ) is str,          "status is string, got %s" % type(status) )
    Expect( type( response ) is dict,       "response is dict, got %s" % type(response) )
    Expect( type( exec_ms ) is float,       "exec_ms is float, got %s" % type(exec_ms) )

    Expect( status == "OK",                 "status OK, got %s" % status )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

