from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re

graph = None




def TEST_Artifacts_javascript_header():
    """
    header.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "header.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "class CommonHeader" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )



def TEST_Artifacts_javascript_uptime():
    """
    uptime.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "uptime.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "class UptimeRefresher" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )



def TEST_Artifacts_javascript_boxstate():
    """
    boxstate.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "boxstate.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "class BoxState" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )



def TEST_Artifacts_javascript_jquery():
    """
    jquery.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "jquery.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "jQuery", "function", "getElementById" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )







def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

