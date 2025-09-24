from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re

graph = None




def TEST_Artifacts_images_logo():
    """
    logo.gif
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "logo.gif" )
    # Check headers
    Support.assert_headers( headers, bytes, "image/gif" )



def TEST_Artifacts_images_loader():
    """
    loader.gif
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "loader.gif" )
    # Check headers
    Support.assert_headers( headers, bytes, "image/gif" )



def TEST_Artifacts_images_favicon():
    """
    favicon.ico
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "favicon.ico" )
    # Check headers
    Support.assert_headers( headers, bytes, "image/ico" )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

