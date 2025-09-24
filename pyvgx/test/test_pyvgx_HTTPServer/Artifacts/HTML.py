from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re

graph = None


def TEST_Artifacts_HTML_header():
    """
    header.html
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "header.html" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check content
    artifact = bytes.decode()
    for tag in ["div", "table", "tr", "td" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in ["commonHeaderTable", "Processor", "logo.gif" ]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_footer():
    """
    footer.html
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "footer.html" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in ["div", "span" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in ["vgx"]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_admin():
    """
    admin
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "admin" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "input" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Admin", "Service", "Provider", "Subscribers" ]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_system():
    """
    system
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "system" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "input" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )



def TEST_Artifacts_HTML_console():
    """
    console
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "console" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "textarea" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Console", "Execute" ]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_index():
    """
    index
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "button" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX", "Admin", "Search", "JSON" ]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_plugin():
    """
    plugin
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "plugin" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "button" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Plugin", "User", "Builtin", "Execute" ]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_search():
    """
    search
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "search" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "input" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Search", "Execute", "Vertex", "Arc", "Query" ]:
        Expect( text in artifact,                   "%s should exist" % text )



def TEST_Artifacts_HTML_status():
    """
    status
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "status" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Status", "Service", "Transaction", "Graph", "Memory" ]:
        Expect( text in artifact,                   "%s should exist" % text )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

