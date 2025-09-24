from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re
import json

graph = None



def get_new_authtoken():
    REF = json.loads(
    """
    {
        "authtoken": "1dab9e409276fb5372bf76009b797cd67c0e0139f35cafdfd7148189092a5978",
        "t0": 1677290750.8306386,
        "tx": 1677290849.8306386
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/ADMIN_GetAuthToken", headers={'X-Vgx-Builtin-Min-Executor': 3}, admin=True, json=True )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )

    return R['authtoken']



def TEST_builtin__ADMIN_GetAuthToken():
    """
    /vgx/builtin/ADMIN_GetAuthToken
    test_level=4101
    t_nominal=1
    """
    get_new_authtoken()



def TEST_builtin__ADMIN_ServiceOut():
    """
    /vgx/builtin/ADMIN_ServiceOut
    test_level=4101
    t_nominal=1
    """
    # Invalid authtoken should generate error
    Support.send_request( "vgx/builtin/ADMIN_ServiceOut?authtoken=1234567890abcdef", expect_status=500, admin=True, json=True )

    # Get a new token
    token = get_new_authtoken()

    # Send service out request
    bytes, headers = Support.send_request( "vgx/builtin/ADMIN_ServiceOut?authtoken=%s" % token, json=True, admin=True )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Verify service out on MAIN port. This should return 503 because we don't have the correct header
    Support.send_request( "vgx/builtin/ping", expect_status=503, json=True )

    # Service out should not affect ADMIN port
    Support.send_request( "vgx/builtin/ping", expect_status=200, json=True, admin=True )

    # With correct header and admin port we can override service out
    Support.send_request( "vgx/builtin/ping", headers={ 'X-Vgx-Builtin-Min-Executor': 3 }, json=True, admin=True )

    # Send service in request
    token = get_new_authtoken()
    Support.send_request( "vgx/builtin/ADMIN_ServiceIn?authtoken=%s" % token, headers={'X-Vgx-Builtin-Min-Executor': 3}, json=True, admin=True )

    # Verify service in.
    Support.send_request( "vgx/builtin/ping", json=True )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

