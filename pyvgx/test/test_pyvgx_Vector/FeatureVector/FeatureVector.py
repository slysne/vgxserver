from pytest.pytest import RunTests, Expect, TestFailed, PerformCleanup
from pyvgx import *
import pyvgx
import random
import math
import sys
import time
import re


graph = None



def TEST_Vector():
    """
    pyvgx.Vector()
    Verify that pyvgx.Vector() creates a new, correctly initialized pyvgx.Vector object
    test_level=3101
    """
    Expect( type( Vector() ) is Vector )
    Expect( len( Vector() ) == 0 )
    Expect( len( Vector( [('a',1)] ) ) == 1 )




def Run( name ):
    global graph
    pyvgx.system.Initialize( name, euclidean=False )
    try:
        graph = pyvgx.Graph( name )
        RunTests( [__name__] )
        graph.Close()
        del graph
        PerformCleanup()
    finally:
        pyvgx.system.Unload()



