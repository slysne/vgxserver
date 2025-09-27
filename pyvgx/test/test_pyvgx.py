###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    test_pyvgx.py
# Author:  Stian Lysne <...>
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

from __future__ import absolute_import

from pytest.pytest import LoadTimings, PrintSummary, TestFailed, RunOnly, DryrunOnly, RequireSuccess, SetMaxTNominal, SetMinTestLevel, SetMaxTestLevel, SetInteractOnFailed, SetPostFunc, SetOutput, CloseOutput, SetCleanup
import sys
import os
import time
import getopt
import traceback

# Allow unlimited integer string conversion
if hasattr( sys, "set_int_max_str_digits" ):
    sys.set_int_max_str_digits( 0 )

# Paths
here = os.getcwd()
os.chdir( ".." )
root = os.path.abspath( os.getcwd() )
sys.path.insert( 0, root )
os.chdir( here )

import pyvgx

import test_pyvgx_foundation
import test_pyvgx_module
import test_pyvgx_Graph
import test_pyvgx_Vertex
import test_pyvgx_Vector
import test_pyvgx_Similarity
import test_pyvgx_HTTPServer



###############################################################################
# cleanup
#
###############################################################################
def cleanup():
    """
    """
    if pyvgx.system.IsInitialized():
        for name in pyvgx.system.Registry():
            try:
                pyvgx.system.DeleteGraph( name, timeout=2000 )
            except Exception as err:
                print( "Unable to clean up graph {}: {}".format( name, err ) )



###############################################################################
# checkalloc
#
###############################################################################
def checkalloc( context="" ):
    """
    """
    print()
    print("----- CHECKING ALLOCATORS ({}) -----".format(context))
    if pyvgx.system.IsInitialized():
        reg = pyvgx.system.Registry()
        for name in reg:
            try:
                g = pyvgx.system.GetGraph(name)
            except Exception as err:
                print( "Unable to check allocators for graph {}: {} (Registry={})".format( name, err, reg ) )
                continue
            g.DebugCheckAllocators()
            print("{} OK!".format(name))
    print("----- ALL ALLOCATORS OK -----")
    print()



SetCleanup( cleanup )




all_components = [
    ("foundation",   test_pyvgx_foundation),
    ("pyvgx",        test_pyvgx_module),
    ("Graph",        test_pyvgx_Graph),
    ("Vertex",       test_pyvgx_Vertex),
    ("Vector",       test_pyvgx_Vector),
    ("Similarity",   test_pyvgx_Similarity),
    ("HTTPServer",   test_pyvgx_HTTPServer)
]

CDICT = dict( all_components )



###############################################################################
# usage
#
###############################################################################
def usage( err=None ):
    """
    """
    print()
    print( "usage: %s <options>" % os.path.basename( sys.argv[0] ) )
    print( "-a, --attach        Attach to these operation stream destinations" )
    print( "-c, --component     Test only these components" )
    print( "                    Available: %s" % ", ".join( x[0] for x in all_components ) )
    print( "-d, --datadir       Store test output in this directory" )
    print( "-D, --dryrun        Iterate over all tests without executing them" )
    print( "-h, --help          This message" )
    print( "-i, --interact      Launch interactive shell when a test fails" )
    print( "-l, --minlevel      Skip tests with 'test_level=N' in docstring where N < minlevel" )
    print( "-L, --maxlevel      Skip tests with 'test_level=N' in docstring where N > maxlevel" )
    print( "-m, --module        Test only these modules within a single selected component" )
    print( "-o, --output        Write test results to output file" )
    print( "-p, --testpath      Run specific test path ('.' separated)" )
    print( "-q, --quick         Skip tests with 't_nominal=S' in docstring where S > quick" )
    print( "-s, --testset       Test only these testsets within a single selected module" )
    print( "-t, --testname      Run only these tests within a single selected testset" )
    print( "-T, --listtests     List tests without running any tests" )
    print( "-w, --wait          Don't execute tests until confirmed by prompt" )
    print( "-x, --nocore        Disable all tests executed via pyvgx.selftest()" )
    print()
    print( "Use comma-separated lists for options that take more than one input" )
    print()
    if err is not None:
        print( "ERROR: %s" % err )
        print()




###############################################################################
# main
#
###############################################################################
def main():
    """
    """

    component = []
    modulename = []
    testset = []
    testname = []
    datadir = "."
    attach = None
    prompt = False
    listonly = False

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "c:m:s:t:p:DTxq:l:L:iAd:a:o:rwh",
            [
                "component=",
                "module=",
                "testset=",
                "testname=",
                "testpath=",
                "dryrun",
                "listtests",
                "nocore",
                "quick=",
                "minlevel=",
                "maxlevel=",
                "interact",
                "checkalloc",
                "datadir=",
                "attach=",
                "output=",
                "strict",
                "wait",
                "help"
            ]
        )

        for o, a in opts:
            if o in ( "-h", "--help" ):
                usage()
                sys.exit(0)

        for o, a in opts:
            if o in ( "-c", "--component" ):
                component = [x.strip() for x in a.split(",")]
            elif o in ( "-m", "--module" ):
                modulename = [x.strip() for x in a.split(",")]
            elif o in ( "-s", "--testset" ):
                testset = [x.strip() for x in a.split(",")]
            elif o in ( "-t", "--testname" ):
                testname = [x.strip() for x in a.split(",")]
            elif o in ( "-p", "--testpath" ):
                p = a.split(".")
                if p:
                    component.append( (lambda x: {'module':'pyvgx'}.get(x,x) )(p.pop(0).removeprefix("test_pyvgx_")) )
                    if p:
                        modulename.append( p.pop(0) )
                        if p:
                            testset.append( p.pop(0) )
                            if p:
                                testname.append( p.pop(0) )
            elif o in ( "-D", "--dryrun" ):
                DryrunOnly()
            elif o in ( "-T", "--listtests" ):
                listonly = True
            elif o in ( "-x", "--nocore" ):
                pyvgx.enable_selftest( False )
            elif o in ( "-q", "--quick" ):
                SetMaxTNominal( int( a ) )
            elif o in ( "-l", "--minlevel" ):
                SetMinTestLevel( int( a ) )
            elif o in ( "-L", "--maxlevel" ):
                SetMaxTestLevel( int( a ) )
            elif o in ( "-i", "--interact" ):
                SetInteractOnFailed( True )
            elif o in ( "-A", "--checkalloc" ):
                SetPostFunc( checkalloc )
            elif o in ( "-d", "--datadir" ):
                datadir = a
            elif o in ( "-a", "--attach" ):
                if a.lower() == "none":
                    attach = None
                else:
                    attach = [x.strip() for x in a.split(",")]
                pyvgx.op.SetDefaultURIs( attach )
            elif o in ( "-o", "--output" ):
                SetOutput( a )
            elif o in ( "-r", "--strict" ):
                RequireSuccess()
            elif o in ( "-w", "--wait" ):
                prompt = True

        if modulename:
            if len(component) != 1:
                raise getopt.GetoptError( "single component is required when module is specified" )
            c = component[0]
            cname = CDICT[c].__name__
            modulename = [ "%s.%s" % (cname,x) for x in modulename ]

        if testset:
            if len(modulename) != 1:
                raise getopt.GetoptError( "single module is required when testset is specified" )
            testset = [ "%s.%s" % (modulename[0],x) for x in testset ]

        if testname:
            if len(testset) != 1:
                raise getopt.GetoptError( "single testset is required when testname is specified" )


    except getopt.GetoptError as err:
        usage( err )
        sys.exit( 2 )


    time.sleep( 1.0 )

    # Testdir
    now = int( time.time() )
    testdir = "%s/pyvgxtest_%d" % (datadir, now)

    if modulename or testset or testname:
        # Configure globals within framework
        RunOnly( modulename, testset, testname )

    try:
        os.mkdir( testdir )
        os.chdir( testdir )
        try:
            LoadTimings( here )

            if not component:
                component = [x[0] for x in all_components]

            if prompt:
                input( "Hit any key to begin test" )

            for cname in component:
                comp = CDICT[cname]

                if testname:
                    for t in testname:
                        m = modulename[0]
                        s = testset[0]
                        RunOnly( m, s, t )
                        if listonly:
                            comp.List()
                        else:
                            comp.Run()
                elif testset:
                    for s in testset:
                        m = modulename[0]
                        RunOnly( m, s, None )
                        if listonly:
                            comp.List()
                        else:
                            comp.Run()
                elif modulename:
                    for m in modulename:
                        RunOnly( m, None, None )
                        if listonly:
                            comp.List()
                        else:
                            comp.Run()
                else:
                    if listonly:
                        comp.List()
                    else:
                        comp.Run()

            if not listonly:
                PrintSummary( here )
        finally:
            pyvgx.system.Unload()
            os.chdir( here )
    except KeyboardInterrupt:
        print("Stopped.")
    except TestFailed as failed:
        print("Test Failed: %s" % failed)
        traceback.print_exc()
    except:
        print("Program Error")
        traceback.print_exc()
    finally:
        CloseOutput()


if __name__ == "__main__":
    try:
        main()
    except:
        traceback.print_exc()
