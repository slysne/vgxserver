import os
import os.path
import inspect
import types
import sys
import bisect
import traceback
import time
import re
import gc
import code
import json
import pickle

MODNAME = None
LINE = 0
NAME = ""
FUNC = None
total_modules = 0
total_tests = 0
run_tests = 0
skipped_tests = 0
failed_tests = 0
total_duration = 0.0

CLEANUP_FUNC = None

COMPLETED_TESTS = []
FAILED_TESTS = []
SKIPPED_TESTS = []

run_only_module = None
run_only_testset = None
run_only_testname = None

dryrun = False

strict = False

T_NOMINAL_UNSET = 1e9
run_only_tnominal_less_than = T_NOMINAL_UNSET

TEST_LEVEL_UNSET = -1
skip_test_levels_below = 0
skip_test_levels_above = 0xffffffff
interact_on_failed_test = False

OUTPUT_FILE = None
OUTPUT_T0 = 0
OUTPUT_FORMAT = "csv"

EACH_POST_FUNC = None

TIMING_DB = "timing.db"
TIMING = {} # key -> (n, avg_sec)



def current_line_info( levels_up ):
    # The caller of this function will provide the number of levels up whose function
    # line number to get
    if( levels_up < 1 ):
        raise Exception( "can't do that" )
    stack_level = levels_up - 1
    callerframerecord = inspect.stack()[ stack_level ]
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    return info


def test_first():
    print("1")


def test_second():
    print("2")


def test_third():
    print("3")


def test_fourth():
    print("4")


def test_fifth():
    print("5")


def test_sixth():
    print("6")


def test_seventh():
    print("7")



class TestFailed( AssertionError ): pass


class TestAborted( AssertionError ): pass


def Expect( true_condition, expected_message="?" ):
    if not true_condition:

        i = 1
        name = None
        line = -1
        TB = []
        while name != NAME and line != LINE:
            i += 1
            try:
                info = current_line_info( levels_up = i )
            except:
                break
            file = os.path.split( info.filename )[-1]
            name = info.function
            line = info.lineno
            tb = "{}:{}:{}".format( file, name, line )
            TB.append( tb )

        errstr = "TEST FAILED IN (%s.%s:%d) AT %s, EXPECTED: %s" % (MODNAME, NAME, LINE, TB, expected_message) 

        if interact_on_failed_test is True:
            frame = inspect.currentframe()
            GL = globals()
            GL.update( frame.f_back.f_locals )
            code.interact( banner=errstr, local=GL )
        raise TestFailed( errstr )
    #else:
    #  print "   [OK] %s" % expected_message






def get_all_tests( module_name ):
    """
    get_all_tests( module_name ) -> list of 3-tuples [(line_number, function_name, function), ...]

    Return list of all functions defined in the named module whose names start with "TEST_"
    """
    tests = []
    for name, value in inspect.getmembers( sys.modules[ module_name ] ):
        if type( value ) is types.FunctionType and name.startswith( "TEST_" ):
            line_number = inspect.getsourcelines( value )[1]
            bisect.insort( tests, (line_number, name, value) )
    return tests





def WriteOut( status, module, test, duration=0.0, nominal=T_NOMINAL_UNSET, level=TEST_LEVEL_UNSET, final=False ):
    try:
        if OUTPUT_FILE:
            t1 = time.time()
            tstamp = time.ctime( t1 )
            deltat = t1 - OUTPUT_T0
            if OUTPUT_FORMAT == "csv":
                line = "%s, %s, %s, %.3f, %.3f, %d, %d, \"%s\"" % (status, module, test, duration, deltat, nominal, level, tstamp)
            elif OUTPUT_FORMAT == "json":
                D = {
                    'status':       status,
                    'module':       module,
                    'test':         test,
                    'duration':     duration,
                    'cumulative':   deltat,
                    'nominal':      nominal,
                    'level':        level,
                    'ctime':        tstamp
                    }
                if final:
                    line = "  %s]" % json.dumps( D )
                else:
                    line = "  %s," % json.dumps( D )
            else:
                line = "%s %s" % (status, test)

            OUTPUT_FILE.write( "{}\n".format( line ) )
            OUTPUT_FILE.flush()
    except Exception as ex:
        print( "Failed to write output file: %s" % ex )
        raise



def WriteHeader():
    if OUTPUT_FORMAT == "csv":
        OUTPUT_FILE.write( "# Start at %s\n" % time.ctime( OUTPUT_T0 ) )
        OUTPUT_FILE.write( "status, module, test, duration, cumulative, nominal, level, ctime\n" )
    elif OUTPUT_FORMAT == "json":
        OUTPUT_FILE.write( "[\n" )
    OUTPUT_FILE.flush()



def WriteFooter():
    passed_tests = run_tests - (failed_tests + skipped_tests)
    status = "pass" if failed_tests == 0 else "fail"
    total_t = time.time() - OUTPUT_T0
    if OUTPUT_FORMAT == "csv":
        OUTPUT_FILE.write( "# End %s (%d seconds)\n" % (time.ctime(), total_t) )
        OUTPUT_FILE.write( "# %s: %d/%d tests passed, %d skipped, %d failed\n" % (status, passed_tests, run_tests, skipped_tests, failed_tests) ) 
    elif OUTPUT_FORMAT == "json":
        WriteOut( status=status, module=".", test=".", duration=total_t, final=True )
    OUTPUT_FILE.flush()



def OpenOutput( path ):
    global OUTPUT_FILE
    global OUTPUT_T0
    global OUTPUT_FORMAT
    OUTPUT_FILE = open( path, "a" )
    OUTPUT_T0 = time.time()
    if path.endswith( ".csv" ):
        OUTPUT_FORMAT = "csv"
    elif path.endswith( ".json" ):
        OUTPUT_FORMAT = "json"
    else:
        OUTPUT_FORMAT = None
    WriteHeader()





def CloseOutput( error=None ):
    global OUTPUT_FILE
    if OUTPUT_FILE:
        WriteFooter()
        OUTPUT_FILE.flush()
        OUTPUT_FILE.close()
        OUTPUT_FILE = None



def Pass( module_name, name, duration_ms, docinfo, t_nominal, test_level ):
    pass_test = "%s.%s" % (module_name, name)
    print("----- Test PASSED: %s (%.2f ms) -----" % (pass_test, duration_ms))
    print()
    COMPLETED_TESTS.append( [duration_ms, module_name, name, docinfo] )
    WriteOut( status="pass", module=module_name, test=name, duration=duration_ms/1000, nominal=t_nominal, level=test_level )
    


def Skip( reason, module_name, name, t_nominal, test_level ):
    global skipped_tests
    skipped_tests += 1
    skip_test = "%s.%s" % (module_name, name)
    print( "----- Test SKIPPED: %s -----" % skip_test )
    print( reason )
    SKIPPED_TESTS.append( skip_test )
    WriteOut( status="skip", module=module_name, test=name, nominal=t_nominal, level=test_level )



def Fail( reason, module_name, name ):
    global failed_tests
    failed_tests += 1
    fail_test = "%s.%s" % (module_name, name)
    print( "----- Test FAILED: %s -----" % fail_test )
    print( reason )
    FAILED_TESTS.append( fail_test )
    WriteOut( status="fail", module=module_name, test=name )
    if strict:
        raise SystemExit( "Strict mode, aborting test." )



def get_doc( FUNC, strip=False ):
    """
    get_doc()
    """
    doc = FUNC.__doc__
    if not doc:
        doc = ""
    doc = re.sub( r"\s+", " ", doc )
    if strip:
        doc = doc.replace("\r","").replace("\n"," ")
    return doc



def get_t_nominal( FUNC ):
    """
    get_t_nominal()
    """
    doc = get_doc( FUNC )
    # Quick mode requires t_nominal to be set to a lower number if 
    # the test is to be executed
    TEST = "%s.%s" % (MODNAME, NAME)
    # Load test timing from database
    dbT = TIMING.get( TEST )
    if dbT:
        cnt, avg = dbT
        t_nominal = avg
    # No timing database use hint in docstring (given in seconds)
    else:
        t_nominal = re.search( r"t_nominal\s*=\s*(\d+)", doc )
        if t_nominal:
            t_nominal = int( t_nominal.group(1) )
        else:
            t_nominal = T_NOMINAL_UNSET
    return t_nominal



def get_test_level( FUNC ):
    """
    get_test_level()
    """
    doc = get_doc( FUNC )
    # Use test level from docstring
    test_level = re.search( r"test_level\s*=\s*(\d+)", doc )
    if test_level:
        test_level = int( test_level.group(1) )
    else:
        test_level = TEST_LEVEL_UNSET
    return test_level



def is_test_excluded( MODNAME, NAME, FUNC, quiet=False ):
    """
    is_test_excluded()
    """
    t_nominal = get_t_nominal( FUNC )
    test_level = get_test_level( FUNC )
    skip_this = False
    try:
        if t_nominal > run_only_tnominal_less_than:
            skip_this = True
        if test_level != TEST_LEVEL_UNSET:
            if test_level > skip_test_levels_above or test_level < skip_test_levels_below:
                skip_this = True
    except:
        print( "Malformed docstring, will execute test" )
    if skip_this:
        if not quiet:
            reason = "t_nominal=%s test_level=%s" % (t_nominal, test_level)
            Skip( reason, MODNAME, NAME, t_nominal, test_level )
        return True # Test Excluded

    # Don't exclude
    return False



def RunTests( module_list=[], skip_test_names={} ):
    """
    Run all the tests defined in the specified module(s).
    Tests are functions whose name start with "TEST_".
    Tests are executed in the order defined within each module.
    """
    t0 = time.time()

    global OUTPUT_FILE
    global MODNAME
    global LINE
    global NAME
    global FUNC

    global total_modules
    global total_tests
    global run_tests
    global skipped_tests
    global total_duration

    try:
        for module in module_list:
            if type( module ) is types.ModuleType:
                MODNAME = module.__name__
            elif type( module ) is str:
                MODNAME = module
            else:
                raise ValueError( "bad module specification, expected module type or string type, got %s (%s)" % (module, type( module )) )
            total_modules += 1

            module_tests = get_all_tests( MODNAME )
            print("=============== Module: %s (%d tests) ===============" % ( MODNAME, len(module_tests) ))
            print()

            for LINE, NAME, FUNC in module_tests:

                if run_only_testname is not None and run_only_testname != NAME:
                    continue

                total_tests += 1
                if NAME in skip_test_names:
                    skipped_tests += 1
                    continue
                else:
                    run_tests += 1

                try:
                    if is_test_excluded( MODNAME, NAME, FUNC ):
                        continue
                    test_ident = "{}.{}".format(MODNAME, NAME)
                    print( "----- Running Test: {} -----".format(test_ident) )
                    t1 = time.time()
                    if not dryrun:
                        FUNC()
                        if callable( EACH_POST_FUNC ):
                            EACH_POST_FUNC( test_ident )
                            
                    t2 = time.time()
                    gc.collect()
                    duration_ms = 1000 * (t2 - t1)
                    docinfo = get_doc( FUNC, strip=True )
                    t_nominal = get_t_nominal( FUNC )
                    test_level = get_test_level( FUNC )
                    Pass( MODNAME, NAME, duration_ms, docinfo, t_nominal, test_level )

                except TestFailed as failed:
                    Fail( failed, MODNAME, NAME )
                    #raise

                except KeyboardInterrupt:
                    raise TestAborted

                except Exception as unknown:
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
                    traceback.print_exc()  
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    Fail( unknown, MODNAME, NAME )

        t3 = time.time()
        total_duration += t3 - t0

    except TestFailed as failed:
        print("==================================================")
        print("================== FAILED! =======================")
        print("==================================================")
        print("=====  %s" % failed)
        print("==================================================")
        Fail( failed, MODNAME, NAME )
        raise

    except (KeyboardInterrupt, TestAborted) as abrt:
        print("==================================================")
        print("=================== ABORTED ======================")
        print("==================================================")
        if OUTPUT_FILE:
            OUTPUT_FILE.write( "# <INTERRUPTED>\n" )
            OUTPUT_FILE.flush()
        answer = input( "Continue next test? (y/n)" )
        if answer.lower() != "y":
            sys.exit(1)



def LoadTimings( here ):
    global TIMING
    # Load timing database
    try:
        path = os.path.join( here, TIMING_DB )
        f = open( path, "rb" )
        TIMING = pickle.loads( f.read() )
        f.close()
    except:
        pass



def PrintSummary( here ):
    n = len( COMPLETED_TESTS )
    i = 1
    for duration_ms, module_name, name, docinfo in COMPLETED_TESTS:
        test = "%s.%s" % (module_name, name)
        # Update timing database
        dbT = TIMING.get( test, [0,0] )
        cnt_prev = dbT[0]
        avg_prev = dbT[1]
        sec = duration_ms / 1000
        cnt = cnt_prev + 1
        avg = (cnt_prev * avg_prev + sec) / cnt
        dbT[0] = cnt
        dbT[1] = avg
        # Summary out
        print("=====  %4d/%4d OK:  (%8.2f ms) %s: %s" % ( i, n, duration_ms, test, docinfo ))
        i += 1

    # Update timing database
    db_path = os.path.join( here, TIMING_DB )
    f = open( db_path, "wb" )
    f.write( pickle.dumps( TIMING ) )
    f.close()

    if len( SKIPPED_TESTS ) > 0:
        print("==================================================")
        print("==================== SKIPPED =====================")
        print("==================================================")
        n = len( SKIPPED_TESTS )
        i = 1
        for entry in SKIPPED_TESTS:
            print("=====  %4d/%4d SKIPPED:  %s" % ( i, n, entry ))
            i += 1

    if len( FAILED_TESTS ) > 0:
        print("==================================================")
        print("==================== FAILED ======================")
        print("==================================================")
        n = len( FAILED_TESTS )
        i = 1
        for entry in FAILED_TESTS:
            print("=====  %4d/%4d FAILED:  %s" % ( i, n, entry ))
            i += 1

    print()

    if len( FAILED_TESTS ) == 0:
        print("==================================================")
        print("===================== PASS =======================")
        print("==================================================")
    else:
        print("==================================================")
        print("=================== FAILED! ======================")
        print("==================================================")

    print("=====  MODULES TESTED  : %5d     ===============" % total_modules)
    print("=====  TOTAL TESTS     : %5d     ===============" % total_tests)
    print("=====  RUN TESTS       : %5d     ===============" % run_tests)
    print("=====  SKIPPED TESTS   : %5d     ===============" % skipped_tests)
    print("=====  FAILED TESTS    : %5d     ===============" % failed_tests)
    print("=====  TOTAL TIME      : %7.1f s ===============" % total_duration)
    print("==================================================")






def ListTests( module_name ):
    module_tests = get_all_tests( module_name )
    for __line, name, func in module_tests:
        if run_only_testname is not None and run_only_testname != name:
            continue
        if is_test_excluded( module_name, name, func, quiet=True ):
            continue
        print( "{}.{}".format( module_name, name ) )




def ListModules( module_list ):
    for mod in module_list:
        if run_only_module is None or mod.__name__ == run_only_module:
            mod.List()



def RunModules( module_list, *args ):
    for mod in module_list:
        if run_only_module is None or mod.__name__ == run_only_module:
            mod.Run( *args )
            gc.collect()



def ListTestSets( module_list ):
    for testset in module_list:
        if run_only_testset is None or testset.__name__ == run_only_testset:
            ListTests( testset.__name__ )



def RunTestSets( module_list, *args ):
    for testset in module_list:
        if run_only_testset is None or testset.__name__ == run_only_testset:
            testset.Run( *args )
            gc.collect()
    if callable( CLEANUP_FUNC ):
        try:
            CLEANUP_FUNC()
        except:
            pass



def DryrunOnly():
    global dryrun
    dryrun = True


def RequireSuccess():
    global strict
    strict = True


def SetMaxTNominal( t ):
    global run_only_tnominal_less_than
    run_only_tnominal_less_than = t


def SetMinTestLevel( n ):
    global skip_test_levels_below
    skip_test_levels_below = n


def SetMaxTestLevel( n ):
    global skip_test_levels_above
    skip_test_levels_above = n


def SetInteractOnFailed( flag ):
    global interact_on_failed_test
    interact_on_failed_test = flag


def SetPostFunc( func ):
    global EACH_POST_FUNC
    EACH_POST_FUNC = func


def RunOnly( modulename, testset, testname ):
    global run_only_module
    global run_only_testset
    global run_only_testname
    run_only_module = modulename
    run_only_testset = testset
    run_only_testname = testname


def SetOutput( path ):
    abspath = os.path.abspath( path )
    dirname = os.path.dirname( abspath )
    if not os.path.exists( dirname ):
        raise FileNotFoundError( "Output directory '{}' does not exist".format( dirname ) )
    if os.path.exists( abspath ):
        if abspath.endswith( ".json" ):
            raise Exception( "File exists {}, remove file before running".format( abspath ) )
        else:
            print( "File exists {}, will append".format( abspath ) )
    OpenOutput( abspath )


def SetCleanup( func ):
    global CLEANUP_FUNC
    CLEANUP_FUNC = func


def PerformCleanup():
    if callable( CLEANUP_FUNC ):
        try:
            CLEANUP_FUNC()
        except:
            pass



def run():
    input( "Any key to start tests..." )
    tests = get_all_tests()
    for line, name, value in tests:
        print("TEST: name=%s  line=%d" % ( name, line ))




if __name__ == "__main__":
    run()
