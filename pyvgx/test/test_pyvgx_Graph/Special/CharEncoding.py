from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import time
import itertools
import random
import json

graph = None


UNICODE = list(itertools.chain(range(0x20,0xd800),range(0xe000,0x110000)))


# Some valid and invalid UTF-8 sequences
SEQUENCES = [
    (True,  bytes( [0x7f] )),
    (False, bytes( [0x80] )),
    (False, bytes( [0x81] )),
    (False, bytes( [0xfe] )),
    (False, bytes( [0xff] )),
    (False, bytes( [0xc1, 0x80] )),
    (False, bytes( [0xc1, 0xff] )),
    (False, bytes( [0xc2, 0x7f] )),
    (False, bytes( [0xc2] )),
    (True,  bytes( [0xc2, 0x80] )),
    (True,  bytes( [0xc2, 0xbf] )),
    (False, bytes( [0xc2, 0xc0] )),
    (True,  bytes( [0xc3, 0x80] )),
    (True,  bytes( [0xdf, 0x80] )),
    (True,  bytes( [0xdf, 0xbf] )),
    (False, bytes( [0xdf, 0xc0] )),
    (False, bytes( [0xe0, 0x80] )),
    (False, bytes( [0xe0, 0x80, 0x80] )),
    (False, bytes( [0xe0, 0x9f, 0x80] )),
    (False, bytes( [0xe0, 0xa0] )),
    (True,  bytes( [0xe0, 0xa0, 0x80] )),
    (False, bytes( [0xe0, 0xa0, 0xc0] )),
    (True,  bytes( [0xe1, 0x80, 0x80] )),
    (True,  bytes( [0xe1, 0xbf, 0xbf] )),
    (True,  bytes( [0xe2, 0x80, 0x80] )),
    (True,  bytes( [0xe2, 0xbf, 0xbf] )),
    (True,  bytes( [0xed, 0x9f, 0xbf] )),
    (False, bytes( [0xed, 0xa0, 0x80] )),
    (False, bytes( [0xed, 0xbf, 0xbf] )),
    (True,  bytes( [0xee, 0x80, 0x80] )),
    (True,  bytes( [0xef, 0xbf, 0xbf] )),
    (False, bytes( [0xf0, 0x80, 0x80, 0x80] )),
    (False, bytes( [0xf0, 0x80, 0xbf, 0xbf] )),
    (False, bytes( [0xf0, 0x8f, 0xbf, 0xbf] )),
    (True,  bytes( [0xf0, 0x90, 0x80, 0x80] )),
    (True,  bytes( [0xf0, 0x90, 0x80, 0xbf] )),
    (True,  bytes( [0xf0, 0x90, 0xbf, 0xbf] )),
    (True,  bytes( [0xf0, 0xbf, 0xbf, 0xbf] )),
    (True,  bytes( [0xf1, 0x80, 0x80, 0x80] )),
    (True,  bytes( [0xf4, 0x8f, 0xbf, 0xbf] )),
    (False, bytes( [0xf4, 0x90, 0x80, 0x80] ))
]



def get_random_unicode_string( sz ):
    """
    return a unicode string
    """
    return "".join( map( chr, random.sample( UNICODE, sz ) ) )



def expect_KeyError( method, name ):
    """
    method should produce KeyError
    """
    try:
        method( name )
        Expect( False,     "%s(%s) -> raise KeyError" % (method, name) )
    except KeyError:
        pass



def expect_UnicodeError( method, name ):
    """
    method should produce UnicodeError
    """
    try:
        method( name )
        Expect( False,     "%s(%s) -> raise UnicodeError" % (method, name) )
    except UnicodeError:
        pass



def get_vertex_key_methods( graph ):
    """
    return vertex lookup methods that take vertex id as first argument
    """
    return [
        graph.GetVertex,
        graph.OpenVertex,
        graph.Neighborhood,
        graph.Initials,
        graph.Terminals,
        graph.Inarcs,
        graph.Outarcs,
        graph.Adjacent,
        graph.Aggregate,
        graph.Degree,
        graph.Disconnect,
        graph.ShowVertex,
        graph.VertexDescriptor
    ]



def get_vertex_create_methods( graph ):
    """
    return vertex creation methods that take vertex id as first argument
    """
    return [
        graph.CreateVertex,
        graph.NewVertex
    ]



def TEST_VertexName_valid_utf8():
    """
    Test API involving vertex names with valid utf-8 sequences
    test_level=3101
    """

    g = graph
    g.Truncate()

    # Ensure repeatable test
    random.seed( 1234 )

    # Test a sample of valid code points
    long_pad = "_%s" % ("x"*50)
    for cp in random.sample( UNICODE, 5000 ):
        name_short = "%s" % cp
        name_long = "%s%s" % (cp, long_pad)
    
        # Lookup should fail when vertex does not exist
        for name in [name_short, name_short.encode(), name_long, name_long.encode()]:
            Expect( name not in g,                  "not in g" )
            Expect( not g.HasVertex( name ),        "g.HasVertex() False" )
            # Methods that take vertex ID argument
            for meth in get_vertex_key_methods( g ):
                expect_KeyError( meth, name )
            # OpenVertices
            try:
                g.OpenVertices( [name] )
                Expect( False,                      "g.OpenVertices() KeyError" )
            except KeyError:
                pass

        # Create
        for name in [name_short, name_long]:
            for variant1, variant2 in [(name, name.encode()),(name.encode(), name)]:
                Expect( variant1 not in g,                      "variant1 not in g" )
                Expect( variant2 not in g,                      "variant2 not in g" )
                Expect( g.CreateVertex( variant1 ) == 1,        "create variant1" )
                Expect( variant1 in g,                          "variant1 in g" )
                Expect( variant2 in g,                          "variant2 in g" )
                Expect( g.CreateVertex( variant2 ) == 0,        "already created" )
                Expect( g.DeleteVertex( variant1 ) == 1,        "delete" )

        # New
        for name in [name_short, name_long]:
            for variant1, variant2 in [(name, name.encode()),(name.encode(), name)]:
                # NewVertex()
                V = g.NewVertex( variant1 )
                Expect( type( V.id ) is str,                    "id should always be str" )
                if type( variant1 ) is bytes:
                    Expect( V.id == variant1.decode(),          "id should match decoded variant" )
                else:
                    Expect( V.id == variant1,                   "id should match variant" )
                g.CloseVertex( V )
                Expect( variant1 in g,                          "variant1 in g" )
                Expect( variant2 in g,                          "variant2 in g" )
                Expect( g.CreateVertex( variant2 ) == 0,        "already created" )
                Expect( g.DeleteVertex( variant1 ) == 1,        "delete" )

        # Open in 'w' mode
        for name in [name_short, name_long]:
            for variant1, variant2 in [(name, name.encode()),(name.encode(), name)]:
                # OpenVertex()
                V = g.OpenVertex( variant1, mode="w" )
                Expect( type( V.id ) is str,                    "id should always be str" )
                if type( variant1 ) is bytes:
                    Expect( V.id == variant1.decode(),          "id should match decoded variant" )
                else:
                    Expect( V.id == variant1,                   "id should match variant" )
                g.CloseVertex( V )
                Expect( variant1 in g,                          "variant1 in g" )
                Expect( variant2 in g,                          "variant2 in g" )
                Expect( g.CreateVertex( variant2 ) == 0,        "already created" )
                Expect( g.DeleteVertex( variant1 ) == 1,        "delete" )

        # Connect
        for init, term in [(name_short, name_long), (name_long, name_short), (name_short,name_short), (name_long,name_long)]:
            for init_var1, init_var2 in [(init, init.encode()),(init.encode(), init)]:
                for term_var1, term_var2 in [(term, term.encode()),(term.encode(), term)]:
                    Expect( g.Connect( init_var1, ("to",M_INT,123), term_var1 ) == 1,       "connect" )
                    Expect( init in g and term in g,                                        "init and term created" )
                    Expect( g.Connect( init_var1, ("to",M_INT,123), term_var2 ) == 0,       "already connected" )
                    Expect( g.Connect( init_var2, ("to",M_INT,123), term_var1 ) == 0,       "already connected" )
                    Expect( g.Connect( init_var2, ("to",M_INT,123), term_var2 ) == 0,       "already connected" )
                    Expect( g.ArcValue( init_var1, "to", term_var1 ) == 123,                "123" )
                    Expect( g.ArcValue( init_var1, "to", term_var2 ) == 123,                "123" )
                    Expect( g.ArcValue( init_var2, "to", term_var1 ) == 123,                "123" )
                    Expect( g.ArcValue( init_var2, "to", term_var2 ) == 123,                "123" )
                    g.DeleteVertex( init )
                    g.DeleteVertex( term )
                    Expect( init not in g and term not in g,                    "init and term deleted" )

        # Count
        for init, term in [(name_short, name_long), (name_long, name_short), (name_short,name_short), (name_long,name_long)]:
            for init_var1, init_var2 in [(init, init.encode()),(init.encode(), init)]:
                for term_var1, term_var2 in [(term, term.encode()),(term.encode(), term)]:
                    Expect( g.Count( init_var1, "to", term_var1 ) == 1,         "Count -> 1" )
                    Expect( init in g and term in g,                            "init and term created" )
                    Expect( g.Count( init_var1, "to", term_var2 ) == 2,         "Count -> 2" )
                    Expect( g.Count( init_var2, "to", term_var1 ) == 3,         "Count -> 3" )
                    Expect( g.Count( init_var2, "to", term_var2 ) == 4,         "Count -> 4" )
                    Expect( g.ArcValue( init_var1, "to", term_var1 ) == 4,      "4" )
                    Expect( g.ArcValue( init_var1, "to", term_var2 ) == 4,      "4" )
                    Expect( g.ArcValue( init_var2, "to", term_var1 ) == 4,      "4" )
                    Expect( g.ArcValue( init_var2, "to", term_var2 ) == 4,      "4" )
                    g.DeleteVertex( init )
                    g.DeleteVertex( term )
                    Expect( init not in g and term not in g,                    "init and term deleted" )

        # Accumulate
        for init, term in [(name_short, name_long), (name_long, name_short), (name_short,name_short), (name_long,name_long)]:
            for init_var1, init_var2 in [(init, init.encode()),(init.encode(), init)]:
                for term_var1, term_var2 in [(term, term.encode()),(term.encode(), term)]:
                    Expect( g.Accumulate( init_var1, "to", term_var1 ) == 1.0,          "Accumulate -> 1.0" )
                    Expect( init in g and term in g,                                    "init and term created" )
                    Expect( g.Accumulate( init_var1, "to", term_var2 ) == 2.0,          "Accumulate -> 2.0" )
                    Expect( g.Accumulate( init_var2, "to", term_var1 ) == 3.0,          "Accumulate -> 3.0" )
                    Expect( g.Accumulate( init_var2, "to", term_var2 ) == 4.0,          "Accumulate -> 4.0" )
                    Expect( g.ArcValue( init_var1, "to", term_var1 ) == 4.0,            "4.0" )
                    Expect( g.ArcValue( init_var1, "to", term_var2 ) == 4.0,            "4.0" )
                    Expect( g.ArcValue( init_var2, "to", term_var1 ) == 4.0,            "4.0" )
                    Expect( g.ArcValue( init_var2, "to", term_var2 ) == 4.0,            "4.0" )
                    g.DeleteVertex( init )
                    g.DeleteVertex( term )
                    Expect( init not in g and term not in g,                    "init and term deleted" )



def TEST_VertexName_invalid_utf8():
    """
    Test API involving vertex names with invalid utf-8 sequences
    test_level=3101
    """

    g = graph
    g.Truncate()


    INVALID_SEQ = [seq for valid,seq in SEQUENCES if not valid]

    # Test a sample of valid code points
    long_pad = b"_%s" % (b"x"*50)
    for seq in INVALID_SEQ:
        name_short = b"%s" % seq
        name_long = b"%s%s" % (seq, long_pad)
    
        # sanity check
        for name in [name_short, name_long]:
            try:
                name.decode()
                Expect( False )
            except UnicodeError:
                pass

        Expect( g.order == 0 )
        # Lookup should fail with UnicodeError
        for name in [name_short, name_long]:
            # in g
            try:
                name in g
                Expect( False,  "UnicodeError" )
            except UnicodeError:
                pass
            # HasVertex()
            try:
                g.HasVertex( name )
                Expect( False,  "UnicodeError" )
            except UnicodeError:
                pass
            # Methods that take vertex ID argument
            for meth in get_vertex_key_methods( g ):
                expect_UnicodeError( meth, name )
            # OpenVertices()
            try:
                g.OpenVertices( [name] )
                Expect( False,                      "g.OpenVertices() UnicodeError" )
            except UnicodeError:
                pass

        # Create
        for name in [name_short, name_long]:
            for meth in get_vertex_create_methods( g ):
                expect_UnicodeError( meth, name )
            try:
                g.OpenVertex( name, "w" )
                Expect( False,                      "g.OpenVertex( mode='w' ) UnicodeError" )
            except UnicodeError:
                Expect( g.order == 0 )


        # Connect, Count, Accumulate
        for init, term in [(name_short, name_long), (name_long, name_short), (name_short,name_short), (name_long,name_long)]:
            for meth in [g.Connect, g.Count, g.Accumulate]:
                try:
                    meth( init, "to", term )
                    Expect( False,                      "%s UnicodeError" % (meth) )
                except UnicodeError:
                    Expect( g.order == 0 )

        # Disconnect
        for init, term in [(name_short, name_long), (name_long, name_short), (name_short,name_short), (name_long,name_long)]:
            # Disconnect( x )
            try:
                g.Disconnect( init )
                Expect( False,                      "Disconnect() UnicodeError" )
            except UnicodeError:
                pass
            # Disconnect( x, arc )
            try:
                g.Disconnect( init, "to" )
                Expect( False,                      "Disconnect() UnicodeError" )
            except UnicodeError:
                pass
            # Disconnect( x, arc, y )
            try:
                g.Disconnect( init, "to", term )
                Expect( False,                      "Disconnect() UnicodeError" )
            except UnicodeError:
                pass

            Expect( g.CreateVertex( "A" ) == 1 )
            Expect( g.order == 1 )
            Expect( "A" in g )
            # Disconnect( x, arc, "A" )
            try:
                g.Disconnect( init, "to", "A" )
                Expect( False,                      "Disconnect() UnicodeError" )
            except UnicodeError:
                Expect( g.order == 1 )
                Expect( "A" in g )
            # Disconnect( "A", arc, y )
            try:
                g.Disconnect( "A", "to", term )
                Expect( False,                      "Disconnect() UnicodeError" )
            except UnicodeError:
                Expect( g.order == 1 )
                Expect( "A" in g )
            # Delete A
            Expect( g.DeleteVertex( "A" ) == 1 )
            Expect( g.order == 0 ) 



def TEST_VertexName_search_result_utf8():
    """
    Test vertex names returned in search results
    test_level=3101
    """

    g = graph
    g.Truncate()

    # Ensure repeatable test
    random.seed( 1234 )

    V_SET = set()

    # Test a sample of valid code points
    long_pad = "_%s" % ("x"*50)
    for cp in random.sample( UNICODE, 50000 ):
        name_short = "%s" % cp
        name_long = "%s%s" % (cp, long_pad)
    
        # Create
        for name in [name_short, name_long]:
            V_SET.add( name )
            Expect( g.CreateVertex( name.encode() ) == 1,       "create" )
            # Check name length in unicode codepoints
            ncp = len( name )
            eval_len = g.Evaluate( "len(next.id)", head=name )
            Expect( eval_len == ncp,                            "id length %d, got %d" % (ncp, eval_len) )



    # R_SIMPLE
    for F_x in [F_ID, F_ID|F_TYPE]:
        R = g.Vertices( result=R_SIMPLE, fields=F_x )
        S = set( R )
        Expect( V_SET == S )
    # R_STR
    R = g.Vertices( result=R_STR )
    S = set( R )
    Expect( V_SET == S )
    R = g.Vertices( result=R_STR, fields=F_ID|F_TYPE )
    S = set( [json.loads(x)['id'] for x in R] )
    Expect( V_SET == S )
    # R_LIST
    for F_x in [F_ID, F_ID|F_TYPE]:
        R = g.Vertices( result=R_LIST, fields=F_x )
        S = set( [x[0] for x in R] )
        Expect( V_SET == S )
    # R_DICT
    for F_x in [F_ID, F_ID|F_TYPE]:
        R = g.Vertices( result=R_DICT, fields=F_x )
        S = set( [x['id'] for x in R] )
        Expect( V_SET == S )

















def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph