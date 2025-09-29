###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Geo.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from ..Query._query_test_support import *
from pyvgx import *
import pyvgx
from math import *
import random
from pyvgxtest.threads import Worker
import threading

graph = None




###############################################################################
# process_city
#
###############################################################################
def process_city( g, country, city, MIN, MAX ):
    """
    """
    ret = 0
    tid = threadid()
    try:
        city_name = city['id']
        lat = city['properties']['lat']
        lon = city['properties']['lon']

        g.Define( "vicinity_%d := geodist( next['lat'], next['lon'], %f, %f )" % (tid, lat, lon) )
        g.Define( "within200km_%d := vertex.id != '%s' && geodist( vertex['lat'], vertex['lon'], %f, %f ) < 200000" % (tid, city_name, lat, lon) )

        # Get city's neighbors within 200km radius
        neighbors = g.Neighborhood(
            country,
            arc         = ( "city", D_OUT ),
            hits        = MAX,
            neighbor    = {
                'filter' : "within200km_%d" % tid
            },
            sortby      = S_RANK | S_ASC,
            rank        = "vicinity_%d" % tid,
            fields      = F_ID | F_RANK,
            result      = R_LIST,
            timeout     = 10000
        )

        # Too few cities, get the minimum regardless of distance
        if len( neighbors ) < MIN:
            neighbors = g.Neighborhood(
                country,
                hits        = MIN,
                neighbor    = {
                    'type'   : 'city',
                    'filter' : "vertex.id != '%s'" % city_name
                },
                sortby      = S_RANK | S_ASC,
                rank        = "vicinity_%d" % tid,
                fields      = F_ID | F_RANK,
                result      = R_LIST,
                timeout     = 10000
            )

        # Assign a vector to city where the vector is its closest neighbors
        vector = [ ( neighbor, 4000.0/distance ) for neighbor, distance in neighbors[:15] ]
        try:
            C = g.OpenVertex( city_name, "w", timeout=10000 )
            C.SetVector( vector )
            g.CloseVertex( C )
        except:
            LogWarning( "Failed to set vector for '%s'" % city_name )

        # Connect city to all neighbors
        remain = neighbors
        while remain:
            failed = []
            for item in remain:
                V = None
                try:
                    neighbor, distance = item
                    V = g.OpenVertices( [city_name, neighbor], timeout=5 )
                    C,N = V
                    g.Connect( C, ("near",M_DIST,distance), N )
                except pyvgx.AccessError:
                    failed.append( item )
                finally:
                    if V is not None:
                        g.CloseVertices( V )
            remain = failed
        ret = 1
    except Exception as ex:
        LogError( ex )
    return ret



LOCK = threading.Lock()


###############################################################################
# travel
#
###############################################################################
def travel( g, START, DESTINATION, outliers ):
    """
    """
    tid = threadid()
    D = None
    try:
        D = g.OpenVertex( DESTINATION, "r", timeout=1000 )
        d_lat = D['lat']
        d_lon = D['lon']
        g.CloseVertex( D ) 
        D = None
        g.Define( "progress_%d := geodist( %f, %f, next['lat'], next['lon'] ) < geodist( %f, %f, vertex['lat'], vertex['lon'] )" % (tid, d_lat, d_lon, d_lat, d_lon) )
        distance_traveled = 0.0
        direct_distance = g.Evaluate( "distance", START, None, DESTINATION )
        current_distance = direct_distance
        current_city = START
        n = 0
        while current_city != DESTINATION:
            n += 1
            previous_distance = current_distance
            try:
                next = g.Neighborhood(
                    current_city,
                    hits    = 1,
                    arc     = ( "near", D_OUT, M_DIST ),
                    filter  = "progress_%d" % tid,
                    sortby  = S_VAL | S_ASC,
                    fields  = F_ID | F_VAL,
                    result  = R_LIST,
                    timeout = 5000
                )
            except pyvgx.AccessError:
                break
            if not next:
                LOCK.acquire()
                try:
                    if current_city not in outliers:
                        outliers[ current_city ] = 0
                    outliers[ current_city ] += 1
                finally:
                    LOCK.release()
                break
            next_distance = next[0][0]
            next_city = next[0][1]
            distance_traveled += next_distance
            current_city = next_city
            current_distance = g.Evaluate( "distance", current_city, None, DESTINATION )
            Expect( current_distance < previous_distance,       "Progress should be made" )

            #print "%s (%.1fkm from %s, %.1fkm traveled)" % (current_city, current_distance/1000.0, DESTINATION, distance_traveled/1000.0)
        traversed = 0
        if current_city == DESTINATION:
            traversed = 1
            if n > 1:
                Expect( distance_traveled >= direct_distance,       "Sum of legs not less than total" )
            #overhead = distance_traveled / direct_distance
            #print "(%s) -[%.1fkm]-> (%s) (%d stops, %.1fkm traveled, overhead=%.3f)" % (START, direct_distance/1000.0, DESTINATION, n, distance_traveled/1000.0, overhead)
    finally:
        if D is not None:
            g.CloseVertex( D )
    return traversed




###############################################################################
# TEST_geo
#
###############################################################################
def TEST_geo():
    """
    Geo filter expressions for pyvgx.Graph.Neighborhood()
    t_nominal=250
    test_level=3702
    """
    g = NewGeoGraph( "geo" )
    print(g)

    #HITS = 500

    g.Define( "simnotself := sim( vector, vertex.vector ) in range( 0.001, 0.9999 )" )
    # Connect all cities to their neighbors within a 50km radius (no more than 500)
    MAX = 500
    MIN = 50
    cities = g.Vertices(
        condition   = {
            'type' : 'city'
        },
        fields      = F_ID,
        select      = "state; lat; lon",
        result      = R_DICT
        #hits        = HITS
    )

    NW = 8

    # Make workers
    WORKERS = []
    for w in range( NW ):
        WORKERS.append( Worker( "geo_worker_%d" % w ) )

    NW_fill = 2

    # Distribute work among workers
    LogInfo( "Distributing city processing work among %d workers" % NW_fill )
    for city in cities:
        wx = strhash64( city['properties']['state'] ) % NW_fill
        worker = WORKERS[ wx ]
        worker.perform( process_city, g, "USA", city, MIN, MAX )


    # Wait for all workers to finish
    time.sleep( 5 )

    remain = sum( [w.size_backlog() for w in WORKERS] )
    while remain > 0:
        LogInfo( "Waiting for workers to complete city processing: %d remaining" % remain )
        time.sleep( 5 )
        remain = sum( [w.size_backlog() for w in WORKERS] )

    total = 0
    for w in WORKERS:
        i = 0
        try:
            while True:
                i += w.collect()
        except:
            LogInfo( "%s processed %d cities" % (w, i) )
            total += 1
    LogInfo( "City processing complete: %d cities" % total )

    g.Save( 60000 )

    LogInfo( "Start traveling" )

    # Traverse from start city to destination city via other cities, such that the next stop on the way
    # is as close as possible to the previous stop while still making progress towards the destination.
    # If at any point during traversal we visit a city where progress cannot be made without moving 
    # farther from destination we terminate the trip and add the city to the outliers.
    g.SetGraphReadonly( 60000 )
    cities = g.Vertices(
        condition = {
            'type' : 'city'
        },
        sortby    = S_RANDOM
        #hits      = HITS
    )
    outliers = {}
    g.Define( "distance := geodist( vertex['lat'], vertex['lon'], next['lat'], next['lon'] )" )
    t0 = time.time()
    ts = t0
    OUTLIERS_TARGET = 23
    while len( outliers ) < OUTLIERS_TARGET:
        # Distribute one new travel task to each worker
        for worker in WORKERS:
            START, DESTINATION = random.sample( cities, 2 )
            worker.perform( travel, g, START, DESTINATION, outliers )
        # Wait here until at least one worker is idle
        while sum([ 1 for w in WORKERS if w.is_idle() ]) == 0:
            time.sleep( 0.01 )
        # Current pending
        t = time.time()
        if t > ts:
            n_pending = sum( [w.size_backlog() for w in WORKERS] )
            LogInfo( "%d sec: %d/%d %d" % ( t - t0, len(outliers), OUTLIERS_TARGET, n_pending) )
            ts += 10




    # Wait for all workers to finish
    LogInfo( "Waiting for all travelers to complete" )
    time.sleep( 5 )
    while sum([ 1 for w in WORKERS if w.is_idle() ]) < NW:
        time.sleep( 0.1 )
    traversed = 0
    for w in WORKERS:
        i = 0
        try:
            while True:
                i += w.collect()
        except:
            LogInfo( "%s performed %d journeys" % (w, i) )
            traversed += i

    LogInfo( "Traveling complete" )
    g.ClearGraphReadonly()

    # Terminate all workers
    for w in WORKERS:
        w.terminate()
    while sum([ 1 for w in WORKERS if w.is_dead() ]) < NW:
        time.sleep( 0.1 )

    # Check results
    Expect( traversed > 0,          "More than zero successful traversals" )
    print("Outliers found:")
    for city, freq in sorted( list(outliers.items()), key=lambda k:k[1], reverse=True ):
        print("%s (%d)" % (city, freq))

    # Look up neighbor cities to a given city using similarity vectors
    #
    #
    #
    nearby = 0
    g.Define( "simnotself := sim( vector, vertex.vector ) in range( 0.001, 0.9999 )" )
    for city in random.sample( cities, 500 ):
        C = g.OpenVertex( city, "r" )
        neighbors = g.Vertices( 
            sortby    = S_SIM,
            fields    = F_RANK|F_ID,
            hits      = MAX,
            condition = {
                'type'       :'city',
                'similarity' : {
                    'vector'    : C.vector
                },
                'filter'     : "simnotself"
            }
        )
        g.CloseVertex( C )
        if len(neighbors) < 3:
            print("Remote: %s" % city)
        else:
            nearby += 1
    Expect( nearby > 0,             "More than zero nearby cities" )


    g.Erase()





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Truncate()
    graph.Close()
    del graph
