###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    vgxdemoplugin.py
# Author:  Stian Lysne slysne.dev@gmail.com
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

from pyvgx import *
import random
import time
    

def Search( request:PluginRequest, graph:Graph, name:str, hits:int=10, sortby:str="val", sortdir:str="desc", fields:int=F_AARC|F_RANK ) -> PluginResponse:
    """
    Engine plugin example: Search
    """
    root = system.Root()
    # Sort descending (default)
    if sortdir == "desc":
        d = S_DESC
    # Sort ascending
    else:
        d = S_ASC

    # Sort by arc value (default)
    if sortby == "val":
        sortspec = S_VAL | d
        sf = F_VAL
        fn = "arc"
        fn2 = "value"
    # Sort by terminal's ID
    elif sortby == "id":
        sortspec = S_ID | d
        sf = F_ID
        fn = "id"
    # Sort by terminal's degree
    elif sortby == "deg":
        sortspec = S_DEG | d
        sf = F_DEG
        fn = "degree"
    # No sorting
    else:
        sortspec = S_NONE
        sf = 0
        fn = None

    # Default arc
    arc = ('to', D_OUT)
    rankspec = None
    probevector = None

    # Special case: index
    if name == "index":
        # Select a random target
        target = graph.Neighborhood( name, hits=1, sortby=S_RANDOM )[0]
        # Create a noisy probe vector from target's vector
        original = graph.OpenVertex(target, timeout=1000).GetVector().external
        probevector = graph.sim.NewVector( [x + 0.5*(random.random() - 0.5) for x in original] )
        # Index arc
        arc = ('lsh', D_OUT)
        sortspec = S_RANK | d
        rankspec = "0.5 * cosine(vector, next.vector) + 0.5"
        sf = F_RANK
        fn = "rankscore"

    # Response
    response = PluginResponse( maxhits=hits, sortby=sortspec )
    try:
        # Execute query
        result = graph.Neighborhood(
            id      = name, 
            arc     = arc,
            rank    = rankspec,
            vector  = probevector,
            hits    = response.maxhits,
            sortby  = response.sortby,
            fields  = fields|sf,
            result  = R_DICT,
            timeout = 1000
        )
        # Build response from query result
        for r in result:
            # Sort value from field name
            rv = r.get(fn)
            if type(rv) is dict:
                rv = rv.get(fn2)
            # Identify the instance
            r['instance'] = root
            # Add to response
            response.Append( rv, r )
    except KeyError:
        return response
    except Exception as err:
        response.message = repr(err)
        print(err)
    return response



def PreAdd( request:PluginRequest, graph ) -> PluginRequest:
    """
    Pre-processor plugin example: PreAdd
    """
    request.primary = 1
    return request



def Add( request:PluginRequest, graph, N:int=10000, count:int=500, sleep:int=0 ) -> PluginResponse:
    """
    Engine plugin example: Add
    """
    response = PluginResponse()
    # Make sure index node exists
    graph.CreateVertex( 'index', type='index' )
    # Add data
    if count >= 0:
        for i in range(count):
            # Two random vertex names
            a = str(random.randint(1, N))
            while True:
                b = str(random.randint(1, N))
                if a != b:
                    break
            # Random arc value
            r = random.random()
            A = None
            B = None
            X = None
            try:
                # Make both vertices and connect them
                graph.CreateVertex( a, type='node' )
                graph.CreateVertex( b, type='node' )
                A,B,X = graph.OpenVertices([a, b, 'index'], timeout=100)
                graph.Connect( A, ('to', M_FLT, r), B )
                # Add some properties
                A['init'] = random.random()
                B['term'] = random.random()
                # Add random vectors to each vertex and connect from index
                Va = graph.sim.rvec(768)
                Vb = graph.sim.rvec(768)
                A.SetVector( Va )
                B.SetVector( Vb )
                fpA = Va.Fingerprint()
                fpB = Vb.Fingerprint()
                graph.Connect( X, ('lsh', M_LSH, A.ArcLSH(fpA) ), A )
                graph.Connect( X, ('lsh', M_LSH, B.ArcLSH(fpB) ), B )
            finally:
                graph.CloseVertices( [A,B,X] )
            # Optional sleep
            if sleep > 0:
                time.sleep( sleep/1000 )
        # Response
        response.Append( "Added {}".format(count) )
    # Remove data
    else:
        count = -count
        c0 = graph.Order()
        for i in range(count):
            if graph.Order() == 0:
                break
            # Get a random vertex in the graph
            x = graph.GetVertexID()
            # Remove all arcs from vertex, then delete it
            graph.Disconnect( x, timeout=2000 )
            graph.DeleteVertex( x, timeout=2000 )
            # Remove from vector index
            graph.Disconnect( 'index', ('lsh',D_OUT), x )
            # Optional sleep
            if sleep > 0:
                time.sleep( sleep/1000 )
        # Response
        c1 = graph.Order()
        response.Append( "Deleted {}".format( c0-c1 ) )
    return response
