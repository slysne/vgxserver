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
    if sortdir == "desc":
        d = S_DESC
    else:
        d = S_ASC

    if sortby == "val":
        pr = PluginResponse( maxhits=hits, sortby=S_VAL|d )
        sf = F_VAL
        fn = "arc"
        fn2 = "value"
    elif sortby == "id":
        pr = PluginResponse( maxhits=hits, sortby=S_ID|d )
        sf = F_ID
        fn = "id"
    elif sortby == "deg":
        pr = PluginResponse( maxhits=hits, sortby=S_DEG|d )
        sf = F_DEG
        fn = "degree"
    else:
        pr = PluginResponse( maxhits=hits, sortby=S_NONE )
        sf = 0
        fn = None

    try:
        result = graph.Neighborhood( name, arc=D_ANY, hits=pr.maxhits, sortby=pr.sortby, fields=fields|sf, result=R_DICT, timeout=1000 )
        pre_message = request.get('pre-message')
        for r in result:
            rv = r.get(fn)
            if type(rv) is dict:
                rv = rv.get(fn2)
            r['pre'] = pre_message
            r['this'] = root
            pr.Append( rv, r )
    except KeyError:
        return pr
    except Exception as err:
        pr.message = repr(err)
        print(err)
    return pr




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
    if count >= 0:
        for i in range(count):
            a = random.randint(1, N)
            while True:
                b = random.randint(1, N)
                if a != b:
                    break
            r = random.random()
            A = None
            B = None
            try:
                graph.CreateVertex( str(a) )
                graph.CreateVertex( str(b) )
                A,B = graph.OpenVertices([str(a), str(b)], timeout=100)
                graph.Connect( A, ("to", M_FLT, r), B )
            finally:
                if A is not None:
                    A.Close()
                if B is not None:
                    B.Close()
            if sleep > 0:
                time.sleep( sleep/1000 )
        response.Append( "Added {}".format(count) )
    else:
        count = -count
        c0 = graph.Order()
        for i in range(count):
            if graph.Order() == 0:
                break
            x = graph.GetVertexID()
            graph.Disconnect( x, timeout=2000 )
            graph.DeleteVertex( x, timeout=2000 )
            if sleep > 0:
                time.sleep( sleep/1000 )
        c1 = graph.Order()
        response.Append( "Deleted {}".format( c0-c1 ) )
    return response
