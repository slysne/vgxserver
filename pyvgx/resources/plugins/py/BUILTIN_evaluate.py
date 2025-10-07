###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_evaluate.py
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

import pyvgx


###############################################################################
# sysplugin__evaluate
#
###############################################################################
def sysplugin__evaluate( request:pyvgx.PluginRequest, content:str, graph:str, expression:str="", tail:str="", arc:str="", head:str="", memory:int=128, memresult:int=0, sharedmem:int=0 ):
    """
    Expression evaluator
    """
    g, close = sysplugin__GetGraphObject( graph )
    try:
        M = sysplugin__GetGraphMemory( g, memory, sharedmem )
        params = { 'memory': M }

        if content:
            params['expression'] = content
        elif expression:
            params['expression'] = expression

        if tail:
            params['tail'] = tail
        if arc:
            params['arc'] = sysplugin__VALIDATOR.SafeEval( arc )
        if head:
            params['head'] = head

        if 'tail' in params:
            params['tail'] = g.OpenVertex( params['tail'], mode='r', timeout=5000 )
        if 'head' in params:
            params['head'] = g.OpenVertex( params['head'], mode='r', timeout=5000 )

        R = g.Evaluate( **params )
        if memresult:
            L = M.AsList()
        else:
            L = []
        response = { 'result': repr(R), 'memory': L }
        return response
    finally:
        try:
            if 'tail' in params:
                g.CloseVertex( params['tail'] )
            if 'head' in params:
                g.CloseVertex( params['head'] )
        finally:
            if close:
                g.Close()


pyvgx.system.AddPlugin( plugin=sysplugin__evaluate )
