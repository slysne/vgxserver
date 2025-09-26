###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    PYVGX_vgxinstance.py
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

import pyvgx
import sys
import os
import json
import importlib
import threading
import time

GRAPHS = []


class vgxinstance__VGXInstance( object ):

    INSTANCE = None
    PLUGIN_MODULES = {}
    RELOADABLE = {}


    @staticmethod
    def get_vgxroot( instance, basedir="." ):
        instance_dir = "{}_{}".format(instance.type, instance.id)
        return os.path.join( basedir, instance_dir )


    @staticmethod
    def LoadLocalTestPlugins( instance ):
        try:
            if instance.type in ["builder", "txproxy", "search"]:
                import test_plugin
            if instance.type == "builder":
                import test_feedfuncs
            if instance.type == "dispatch":
                import test_prepost
        except Exception as err:
            pass
            #print( "No local test plugins loaded: {}".format(err) )
        


    @staticmethod
    def InitAdmin( instance, root ):
        pyvgx.system.Initialize( root, euclidean=True, events=False )
        for gname in instance.graphs:
            graph = pyvgx.Graph( gname, local=instance.IsLocal() )
            graph.ClearGraphReadonly()
            graph.EventEnable()
            GRAPHS.append(graph)



    @staticmethod
    def InitGeneric( instance, root ):
        pyvgx.system.Initialize( root, euclidean=True )
        for gname in instance.graphs:
            graph = pyvgx.Graph( gname, local=instance.IsLocal() )
            graph.ClearGraphReadonly()
            graph.EventEnable()
            GRAPHS.append(graph)



    @staticmethod
    def InitBuilder( instance, root ):
        pyvgx.system.Initialize( root, euclidean=True, idle=True )
        for gname in instance.graphs:
            graph = pyvgx.Graph( gname, local=instance.IsLocal() )
            graph.ClearGraphReadonly()
            graph.EventEnable()
            GRAPHS.append(graph)



    @staticmethod
    def InitTXProxy( instance, root ):
        pyvgx.system.Initialize( root, euclidean=True, events=False )
        for gname in instance.graphs:
            graph = pyvgx.Graph( gname, local=instance.IsLocal() )
            graph.ClearGraphReadonly()
            GRAPHS.append(graph)
        pyvgx.op.Bind( port=instance.tport, durable=instance.durable, snapshot_threshold=1<<32 )



    @staticmethod
    def InitSearch( instance, root ):
        pyvgx.system.Initialize( root, euclidean=True, events=False )
        for gname in instance.graphs:
            graph = pyvgx.Graph( gname, local=instance.IsLocal() )
            graph.ClearGraphReadonly()
            GRAPHS.append(graph)
        pyvgx.op.Bind( port=instance.tport )



    @staticmethod
    def InitDispatcher( instance, root ):
        pyvgx.system.Initialize( root, euclidean=True )
        for gname in instance.graphs:
            graph = pyvgx.Graph( gname, local=instance.IsLocal() )
            graph.ClearGraphReadonly()
            GRAPHS.append(gname)



    @staticmethod
    def GetDescriptor( descriptor_file=None ):
        """
        Get system descriptor
        """
        if descriptor_file is None:
            descriptor_file = "vgx.cf"
        descriptor = pyvgx.Descriptor( descriptor=descriptor_file )
        return descriptor



    @staticmethod
    def LoadFuncFromModule( plugin_module, func_ref ):
        if type(func_ref) is not str:
            return func_ref
        obj = plugin_module
        for elem in func_ref.split("."):
            obj = getattr( obj, elem )
        return obj



    @staticmethod
    def GetGraph( name, local ):
        try:
            return pyvgx.Graph( name, local=local )
        except Exception as err:
            return pyvgx.system.GetGraph( name )



    @staticmethod
    def LoadPlugins( instance=None, plugins=None ):
        """
        Load or reload plugins
        """
        if instance is None:
            instance = vgxinstance__VGXInstance.INSTANCE
        else:
            vgxinstance__VGXInstance.INSTANCE = instance
        if plugins is None:
            plugins = vgxinstance__VGXInstance.RELOADABLE.values()
        # Find all unique modules
        MODULES = {}
        for plugin_def in plugins:
            plugin_package_name = plugin_def.get( "package", "" )
            plugin_module_name = plugin_def.get( "module" )
            if plugin_module_name is not None:
               key = "{}{}".format( plugin_package_name, plugin_module_name )
               if key not in MODULES:
                   MODULES[ key ] = ( plugin_package_name, plugin_module_name )
        # Import/reload all modules
        for key, value in MODULES.items():
            plugin_package_name, plugin_module_name = value
            plugin_module = vgxinstance__VGXInstance.PLUGIN_MODULES.get( key )
            # First time: import
            if plugin_module is None:
                plugin_module = importlib.import_module( plugin_module_name, package=plugin_package_name )
                pyvgx.LogInfo( "Imported plugin module: {}".format( plugin_module ) )
            # Seen before: reload
            else:
                plugin_module = importlib.reload( plugin_module )
                pyvgx.LogInfo( "Reloaded plugin module: {}".format( plugin_module ) )
            vgxinstance__VGXInstance.PLUGIN_MODULES[ key ] = plugin_module
        # Add all plugins
        ADDED = []
        for plugin_def in plugins:
            plugin_name = plugin_def.get( "name" )
            plugin_package_name = plugin_def.get( "package", "" )
            plugin_module_name = plugin_def.get( "module" )
            plugin_engine = plugin_def.get( "engine" )
            plugin_pre = plugin_def.get( "pre" )
            plugin_post = plugin_def.get( "post" )
            plugin_graph_name = plugin_def.get( "graph" )
            if plugin_module_name is not None:
                key = "{}{}".format( plugin_package_name, plugin_module_name )
                plugin_module = vgxinstance__VGXInstance.PLUGIN_MODULES[ key ]
                plugin_engine = vgxinstance__VGXInstance.LoadFuncFromModule( plugin_module, plugin_engine )
                plugin_pre = vgxinstance__VGXInstance.LoadFuncFromModule( plugin_module, plugin_pre )
                plugin_post = vgxinstance__VGXInstance.LoadFuncFromModule( plugin_module, plugin_post )
                if plugin_name not in vgxinstance__VGXInstance.RELOADABLE:
                    vgxinstance__VGXInstance.RELOADABLE[ plugin_name ] = plugin_def
            # name
            if type( plugin_name ) is not str:
                raise ValueError( "Plugin 'name' required, and must be string" )
            # graph
            if plugin_graph_name is None:
                graph = None
            elif plugin_graph_name == "*":
                if instance.graphs:
                    graph = vgxinstance__VGXInstance.GetGraph( name=instance.graphs[0], local=instance.IsLocal() )
                else:
                    graph = None
            elif plugin_graph_name in instance.graphs:
                graph = vgxinstance__VGXInstance.GetGraph( name=plugin_graph_name, local=instance.IsLocal() )
            else:
                raise ValueError( "Unknown graph name '{}' in definition of plugin '{}'".format( plugin_graph_name, plugin_name ) )
            # functions
            if instance.type == "dispatch":
                pre = plugin_pre
                post = plugin_post
                if plugin_pre or plugin_post:
                    if plugin_pre and not callable(plugin_pre):
                        raise ValueError( "'pre' must be callable for plugin '{}', got {}".format( plugin_name, type(plugin_pre) ) )
                    if plugin_post and not callable(plugin_post):
                        raise ValueError( "'post' must be callable for plugin '{}', got {}".format( plugin_name, type(plugin_post) ) )
                    infos = ["Dispatcher plugin"]
                    if plugin_pre is not None:
                        infos.append( "pre={}.{}".format( plugin_pre.__module__, plugin_pre.__name__ ) )
                    if plugin_post is not None:
                        infos.append( "post={}.{}".format( plugin_post.__module__, plugin_post.__name__ ) )
                    info = " ".join( infos )
                    ADDED.append( info )
                    pyvgx.LogInfo( info )
                    if graph is None:
                        pyvgx.system.AddPlugin( pre=plugin_pre, post=plugin_post, name=plugin_name )
                    else:
                        pyvgx.system.AddPlugin( pre=plugin_pre, post=plugin_post, name=plugin_name, graph=graph )
            else:
                if plugin_engine:
                    if not callable(plugin_engine):
                        raise ValueError( "'engine' must be callable for plugin '{}', got {}".format( plugin_name, type(plugin_engine) ) )
                    info = "Plugin engine={}.{}".format( plugin_engine.__module__, plugin_engine.__name__ )
                    ADDED.append( info )
                    pyvgx.LogInfo( info )
                    if graph is None:
                        pyvgx.system.AddPlugin( engine=plugin_engine, name=plugin_name )
                    else:
                        pyvgx.system.AddPlugin( engine=plugin_engine, name=plugin_name, graph=graph )
        return ADDED



    @staticmethod
    def StartInstance( id, descriptor, basedir=".", plugins=[] ):
        """
        Start VGX instance
        """
        if type( descriptor ) is pyvgx.Descriptor:
            pass
        elif type( descriptor ) is str or descriptor is None:
            descriptor = vgxinstance__VGXInstance.GetDescriptor( descriptor )
        elif type( descriptor ) is dict:
            descriptor = pyvgx.Descriptor( descriptor )
        else:
            raise TypeError( "descriptor must be pyvgx.Descriptor, str, or None" )

        instance = descriptor.Get(id)

        root = vgxinstance__VGXInstance.get_vgxroot( instance, basedir )
        if instance.type in ["admin", "generic"]:
            if instance.type == "admin":
                vgxinstance__VGXInstance.InitAdmin( instance, root )
            else:
                vgxinstance__VGXInstance.InitGeneric( instance, root )
        elif instance.type == "builder":
            vgxinstance__VGXInstance.InitBuilder( instance, root )
        elif instance.type == "txproxy":
            vgxinstance__VGXInstance.InitTXProxy( instance, root )
        elif instance.type == "search":
            vgxinstance__VGXInstance.InitSearch( instance, root )
        elif instance.type == "dispatch":
            vgxinstance__VGXInstance.InitDispatcher( instance, root )
        else:
            raise Exception( "Unknown instance type: {}".format(instance.type) )

        # Start HTTP server
        pyvgx.system.StartHTTP(
            port       = instance.hport,
            ip         = instance.ip,
            prefix     = instance.prefix,
            servicein  = instance.s_in,
            dispatcher = instance.cfdispatcher
        )

        sysplugin__SetSystemDescriptor( descriptor.AsDict(), ident=id )

        vgxinstance__VGXInstance.LoadLocalTestPlugins( instance )

        vgxinstance__VGXInstance.LoadPlugins( instance, plugins )

        if instance.attach:
            attacher = threading.Thread( target=vgxinstance__VGXInstance.TryAttach, args=(instance,) )
            attacher.start()


        return instance



    @staticmethod
    def TryAttach( instance ):
        if not instance.attach:
            return
        # Best effort
        try:
            n = 5
            while n > 0:
                try:
                    instance.Attach()
                    break
                except Exception:
                    time.sleep(1.0)
                    n -= 1
        except Exception:
            pass



import pyvgx
pyvgx.VGXInstance = vgxinstance__VGXInstance
