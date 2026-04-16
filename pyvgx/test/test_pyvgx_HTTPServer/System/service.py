import argparse
import os
import time

import pyvgx
from pyvgx import *



DESCRIPTOR = None

def SetGlobalDescriptor( descriptor ):
    global DESCRIPTOR
    DESCRIPTOR = descriptor





def SimpleEchoEngine( request:PluginRequest, graph, message:str ):
    return f"engine_{message}"



def SimpleEchoPre( request:PluginRequest, graph, message:str ):
    return f"pre_{message}"



def GetPluginDefinitions():
    """
    Returns:
    [ 
      {
        "name"   : <str>,
        "engine" : <func>,
        "pre"    : <func>,
        "post"   : <func>,
        "graph"  : <str>
      }, 
      ...
    ]
    """

    plugins = [
        { 
            "name"   : "SimpleEcho",
            "engine" : SimpleEchoEngine,
            "pre"    : SimpleEchoPre,
            "post"   : None,
            "graph"  : "*"
        }
    ]

    return plugins





def RunService( instance_id, vgxroot, descriptor_file ):

    outpath = f"out_{instance_id}.txt"
    SetOutputStream( outpath )

    pyvgx.initadmin()

    # Plugin definitions
    plugins = GetPluginDefinitions()

    # VGX System descriptor
    descriptor = pyvgx.Descriptor( descriptor_file, printf=pyvgx.LogInfo )
    SetGlobalDescriptor( descriptor )

    # Start VGX instance
    instance = pyvgx.VGXInstance.StartInstance( id         = instance_id,
                                                descriptor = descriptor,
                                                basedir    = vgxroot,
                                                plugins    = plugins)
    # Run until SIGINT
    pyvgx.system.RunServer( name=instance.description )






