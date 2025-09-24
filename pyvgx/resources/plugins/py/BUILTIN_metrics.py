import pyvgx
import json

def sysplugin__metrics( request:pyvgx.PluginRequest, headers:dict, content:str, latency:json=[80,90,95,99] ):
    """
    HTTP Server Metrics
    """
    return pyvgx.system.ServerMetrics( latency )

pyvgx.system.AddPlugin( plugin=sysplugin__metrics )

