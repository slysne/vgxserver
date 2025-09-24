import pyvgx


def sysplugin__system_rates( request:pyvgx.PluginRequest, headers:dict, idlist:str="" ):
    """
    Multi-node system service rates
    """

    R = []
    result = {
        "instances": R,
        "message": None
    }

    try:
        D = sysplugin__GetSystemDescriptor()
    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True
        return result

    try:
        instances = D.get("instances",{})
        idlist = idlist.replace(" ","")
        include = [(x,instances[x]) for x in idlist.split(",") if x in instances]
        for id, instance in include:
            host = instance.get("host")
            aport = instance.get("hport",-1) + 1
            if host and aport:
                try:
                    sysplugin__SendAdminRequest( host, aport, path="/vgx/hc", timeout=0.2 )
                    status, headers = sysplugin__SendAdminRequest( host, aport, path="/vgx/status", timeout=2.0 )
                    rates = status["response_ms"]
                    rates["rate"] = status["request"]["rate"]
                    clients = {}
                    clients["connected"] = status["connected_clients"]
                    matrix = {}
                    matrix["active-channels"] = status["dispatcher"]["matrix-active-channels"];
                    matrix["total-channels"] = status["dispatcher"]["matrix-total-channels"];
                except:
                    rates = None
                    clients = None
                    matrix = None
            R.append({
                "id": id,
                "hostname": host,
                "adminport": aport,
                "rates": rates,
                "clients": clients,
                "matrix": matrix
            })
    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True

    return result

pyvgx.system.AddPlugin( plugin=sysplugin__system_rates )

