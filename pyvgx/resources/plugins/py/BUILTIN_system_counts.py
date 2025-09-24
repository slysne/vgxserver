import pyvgx


def sysplugin__system_counts( request:pyvgx.PluginRequest, headers:dict, idlist:str="" ):
    """
    Multi-node system object counts
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
                    objcnt, headers = sysplugin__SendAdminRequest( host, aport, path="/vgx/objcnt", timeout=2.0 )
                except:
                    objcnt = None
            R.append({
                "id": id,
                "hostname": host,
                "adminport": aport,
                "objcnt": objcnt
            })
    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True

    return result

pyvgx.system.AddPlugin( plugin=sysplugin__system_counts )

