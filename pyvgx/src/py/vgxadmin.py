#!/usr/bin/env python3
import sys
import os
import pprint

os.environ["PYVGX_NOBANNER"] = "1"

import pyvgx

pyvgx.initadmin()


Descriptor = pyvgx.Descriptor
VGXRemote = pyvgx.VGXRemote
VGXAdmin = pyvgx.VGXAdmin


def main():
    optidx = 1
    address = None
    if len( sys.argv ) > 1:
        if not sys.argv[1].startswith("-"):
            address = sys.argv[1]
            optidx += 1

    program = os.path.basename( sys.argv[0] )
    arguments = sys.argv[optidx:]

    try:
        R = VGXAdmin.Run( arguments=arguments, address=address, program=program, default_descriptor_filename="vgx.cf" )
        if R:
            print()
            print( "Result:" )
            if type(R) is list:
                for ret in R:
                    if ret is not None:
                        pprint.pprint( ret )
            else:
                pprint.pprint( R )
            print()
    except Exception as ex:
        print( ex )
        sys.exit(-1)



if __name__ == "__main__":
    main()
    sys.exit(0)


