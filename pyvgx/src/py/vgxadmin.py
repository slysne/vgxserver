###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    vgxadmin.py
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

import sys
import os
import pprint

os.environ["PYVGX_NOBANNER"] = "1"

import pyvgx

pyvgx.initadmin()


Descriptor = pyvgx.Descriptor
VGXRemote = pyvgx.VGXRemote
VGXAdmin = pyvgx.VGXAdmin



###############################################################################
# main
#
###############################################################################
def main():
    """
    """
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
