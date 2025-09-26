###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    raw2files.py
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

import sys


def process( data ):
    py = []
    for line in data:
        if line.startswith( "static" ):
            line = line.replace( "static const char *PY_BUILTIN__", "" )
            line = line.replace( "[] = {", "" )
            name = line.strip() + ".py"
        else:
            line = line.strip()
            line = line.replace( "\",", "" )
            line = line.lstrip( "\"" )
            line = line.replace( "};", "" )
            line = line.replace( "\\\"", "\"" )
            line = line.replace( "'''", "\"\"\"" )
            line = line.replace( "''", "\"\"" )
            line = line.replace( "' '", "\" \"" )
            if line.startswith( "NULL" ):
                f = open( name, "w" )
                for x in py:
                    f.write( x )
                f.close()
                py = []
            else:
                py.append( line + "\n" )



def main():
    f = open( sys.argv[1] )
    data = f.readlines()
    f.close()
    process( data )




if __name__ == "__main__":
    main()
