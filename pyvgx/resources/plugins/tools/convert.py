###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    convert.py
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
import time

OUT_FILE = "_pyvgx_plugin_builtins.h"
ARRAY_PREFIX = "PYVGX_BUILTIN__"
EXPORT_TABLE = "PYVGX_PLUGIN_BUILTINS"


###############################################################################
# process
#
###############################################################################
def process( name, data ):
    """
    """
    OUT = []
    name = os.path.basename( name )
    name = name.replace( ".py", "" )
    line = "static const char *{}{}[] = {{".format( ARRAY_PREFIX, name )
    OUT.append( line )
    OUT.append( '    "# {}"'.format( name ) )

    for line in data:
        sline = line.strip()
        # Ignore comment lines or empty lines
        if not sline or sline.startswith("#"):
            continue
        line = line.rstrip()
        line = line.replace( '\\', '\\\\' )
        line = line.replace( '"', r'\"' )
        line = '    "{}",'.format( line )
        OUT.append( line )

    OUT.append( '    0' )
    OUT.append( '};' )
    OUT.append( '' )
    OUT.append( '' )
    OUT.append( '' )
    return OUT




###############################################################################
# convert_files
#
###############################################################################
def convert_files( names, outname ):
    """
    """
    OUT = []
    for name in names:
        f = open( name )
        data = f.readlines()
        f.close()
        lines = process( name, data )
        OUT.extend( lines )

    fout = open( outname, "w" )
    fout.write( '/*\n' )
    fout.write( '#########################################\n' )
    fout.write( '#\n' )
    fout.write( '# File: {}\n'.format( OUT_FILE ) )
    fout.write( '# Time: {}\n'.format( time.ctime() ) )
    fout.write( '#\n' )
    fout.write( '#\n' )
    fout.write( '# Automatically generated from input sources:\n' )
    fout.write( '#\n' )
    for name in names:
        parts = os.path.abspath( name ).split(os.sep)
        index = parts.index( "pyvgx" )
        path = os.path.join(*parts[index + 1:]).replace( "\\", "/" ).removeprefix( "/" )
        fout.write( '#    {}\n'.format( path ) )
    fout.write( '#\n' )
    fout.write( '#\n' )
    fout.write( '#########################################\n' )
    fout.write( '*/\n\n' )
    define = OUT_FILE.upper().replace( '.', '_')
    fout.write( '#ifndef {}\n'.format( define ) )
    fout.write( '#define {}\n'.format( define ) )
    fout.write( "\n\n\n" ) 

    for line in OUT:
        fout.write( "{}\n".format( line ) )

    fout.write( 'const char **{}[] = {{\n'.format( EXPORT_TABLE ) )
    for name in names:
        name = os.path.basename( name )
        name = name.replace( '.py', '' )
        name = "{}{}".format( ARRAY_PREFIX, name )
        fout.write( '    {},\n'.format( name ) )
    fout.write( '    0\n' )
    fout.write( '};\n' )
    fout.write( "\n\n\n" ) 
    fout.write( '#endif\n' )
    fout.write( "\n\n" ) 
    fout.close()



###############################################################################
# main
#
###############################################################################
def main():
    """
    """
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    os.makedirs(output_dir, exist_ok=True)
    PY = []
    for name in os.listdir( input_dir ):
        if name.endswith( ".py" ):
            PY.append( os.path.join( input_dir, name ) )

    outname = os.path.join( output_dir, OUT_FILE )

    convert_files( sorted(PY), outname )




if __name__ == "__main__":
    main()
