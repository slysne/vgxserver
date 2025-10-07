###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  vgx
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
import re
import time

OUT_FILE = "_vxhtml.h"
ARRAY_PREFIX = "VGX_BUILTIN_"



###############################################################################
# process_html
#
###############################################################################
def process_html( kind, name, data, ARR ):
    """
    """
    L = []
    basename = os.path.basename( name )
    name = basename.replace( ".html", "" )
    name = name.replace( ".", "_" )
    name = name.replace( "-", "_" )
    array_name = '{}HTML_{}'.format(ARRAY_PREFIX, name)
    ARR.append( (kind, basename, array_name ) )
    line = 'const char *{}[] = {{'.format( array_name )
    L.append( line )
    for line in data.split( '\n' ):
        line = line.rstrip()
        line = line.replace( '\\', '\\\\' )
        line = line.replace( '"', r'\"')
        if "vgx v?.?.?":
           line = re.sub( "(vgx v.*)(</span>)", "%VERSION%\\2", line ) 
        L.append( '    "{}",'.format(line) )
    L.append( '    0' )
    L.append( '};' )
    L.append( '' )
    L.append( '' )
    L.append( '' )
    return L




###############################################################################
# process_css
#
###############################################################################
def process_css( kind, name, data, ARR ):
    """
    """
    L = []
    basename = os.path.basename( name )
    name = basename.replace( ".css", "" )
    name = name.replace( ".", "_" )
    name = name.replace( "-", "_" )
    array_name = '{}CSS_{}'.format(ARRAY_PREFIX, name)
    ARR.append( (kind, basename, array_name ) )
    line = 'const char *{}[] = {{'.format( array_name )
    L.append( line )
    for line in data.split( '\n' ):
        line = line.rstrip()
        line = line.replace( '\\', '\\\\' )
        line = line.replace( '"', r'\"')
        L.append( '    "{}",'.format(line) )
    L.append( '    0' )
    L.append( '};' )
    L.append( '' )
    L.append( '' )
    L.append( '' )
    return L




###############################################################################
# process_image
#
###############################################################################
def process_image( kind, name, data, ARR ):
    """
    """
    L = []
    basename = os.path.basename( name )
    name = basename.replace( ".", "_" )
    name = name.replace( "-", "_" )
    array_name = '{}IMAGE_{}'.format(ARRAY_PREFIX, name)
    ARR.append( (kind, basename, array_name ) )
    line = 'const char *{}[] = {{'.format( array_name )
    L.append( line )
    B = []
    def output():
        s = "".join(B)
        B.clear()
        line = '    "{}",'.format(s)
        L.append( line )
    for x in data:
        B.append( "{:02X}".format(x) )
        if len(B) == 60:
            output()
    if len(B):
        output()
    L.append( '    0' )
    L.append( '};' )
    L.append( '' )
    L.append( '' )
    L.append( '' )
    return L




###############################################################################
# process_js_binary
#
###############################################################################
def process_js_binary( kind, name, data, ARR ):
    """
    """
    L = []
    basename = os.path.basename( name )
    name = basename.replace( ".", "_" )
    name = name.replace( "-", "_" )
    array_name = '{}JS_{}'.format(ARRAY_PREFIX, name)

    if "jquery" in name:
        basename = "jquery.js"
        
    ARR.append( (kind, basename, array_name ) )
    line = 'const char *{}[] = {{'.format( array_name )
    L.append( line )
    B = []
    def output():
        s = "".join(B)
        B.clear()
        line = '    "{}",'.format(s)
        L.append( line )
    for x in data:
        B.append( "{:02X}".format(x) )
        if len(B) == 60:
            output()
    if len(B):
        output()
    L.append( '    0' )
    L.append( '};' )
    L.append( '' )
    L.append( '' )
    L.append( '' )
    return L




###############################################################################
# process_js
#
###############################################################################
def process_js( kind, name, data, ARR ):
    """
    """
    L = []
    basename = os.path.basename( name )
    name = basename.replace( ".", "_" )
    name = name.replace( "-", "_" )
    array_name = '{}JS_{}'.format(ARRAY_PREFIX, name)
    ARR.append( (kind, basename, array_name ) )
    line = 'const char *{}[] = {{'.format( array_name )
    L.append( line )
    for line in data.split( '\n' ):
        line = line.rstrip()
        line = line.replace( '\\', '\\\\' )
        line = line.replace( '"', r'\"')
        L.append( '    "{}",'.format(line) )
    L.append( '    0' )
    L.append( '};' )
    L.append( '' )
    L.append( '' )
    L.append( '' )
    return L




###############################################################################
# convert_files
#
###############################################################################
def convert_files( items, outname ):
    """
    """
    OUT = []
    ARR = []
    for kind, name in items:
        if kind in ["IMAGE"] or "jquery" in name:
            mode = "rb"
        else:
            mode = "r"
        f = open( name, mode )
        data = f.read()
        f.close()
        print( name )
        if kind == "HTML":
            lines = process_html( kind, name, data, ARR )
        elif kind == "CSS":
            lines = process_css( kind, name, data, ARR )
        elif kind == "IMAGE":
            lines = process_image( kind, name, data, ARR )
        elif kind == "JS":
            if mode == "rb":
                lines = process_js_binary( kind, name, data, ARR )
            else:
                lines = process_js( kind, name, data, ARR )
        else:
            continue
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
    for kind, name in items:
        parts = os.path.abspath( name ).split(os.sep)
        index = parts.index( "vgx" )
        path = os.path.join(*parts[index + 1:]).replace( "\\", "/" ).removeprefix( "/" )
        fout.write( '#    {}\n'.format(path) )
    fout.write( '#\n' )
    fout.write( '#\n' )
    fout.write( '#########################################\n' )
    fout.write( '*/\n\n' )
    define = OUT_FILE.upper().replace( '.', '_' )
    fout.write( '#ifndef {}\n'.format(define) )
    fout.write( '#define {}\n'.format(define) )
    fout.write( "\n\n\n" ) 

    for line in OUT:
        fout.write( "{}\n".format(line) )

    fout.write( "\n\n\n" ) 

    ARTIFACT_FMT = """// {}
  {{ .name = \"/{}\", 
    .namehash = 0, 
    .MediaType = {}, 
    .raw = {{ .data = {} }}, 
    .servable = {{ .bytes = 0, .sz = -1, .public = {} }}
  }},"""
    fout.write( "static vgx_server_artifact_t VGX_SERVER_ARTIFACTS[] = {\n" )
    for kind, basename, array_name in ARR:
        public = "true"
        if kind == "IMAGE":
            imgtype = basename.split(".")[-1]
            if imgtype not in ['ico', 'gif', 'png', 'jpg']:
                imgtype = "ANY"
            MediaType = "MEDIA_TYPE__image_{}".format( imgtype )
        elif kind == "JS":
            MediaType = "MEDIA_TYPE__application_javascript"
        elif kind == "HTML":
            MediaType = "MEDIA_TYPE__text_html"
            basename = basename.replace( ".html", "" )
            if basename not in ["header", "footer"]:
                public = "false"
        elif kind == "CSS":
            MediaType = "MEDIA_TYPE__text_css"
        else:
            MediaType = "MEDIA_TYPE__text_plain"
        line = ARTIFACT_FMT.format( basename, basename, MediaType, array_name, public )
        fout.write( "  {}\n".format(line) )
    fout.write( "  \n" )
    fout.write( "  0\n" )
    fout.write( "};\n" ) 
    
    fout.write( "\n\n\n" ) 
    fout.write( '#endif\n\n' )
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
    FILES = []
    DIR = sorted(os.listdir( input_dir ))
    INPUT = []
    for ext in [".html",".css",".js",".ico",".gif",".png"]:
        for name in DIR:
            if name.endswith(ext):
                INPUT.append(name)
    for name in INPUT:
        if name.endswith( ".html" ):
            kind = "HTML"
        elif name.endswith( ".css" ):
            kind = "CSS"
        elif name.endswith( ".js" ):
            kind = "JS"
        elif name.endswith( ".gif" ) or name.endswith( ".ico" ) or name.endswith(".png"):
            kind = "IMAGE"
        else:
            print( "skipping {}".format(name) )
            continue
            
        fullpath = os.path.join( input_dir, name )
        item = ( kind, fullpath )
        FILES.append( item )

    outname = os.path.join( output_dir, OUT_FILE )

    convert_files( FILES, outname )




if __name__ == "__main__":
    main()
