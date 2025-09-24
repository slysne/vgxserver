import sys
import os

OUT_FILE = "_pyvgx_plugin_builtins.h"
ARRAY_PREFIX = "PYVGX_BUILTIN__"
EXPORT_TABLE = "PYVGX_PLUGIN_BUILTINS"

def process( name, data ):
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



def convert_files( names, outname ):
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


def main():
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    PY = []
    for name in os.listdir( input_dir ):
        if name.endswith( ".py" ):
            PY.append( os.path.join( input_dir, name ) )

    outname = os.path.join( output_dir, OUT_FILE )

    convert_files( sorted(PY), outname )




if __name__ == "__main__":
    main()



