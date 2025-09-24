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



