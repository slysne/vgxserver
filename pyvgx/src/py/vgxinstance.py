import sys
import pyvgx

pyvgx.initadmin()

GetDescriptor = pyvgx.VGXInstance.GetDescriptor
StartInstance = pyvgx.VGXInstance.StartInstance



def main( id, descriptor_file=None, basedir="." ):
    descriptor = GetDescriptor( descriptor_file )
    instance = StartInstance( id, descriptor, basedir )
    pyvgx.system.RunServer( name=instance.description )



if __name__ == "__main__":
    if len(sys.argv) > 1:
        id = sys.argv[1]
        if len(sys.argv) > 2:
            descriptor_file = sys.argv[2]
        else:
            descriptor_file = None
        main( id, descriptor_file )



