###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    apply_backlog.py
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

from pyvgx import *


import os
import sys







def apply_transactions_after( tx, sn ):

    txid = int( tx, 16 )

    LogInfo( "Durable @ %032x (sn=%d)" % (txid, sn) )

    durable_TRANSACTION = b"TRANSACTION %032x %016X" % (txid, sn)
    durable_COMMIT = b"COMMIT %032x" % txid

    start_name = None
    start_offset = -1

    sysroot = system.Root()
    txdir = os.path.join( sysroot, "_system", "txlog" )
    txfiles = [ os.path.join( txdir, name ) for name in sorted( os.listdir( txdir ) ) if name.endswith( ".tx" ) ]
 
    # Check if entire set of logs is past the durability point
    if len(txfiles) > 0:
        base = txfiles[0].split( os.sep )[-1].removesuffix(".tx")
        if sn < int( base, 16 ):
            start_name = txfiles[0]
            start_offset = 0
        
    # Find start of backlog
    if start_name is None:
      for name in txfiles:
          LogInfo( "Locating durability point (%s)..." % name )
          f = open( name, "rb" )
          for line in f:
              if line.startswith( durable_TRANSACTION ):
                  LogInfo( "Durable transaction found in %s" % name )
              if line.startswith( durable_COMMIT ):
                  start_name = name
                  start_offset = f.tell()
                  LogInfo( "Backlog starts at offset %d in %s" % (start_offset, start_name) )
                  break
          f.close()
          if start_offset >= 0:
              break

      # Skip files up to the one containing start of backlog
      while len(txfiles) > 0 and txfiles[0] != start_name:
          txfiles.pop(0)

    # Registry state before we start
    LogInfo( "VGX Registry: %s" % system.Registry() )

    answer = input( "Apply backlog? ('Y' to confirm)" )
    if answer.lower() != 'y':
        return


    # Apply backlog
    for name in txfiles:
        f = open( name, "rb" )
        LogInfo( "Applying transactions in %s" % name )
        if name == start_name:
            LogInfo( "Starting at offset %d" % start_offset )
            f.seek( start_offset )
        for line in f:
            op.Consume( line )
        f.close()
        LogInfo( "VGX Registry: %s" % system.Registry() )




def main():
    sysroot = sys.argv[1]
    if len(sys.argv) > 2:
        http = int( sys.argv[2] )
    else:
        http = 0

    system.Initialize( sysroot, http=http, events=False )

    system.System().ResetSerial()

    durable_tx = system.Status()['durable']['id']
    durable_sn = system.Status()['durable']['serial']

    apply_transactions_after( durable_tx, durable_sn )




if __name__ == "__main__":
    main()
