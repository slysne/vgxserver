###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    vgxinstance.py
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
import pyvgx

pyvgx.initadmin()

GetDescriptor = pyvgx.VGXInstance.GetDescriptor
StartInstance = pyvgx.VGXInstance.StartInstance




###############################################################################
# main
#
###############################################################################
def main( id, descriptor_file=None, basedir="." ):
    """
    """
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
