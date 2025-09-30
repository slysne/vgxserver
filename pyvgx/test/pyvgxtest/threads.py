###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    threads.py
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

from threading import Thread, Lock
import time


class WorkerTimeout( Exception ): pass




###############################################################################
# Worker
#
###############################################################################
class Worker( Thread ):
    """
    """
    def __init__(self, name):
        Thread.__init__(self, name=name)
        self._task = []
        self._result = []
        self._lock = Lock()
        self._idle = True
        self._dead = False
        self.daemon = True
        self.start()



    def terminate( self ):
        self._lock.acquire()
        try:
            self.running = False
        finally:
            self._lock.release()



    def terminate_sync( self, timeout ):
        t0 = time.time()
        delta = timeout if timeout >= 0 else 1e9
        deadline = t0 + delta
        self.terminate()
        while not self.is_dead():
            now = time.time()
            if now > deadline:
                raise WorkerTimeout( "after %f seconds" % (now - t0) )
            time.sleep( 0.01 )



    def perform( self, func, *args, **kwds ):
        self._lock.acquire()
        try:
            self._idle = False
            self._task.append( (func, args, kwds) )
        finally:
            self._lock.release()



    def perform_sync( self, timeout, func, *args, **kwds ):
        t0 = time.time()
        delta = timeout if timeout >= 0 else 1e9
        deadline = t0 + delta
        self.perform( func, *args, **kwds )
        while not self.is_idle():
            now = time.time()
            if now > deadline:
                raise WorkerTimeout( "after %f seconds" % (now - t0) )
            time.sleep( 0.01 )



    def is_idle( self ):
        self._lock.acquire()
        try:
            return self._idle
        finally:
            self._lock.release()



    def task_size( self ):
        """
        May override
        """
        return len( self._task )



    def size_backlog( self ):
        sz = 0
        self._lock.acquire()
        try:
            sz = self.task_size()
        finally:
            self._lock.release()
        return sz



    def clear( self ):
        self._lock.acquire()
        try:
            self._task = []
            self._result = []
        finally:
            self._lock.release()



    def set_running( self ):
        self._lock.acquire()
        try:
            self.running = 1
        finally:
            self._lock.release()



    def is_running( self ):
        self._lock.acquire()
        try:
            return self.running
        finally:
            self._lock.release()



    def set_dead( self ):
        self._lock.acquire()
        try:
            self._dead = 1
        finally:
            self._lock.release()



    def is_dead( self ):
        self._lock.acquire()
        try:
            return self._dead
        finally:
            self._lock.release()



    def collect( self ):
        ret = None
        self._lock.acquire()
        try:
            ret = self._result.pop(0)
        finally:
            self._lock.release()
        return ret



    def run( self ):
        self.set_running()
        while self.is_running():
            func = None
            self._lock.acquire()
            try:
                if len(self._task) > 0:
                    func, args, kwds = self._task.pop(0)
                else:
                    self._idle = True
            finally:
                self._lock.release()
                if func is None:
                    time.sleep(0.01)
            # Do the task now
            if func:
                try:
                    ret = func( *args, **kwds )
                    self._lock.acquire()
                    try:
                        self._result.append( ret )
                    finally:
                        self._lock.release()
                except Exception as ex:
                    print( "Task exception: %s" % ex )
                    #print( "Terminating thread %s" % repr(self) )
                    #raise
        self.set_dead()
        self._idle = True
