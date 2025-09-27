/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _fhbase.h
 * Author:  Stian Lysne <...>
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

#ifndef FHBASE_H
#define FHBASE_H

#include "framehash.h"


/**********************************************************************
The largest order (p) of a leaf frame. Chains are introduced when
attempting to expand a frame beyond this order.
***********************************************************************/
#define _FRAMEHASH_P_MAX 6 /* never higher than FRAMEHASH_MAX_P_MAX */


/**********************************************************************
The smallest order (p) of a leaf frame. New frames will start at this order.
***********************************************************************/
#define _FRAMEHASH_P_MIN 0


/**********************************************************************
Frames will expand in increments of this value. Smaller values minimize
memory waste but will trigger rehashing more often. Larger values reduce
the number of rehashes but waste more memory on average. Never less than 1.
***********************************************************************/
#define _FRAMEHASH_P_INCREMENT 2


/**********************************************************************
The number of cells in a basement
***********************************************************************/
#define _FRAMEHASH_BASEMENT_SIZE 6


/**********************************************************************
The largest load factor allowed. When load factor exceeds this value
the frame is expanded if possible, or chains to subframes are introduced.
Value is given as an integer percentage.
***********************************************************************/
#define _FRAMEHASH_MAX_LOADFACTOR 90


/**********************************************************************
The smallest load factor we allow. When load factor falls below this
value the frame is rehashed into a smaller frame.
Value is given as an integer percentage.
***********************************************************************/
#define _FRAMEHASH_MIN_LOADFACTOR 40


/**********************************************************************
Frames of this order or lower will be treated differently with respect
to rehash threshold, rehash increments, and the use of header cells.
                        p <= this_value     |      p > this_value
                        -----------------------------------------
Rehash increment      :         1           |  _FRAMEHASH_P_INCREMENT
Rehash fill threshold :     Full frame      |  _FRAMEHASH_MAX_LOADFACTOR
Use header cells?     :        Yes          |             No
***********************************************************************/
#define _FRAMEHASH_MAX_P_SMALL_FRAME 2


/**********************************************************************
Starting at this domain, maximum utilization of memory is prioritized,
i.e. _FRAMEHASH_MAX_LOADFACTOR is suspended (set to 100), and 
_FRAMEHASH_P_INCREMENT is suspended (set to 1).
***********************************************************************/
#define _FRAMEHASH_OPTIMIZE_MEM_FROM_DOMAIN 3


/**********************************************************************
Set to 1 to enable Cache Frames in Framehahs. Set to 0 to disable the 
creation of Cache Frames except for the top level frame (domain=0) which
is always a cache frame. Note that when this value is 0, the top frame
will not perform any caching although it is a cache frame.
Enabling cache frames will speed up read access for small working sets.
***********************************************************************/
#define _FRAMEHASH_ENABLE_CACHE_FRAMES 1


/**********************************************************************
Set to 1 to enable caching of read items into cache frames.
***********************************************************************/
#define _FRAMEHASH_ENABLE_READ_CACHE 1


/**********************************************************************
Set to 1 to cache items only once per read, in the deepest cache domain
that does not already cache the item. It takes several consecutive reads
of the same item to bring it all the way to the top cache, thus making 
lookup of that item faster and faster each time while avoiding destroying
the top cache when mixing small working sets with random reads.
***********************************************************************/
#define _FRAMEHASH_CONSERVATIVE_READ_CACHE 1


/**********************************************************************
When _FRAMEHASH_ENABLE_CACHE_FRAMES is 1, this setting determines the last domain
where Internal Frames will be converted to Caches Frames when all chains
are active. Limiting how deeply the caches go will reduce overhead for
more random workloads since caching will write to memory.
***********************************************************************/
#define _FRAMEHASH_MAX_CACHE_DOMAIN 3


/**********************************************************************
Set to 1 to enable _mm_prefetch instructions to be issued when probing
zones in leaves. Set to 0 to disable. (Note that explicit prefetch hints
do not necessarily improve performance and may in fact hurt performance
due to wasted memory cycles.)
***********************************************************************/
#define _FRAMEHASH_ENABLE_ZONE_PREFETCH 1


/**********************************************************************
Set to 1 to enable _mm_prefetch instructions to be issued for cache chains
before the cache cells are probed for a match. Set to 0 to disable.
***********************************************************************/
#define _FRAMEHASH_ENABLE_CACHE_CHAIN_PREFETCH 0


/**********************************************************************
Set to 1 to use macros for low level cell operations. Set to 0 to use
functions, primarily to assist debugging.
***********************************************************************/
#ifdef FRAMEHASH_NO_CELL_MACROS
#define _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS 0
#else
#define _FRAMEHASH_USE_MACRO_FOR_CELL_ACCESS 1
#endif


/**********************************************************************
Set to 1 to use the _mm_stream_* intrinsics when reading and writing
cells from memory.
***********************************************************************/
#define _FRAMEHASH_USE_MEMORY_STREAMING 0


/**********************************************************************
Only effective when _FRAMEHASH_USE_MEMORY_STREAMING is 1, this sets the
domain where streaming becomes enabled. Domain numbers lower than this
will not use streaming even when streaming is globally enabled.
***********************************************************************/
#define _FRAMEHASH_MEMORY_STREAMING_MIN_DOMAIN 2




#endif
