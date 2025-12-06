from pyvgx import *
import random
import time
import heapq
from collections import deque




ROOT = "root"


MEM = None
Q = None

def INIT(graph, h=300, bw=300, bc=1.0, bmin=100 ):
    MEM = graph.Memory(32)
    Q = graph.NewNeighborhoodQuery(
                                memory  =   MEM,
                                #arc     =   ('lsh32', D_OUT, M_LSH, V_LTE, (0,0)),
                                arc     =   ('lsh32', D_OUT),
                                filter  =   "anncollect( 0.0 )",
                                collect =   C_SCAN,
                                sortby  =   S_RVAL,
                                fields  =   F_VAL | F_ID,
                                result  =   R_LIST,
                                recursion = {
                                    'heap_shadow': h,
                                    'beam_width': bw,
                                    'beam_curve': bc,
                                    'beam_min': bmin,
                                    'arclsh_mincos': 1.0
                                }
    )
    return MEM, Q




MEM, Q = INIT(g)



def search( MEM, Q, graph, probe, k, start):
    MEM.vector = probe
    #Q.arclsh = (probe.LSH32(), 0)
    Q.id = start
    return Q.Execute( hits=k )



def old_search(graph, probe, k, h, start):
    MEM.R1 = probe
    MEM.R2 = -1.0
    MEM.R4 = 0
    MEM.ClearSet()
    heap = [(-1.0,None)] * h
    sim = 1 + graph.sim.Cosine(graph[start], probe)
    item = (sim, start)
    heapq.heapreplace( heap, item )
    queue = deque( [item] )
    while queue:
        sim, node = queue.popleft()
        if sim <= MEM.R2:
            continue
        Q.id = node
        for item in Q.Execute():
            if item[0] <= MEM.R2:
                break
            queue.append( item )
            heapq.heapreplace( heap, item )
            MEM.R2 = heap[0][0]
    return heapq.nlargest(k, heap)



def ptest(MEM, Q, graph, probe, k=10, root=None, recall=False, recall_only=False, recall_with_timing=False, fname=None ):
    start = root if root is not None else graph[ROOT].Terminals()[0]
    if type(probe) is not Vector:
        raise TypeError("probe must be vector")
    t0 = time.perf_counter_ns()
    result = search( MEM, Q, graph, probe, k, start=start )
    t1 = time.perf_counter_ns()
    t_ms = (t1-t0)/1000000.0
    if not recall and not recall_only:
        for score, id in result:
            print( f"{score:0.3f}  {graph[id]['title']}" )
    else:
        fname = fname if fname is not None else graph[result[0][1]]['fname']
        scan_result = scan(graph, probe, k=k, fname=fname)
        scanned = set([ id for id, score in scan_result ])
        searched = set([ id for score, id in result ])
        n = 0
        if not recall_only:
            for id, score in scan_result:
                n += 1
                if id in searched:
                    print( f"{n:3d}. [{graph[id]['dist']}]   {score:0.3f}   {id}  {graph[id]['title']}" )
                else:
                    print( f"{n:3d}. [{graph[id]['dist']}]   {score:0.3f} ! {id} ({graph[id]['title']})" )
        r = (len(scanned) - len(scanned - searched)) / len(scanned)
        if not recall_only:
            print( f"RECALL={100*r:0.1f}" )
    if recall_only:
        if recall_with_timing:
            return r, t_ms
        else:
            return r
    else:
        print( f"{t_ms:0.5f} ms" )
    
   



def rtest(graph, r=128, k=10, h=64, root=None):
    probe = graph.sim.rvec(r)
    ptest(graph, probe, k, h, root=root)



SCAN_CACHE = {}

def scan(g, probe, k=10, sortdir=S_DESC, fname=None):
    key = f"{probe.ident}_{k}"
    result = SCAN_CACHE.get( key )
    if result is not None:
        return result
    mem = g.Memory(4)
    mem.R1 = probe.internal if type(probe) is Vector else graph.sim.NewVector(probe).internal
    cond = f"!isnan(vertex.property('dist'))"
    if fname:
        cond += f" && vertex.property('fname') == '{fname}'"
    result = g.Vertices(
        memory = mem,
        condition = { 'filter': cond },
        sortby = S_RANK|sortdir,
        rank = "1 + cos_pi8( r1, vertex.vector)",
        hits = k,
        fields = F_ID|F_RANK,
        result = R_LIST
    )
    SCAN_CACHE[key] = result
    return result



def pscan(graph, probe, k=10):
    t0 = time.time()
    for id, score in scan( graph, probe, k ):
        #print( f"{score:0.3f}  {id}  {graph[id]['title']}" )
        print( f"{score:0.3f}  {graph[id]['title']}" )
    t1 = time.time()
    print( f"{1000*(t1-t0):0.5f} ms" )

   
"""

# Get probe vectors from another partition
g2 = Graph("samples")
load( g2, "part2.dump", loadarcs=False )
UNINDEXED_VECTORS = [g2[g2.GetVertexID()].GetVector() for i in range(5000)]

# Make vectors for our searchable graph from those other vectors
PROBES = [ g.sim.NewVector(v.external, cosine_mode=1)  for v in UNINDEXED_VECTORS]


def fillcache( g, probes ):
    fname = g[g[ROOT].Terminals()[0]]['fname']
    N = len(probes)
    n = 0
    for p in probes:
        n += 1
        scan(g, p, fname=fname)
        if not n % 100:
            print( f"{100*n/N:.1f}%", end="\r", flush=1 )
    print( f"{100:.1f}%" )





PROBES = [ g.sim.NewVector(p, cosine_mode=1) for p in g['cache']['probes'] ]

SCAN_CACHE = g['cache']['SCAN_CACHE']



def get_noisy( vector, c=70 ):
  return g.sim.NewVector( [x+c*(random.random()-0.5) for x in vector.external], cosine_mode=1 )


noisy_PROBES = [ get_noisy(v) for v in RANDOM_ITEMS ]
easy_PROBES = RANDOM_ITEMS

random_PROBES = [g.sim.rvec(128) for i in range(5000)]

PROBES = random_PROBES

PROBES = [ v for v in RANDOM_ITEMS ]





import threading

def workall(g, PROBES):
    for h in [1024, 768, 512, 384, 256, 192, 128, 96, 64, 48, 32, 24]:
        bw = 3*h // 4
        for bc in [1.0, 0.9, 0.8, 0.7]:
            MEM, Q = INIT(g, h=h, bw=bw, bc=bc, bmin=8)
            result, qps = testrecall(MEM, Q, g, k=10, P=PROBES, V=ENTRIES, show=False)
            print( f"heap_shadow={h} beam_width={bw} beam_taper={bc} --> {result}" )
            print( "-----------------------------------------" )


def work(g, PROBES, k, h, bw, bc, r_result, show=False):
    MEM, Q = INIT(g, h=h, bw=bw, bc=bc, bmin=8)
    testrecall(MEM, Q, g, k, P=PROBES, V=ENTRIES, show=show, r_result=r_result)


def threadwork( g, N, PROBES, k, h, bw, bc ):
    T = []
    sz = len(PROBES) // N
    i = 0
    for n in range(N):
        sample = PROBES[i:i+sz]
        r_result = {}
        args = (g, sample, k, h, bw, bc, r_result)
        t = threading.Thread( target=work, args=args )
        T.append( (t, r_result) )
        i += sz
    for t,_ in T:
        t.start()
    alive = len(T)
    while alive:
        alive = sum([1 for t,_ in T if t.is_alive()])
        time.sleep(1)
    for t,_ in T:
        t.join(timeout=1.0) # just in case
    recall = sum([r_result['recall'] for _, r_result in T]) / len(T)
    latency = sum([r_result['latency'] for _, r_result in T]) / len(T)
    qps = sum([r_result['qps'] for _, r_result in T])
    evals = sum([r_result['evals'] for _, r_result in T]) // len(T)
    accepts = sum([r_result['accepts'] for _, r_result in T]) // len(T)
    evalrate = (evals / (latency/1000)) / 1000000
    acceptrate = 100*accepts/evals
    config = f"threads={N} heap_shadow={h} beam_width={bw} beam_taper={bc}"
    result = f"qps@{N}={qps:0.1f} recall={recall:0.3f}@{k} latency={latency:0.2f}ms evals={evals} ({evalrate:0.1f}M/s/t {N*evalrate:0.1f}M/s) accepts={accepts} ({acceptrate:0.1f}%)"
    print( f"{config} --> {result}" )



def threadtest( g, N, PROBES, heaps=None, bwfactor=3/4, bcs=None ):
    if heaps is None:
        heaps = [1024, 768, 512, 384, 256, 192, 128, 96, 64, 48, 32, 24]
    if bcs is None:
        bcs = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]
    for h in heaps:
        bw = int(bwfactor * h)
        for bc in bcs:
            threadwork( g, N, PROBES, k=10, h=h, bw=bw, bc=bc ) 



ENTRIES = g.Vertices( hits=len(PROBES), sortby=S_RANDOM, condition={'type':'item', 'outdegree':(V_GT,63)} )

TOO_CLOSE = set()
for i in range(len(ENTRIES)-1):
    for k in range(i+1,len(ENTRIES)):
        if g.sim.Cosine( g[ENTRIES[i]], g[ENTRIES[k]] ) > 0.4:
            TOO_CLOSE.add(ENTRIES[k])

SPREAD = list( set(ENTRIES) - TOO_CLOSE )

A = g.NewVertex( 'entry' )

for node in SPREAD:
    lsh32 = g[node].GetVector().LSH32()
    r = g.Connect( A, ('lsh32', M_LSH|M_FWDONLY, lsh32), node )

A.Close()



"""

def testrecall( MEM, Q, g, k=25, N=1500, P=None, V=None, show=True, r_result=None ):
    if P is None and V is None:
        P = [g[g.GetVertexID()].GetVector() for i in range(N)]
        V = g.Vertices( hits=len(P), sortby=S_RANDOM, condition={'type':'item'} )
    else:
        N = min(len(P), len(V))
    R = []
    T = []
    cnt = 0
    tot_neval = 0
    tot_nhampass = 0
    tot_nsimpass = 0
    fname = g[g[ROOT].Terminals()[0]]['fname']
    t0 = time.perf_counter()
    for n in range(len(P)):
        cnt += 1
        p = P[n]
        entry = V[n]
        r, t_ms = ptest(MEM, Q, g, p, k=k, root=entry, recall_only=1, recall_with_timing=1, fname=fname)
        R.append(r)
        T.append(t_ms)
        neval, nhampass, nsimpass, _ = MEM.counters
        tot_neval += neval
        tot_nhampass += nhampass
        tot_nsimpass += nsimpass
        eval_per_query = tot_neval // cnt
        pct_hampass = 100.0*tot_nhampass / tot_neval
        pct_simpass = 100.0*tot_nsimpass / tot_neval
        avg_recall = sum(R) / len(R)
        avg_latency = sum(T) / len(T)
        if show:
            print( f"{cnt}/{len(P)} {r:0.3f} {t_ms:0.2f}ms  avg:{avg_recall:0.3f} {avg_latency:0.2f}ms  {eval_per_query}e/q  {pct_hampass:0.1f}%h/e {pct_simpass:0.1f}%s/q    ", end="\r", flush=1 )
    if show:
        print( f"{cnt}/{len(P)} {r:0.3f} {t_ms:0.2f}ms  avg:{avg_recall:0.3f} {avg_latency:0.2f}ms  {eval_per_query}e/q  {pct_hampass:0.1f}%h/e {pct_simpass:0.1f}%s/q    " )
    else:
        t1 = time.perf_counter()
        qps = cnt / (t1-t0) if cnt else 0.0
        if r_result is None:
            return f"recall={avg_recall:0.3f}@{k} latency={avg_latency:0.2f}ms qps={qps:0.1f}"
        if type(r_result) is dict:
            r_result['recall'] = avg_recall
            r_result['latency'] = avg_latency
            r_result['qps'] = qps
            r_result['evals'] = tot_neval // cnt
            r_result['accepts'] = tot_nsimpass // cnt
            return avg_recall, avg_latency, qps







def deltop(graph):
    toparcs = graph.Arcs( condition={'arc':('top',D_OUT)}, result=R_LIST )
    for init, d, rel, m, x, term in toparcs:
        r = graph.Disconnect( init, (rel,D_OUT), term )





def toplayer(graph):
    # Clean up
    deltop(graph)
    # Create new
    top_X = g.Vertices( condition={ 'filter':'vertex.ideg > 90' }, sortby=S_IDEG, fields=F_ID, hits=5000 )
    n = 0
    m = 0
    for init in top_X:
        A = graph.OpenVertex( init )
        n += 1
        for term in top_X:
            if term == init:
                continue
            B = graph.OpenVertex( term )
            sim = graph.sim.Cosine(A,B)
            if sim < -0.3:
                m += 1
                graph.Connect( A, ('top', M_SIM|M_FWDONLY, sim), B )
            B.Close()
        A.Close()
        print( f"\r{100*n/len(top_X):0.3f}% arcs={m}", end="", flush=True )
    print()






def fulltraverse( graph, start ):
    visited = set()
    explored = set()
    mem = graph.Memory(4)
    R = graph.OpenVertex( start )
    queue = deque([(start,0)])
    while queue:
        node, dist = queue.popleft()
        if node in explored:
            continue
        A = graph.OpenVertex( node )
        sim = graph.sim.Cosine( R, A )
        A['dist'] = dist
        A['sim'] = sim
        neighbors = graph.Neighborhood( A, D_OUT, filter='vset.add(next) > 0', memory=mem )
        A.Close()
        explored.add( node )
        if not neighbors:
            continue
        visited.update( neighbors )
        queue.extend( [(x,dist+1) for x in neighbors] )
        print( f"\rv={len(visited)} x={len(explored)} q={len(queue)} d={dist} s={sim}            ", end="", flush=True )
    R.Close()






def faraway(graph, start, N=1000):
    visited = set()
    id = start
    while len(visited) < N:
        if id in visited:
            break
        visited.add(id)
        vectors = [graph[x].GetVector() for x in visited]
        vectors.append( g[id].GetVector() )
        centroid = graph.sim.NewCentroid( vectors )
        for far,prop,sim in g.Vertices( vector=centroid, sortby=S_SIM|S_ASC, hits=1, fields=F_ID|F_SIM, select="title", result=R_LIST ):
            print(far, prop, sim)
            pass
        id = far
    return visited



def trim(graph, visited):
    trimmed = set(visited) 
    for a in list(trimmed):
        for b in list(trimmed):
            if a == b:
                continue
            sim = g.sim.Cosine( g[a], g[b] )
            if sim > 0.3:
                print( a, b, sim )
                trimmed.remove(b)
    return trimmed



def bestroot(graph, roots, probe):
    return sorted([g.sim.Cosine(probe, graph[r].GetVector(), r) for r in roots], reverse=True)[0][1]



def enhance(graph, start, neighbors):
    A = g.OpenVertex( start )
    for term in neighbors:
        if A.Adjacent( D_OUT, term ):
            continue
        graph.Connect( A, ('reach', M_STAT|M_FWDONLY), term )
    A.Close()



def subvec( graph, v1, v2=None ):
    if type(v1) is str:
        v1 = graph[v1].GetVector()
    if type(v2) is str:
        v2 = graph[v2].GetVector()
    elif v2 is None:
        v2 = graph.sim.NewVector()
    return v1 - v2



def addvec( graph, v1, v2=None ):
    if type(v1) is str:
        v1 = graph[v1].GetVector()
    if type(v2) is str:
        v2 = graph[v2].GetVector()
    elif v2 is None:
        v2 = graph.sim.NewVector()
    return v1 + v2



        
def get_all( graph, start ):
    visited = set()
    explored = set()
    mem = graph.Memory(4)
    R = graph.OpenVertex( start )
    queue = deque([(start,0)])
    while queue:
        node, dist = queue.popleft()
        if node in explored:
            continue
        A = graph.OpenVertex( node )
        neighbors = graph.Neighborhood( A, D_OUT, filter='vset.add(next) > 0', memory=mem )
        A.Close()
        explored.add( node )
        if not neighbors:
            continue
        visited.update( neighbors )
        queue.extend( [(x,dist+1) for x in neighbors] )
        print( f"\rv={len(visited)} x={len(explored)} q={len(queue)} d={dist}          ", end="", flush=True )
    R.Close()
    return list(visited)



def find_all( graph, start ):
    return len( get_all( graph, start ) )


"""
g = Graph("ann")
MEM, Q = INIT(g)

ROOT = "root-part1.dump"
medoid = g[ROOT].Terminals()[0] # 4fa8ff21-6154-4485-b539-8a1ebf8fac00
P = [g[g.GetVertexID()].GetVector() for i in range(1000)]
V = g.Vertices( hits=len(P), sortby=S_RANDOM, condition={'type':'item'} )


ptest(MEM, Q, g, P[123])




"""







