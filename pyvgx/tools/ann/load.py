from pyvgx import *
import json
import sys
import random
import time
import struct
import base64
import threading
from math import log2, sqrt, exp, exp2
import itertools
import code



def vertex(g, J, fname):
    data = json.loads(J)
    id, title, vecpack, term = data
    vlen, alpha, vdata = vecpack
    if id == "root":
        g.CreateVertex(f"root-{fname}", type="root")
        return
    if id in g:
        raise Exception(f"{fname}:{id} already exists: {g[id]['fname']}")
    V = g.NewVertex( id, type="item" )
    V['*title'] = title
    V['fname'] = fname
    V.SetVector( struct.unpack(f"{vlen}b", base64.b64decode(vdata)), alpha=alpha, cosine_mode=True )
    V.Close()



def arcs(g, J, fname):
    data = json.loads(J)
    id, title, vecpack, term = data
    if id == "root":
        id = f"root-{fname}"
    V = g.OpenVertex( id )
    TL = g.OpenVertices( term )
    for T in TL:
        if T.id == V.id:
            raise Exception(f"loop! {V.id}")
        lsh32 = T.GetVector().LSH32()
        g.Connect( V, ("lsh32", M_LSH|M_FWDONLY, lsh32), T )
    g.CloseVertices(TL)
    V.Close()



def friends(g, id):
    result = g.Neighborhood(
        id = id,
        arc = ("lsh32", D_OUT),
        rank = "cosine( vertex.vector, next.vector )",
        sortby = S_RANK,
        fields = F_ID|F_RANK,
        result = R_DICT,
        select = "title"
    )
    return result



def travel(g, start=None):
    if start is None:
        start = g.GetVertexID() # random
    visited={}
    id = start 
    n = 0
    try:
        while True:
            n += 1
            # count
            if not id in visited:
                visited[id] = 0
            visited[id] += 1
            # query
            top32 = friends(g, id)[:32]
            # pick
            pick = sorted([(visited.get(item["id"],0),item) for item in top32], key=lambda x:x[0])[0][1]
            # result
            id = pick["id"]
            if not n % 10000:
                rank = pick["rankscore"]
                title = pick["properties"]["title"]
                toploop = sorted([(v,k) for k,v in visited.items()], reverse=True)[:3]
                print( title, id, rank, len(visited), toploop[0][0], g[toploop[0][1]]['title'] )
    except KeyboardInterrupt:
        return visited    

        
def load( g, fname, loadarcs=True ):
    f = open( fname )
    for line in f:
        vertex(g, line, fname)
    if loadarcs:
        f.seek(0)
        for line in f:
            arcs(g, line, fname)
    f.close()






def run( fname, arcs=True ):
    system.Initialize("annindex")
    system.StartHTTP(9000)
    g = Graph("ann")
    if g.order < 1000000:
        load(g, fname, arcs)
    g.Save()
    #system.RunServer()



def rundebug():
    global MEM
    global ROOT
    system.Initialize("annindex")
    system.StartHTTP(9000)
    g = Graph("ann")
    MEM = g.Memory(4)
    MEM.R1 = g.sim.rvec(128)
    ROOT = "root-part1.dump"
    print( g[ROOT].Terminals()[0] )
    return g


#g = Graph("ann")
#MEM = g.Memory(4)
#MEM.R1 = g.sim.rvec(128)
#ROOT = "root-part1.dump"
#medoid = g[ROOT].Terminals()[0]


def debugquery(g):
    MEM.R2 = 0
    MEM.VSetClear()
    medoid = g[ROOT].Terminals()[0]
    return g.Neighborhood( medoid, memory=MEM, hits=25, arc=D_OUT, filter="anncollect(R1,R2,R3)", collect=C_SCAN, sortby=S_VAL, fields=F_VAL|F_ID, result=R_LIST, recursive=1 )







def build_seed(g, sz=10000, R=32, ham=15):
    if "seedroot" in g:
        return
    # Get random nodes
    t0 = time.perf_counter()
    print( f"Getting {sz} random nodes" )
    nodes = g.Vertices( condition={'type':'item'}, sortby=S_RANDOM, hits=sz, fields=F_ADDR, result=R_SIMPLE )
    # Connect temp root to nodes
    T = g.NewVertex( "seedroot", type="temp" )
    for node in nodes:
        A = g.OpenVertex( node )
        lsh32 = A.GetVector().LSH32()
        A['lsh32'] = lsh32
        g.Connect( T, ("lsh", M_LSH|M_FWDONLY, lsh32), A )
        A.Close()
    T.Close()
    # Connect all seed nodes to their closest neighbors
    M = g.Memory(4)
    Q_exact = g.NewNeighborhoodQuery(
            arc    = ('lsh', D_OUT, M_LSH),
            memory = M,
            filter = "next.address != r2",
            rank   = "1+cosine(M.vector, next.vector)",
            sortby = S_RANK,
            fields = F_ADDR,
            result = R_SIMPLE )
    Q_ham = g.NewNeighborhoodQuery(
            arc    = ('lsh', D_OUT, M_LSH, V_LTE, (0,ham)),
            memory = M,
            filter = "next.address != r2",
            rank   = "1+cosine(M.vector, next.vector)",
            sortby = S_RANK,
            fields = F_ADDR,
            result = R_SIMPLE )
    Q_exact.id = "seedroot"
    Q_ham.id = "seedroot"
    i = 0
    j = 0
    R_max = int(3*R/2)
    cos_min = 0.3
    cos_target = 0.75
    for node in nodes:
        i += 1
        A = g.OpenVertex( node )
        # Find true nearest neigbors
        probe = A.GetVector()
        M.Reset()
        M.vector = probe
        M.R2 = A.address # Exclude self
        if i < 10000:
            nearest = Q_exact.Execute( hits=2000 ) 
        else:
            Q_ham.arclsh = (A['lsh32'], ham)
            nearest = Q_ham.Execute( hits=2000 ) 
        # How close is the best neighbor? Boost degree if best neighbor is not good
        B = g.OpenVertex( nearest[0] )
        cos_top = g.sim.Cosine( probe, B.GetVector() )
        B.Close()
        R_node = int(R + (cos_target-cos_top)/(cos_target-cos_min) * (R_max-R))
        R_node = R if R_node < R else R_max if R_node > R_max else R_node
        for near in nearest[:R_node]:
            B = g.OpenVertex( near )
            assert A.address != B.address
            cosine = g.sim.Cosine( probe, B.GetVector() )
            j += 1
            g.Connect( A, ('cos', M_FLT|M_FWDONLY, cosine), B )
            B.Close()
        A.Close()
        print( f"\r{i}  ", end="", flush=True )
    print()
    print( f"Collecting remote nodes" )
    RN = int((sz // 10) ** 0.5)
    anchors = random.sample( nodes, RN )
    remotes_set = set()
    r = 0
    for anchor in anchors:
        r += 1
        vector = g[anchor].GetVector()
        remotes = g.Vertices( vector=vector, condition={'type':'item', 'filter':"vertex.deg == 0 && cosine(vertex.vector, vector) < -0.3" }, sortby=S_RANDOM, hits=RN, fields=F_ADDR, result=R_SIMPLE )
        remotes_set.update( remotes )
        print( f"\r{r}  ", end="", flush=True )
    print()
    remotes = list(remotes_set)
    print( f"Connecting {len(remotes)} remotes to remotes for build navigation" )
    rc = 0
    for i in range(len(remotes)-1):
        A = g.OpenVertex(remotes[i])
        cutoff = 0.0
        for j in range(i+1,len(remotes)):
            B = g.OpenVertex(remotes[j])
            assert A.address != B.address
            cosine = g.sim.Cosine(A,B)
            if cosine > cutoff:
                cutoff = cosine
                g.Connect( A, ('cos', M_FLT|M_FWDONLY, cosine), B )
                g.Connect( B, ('cos', M_FLT|M_FWDONLY, cosine), A )
                rc += 1
            odeg = B.odeg
            B.Close()
            if odeg >= R//16:
                break
        A.Close()
        print( f"\r{i+1}/{len(remotes)-1}  {rc}", end="", flush=True )
    print()
    print( f"Connecting {len(remotes)} remotes to main manifold for build navigation" )
    QX = g.NewNeighborhoodQuery(
            arc    = ('lsh', D_OUT),
            memory = M,
            filter = "next.address != r2",
            rank   = "1+cosine(M.vector, next.vector)",
            sortby = S_RANK,
            fields = F_ADDR,
            result = R_SIMPLE )
    QX.id = "seedroot"
    i = 0
    rc = 0
    for remote in remotes:
        i += 1
        M.Reset()
        A = g.OpenVertex(remote)
        M.vector = A.GetVector()
        M.R2 = A.address # Exclude self
        inners = QX.Execute(hits=R//16)
        for inner in inners:
            B = g.OpenVertex(inner)
            assert A.address != B.address
            cosine = g.sim.Cosine(A,B)
            g.Connect( A, ('cos', M_FLT|M_FWDONLY, cosine), B )
            g.Connect( B, ('cos', M_FLT|M_FWDONLY, cosine), A )
            rc += 1
            B.Close()
        A.Close()
        print( f"\r{i}/{len(remotes)}  {rc}", end="", flush=True )
    print()
    # Connect temp root to remotes
    T = g.OpenVertex( "seedroot" )
    for remote in remotes:
        A = g.OpenVertex( remote )
        lsh32 = A.GetVector().LSH32()
        A['lsh32'] = lsh32
        g.Connect( T, ("lsh", M_LSH|M_FWDONLY, lsh32), A )
        A.Close()
    T.Close()
    t1 = time.perf_counter()
    print( f"Seed graph o={g.order} s={g.size} (R={g.size/g.order:.1f}) in {t1-t0:.1f} sec" )






def destroy_seed(g):
    g.CloseAll()
    if 'seedroot' in g:
        for node in g.Terminals( "seedroot" ):
            g.Disconnect( node, arc=('nav', D_OUT) )
        g.DeleteVertex( "seedroot" )
    

        


def sample_medoid(g, sz=50000, degree=32):
    sample = []
    sample_degree = degree
    while len(sample) < sz and sample_degree > 4:
        sample_degree -= 4
        sample = g.Vertices( condition={ 'type':'item', 'outdegree':(V_GTE,sample_degree) }, sortby=S_RANDOM, hits=sz )
    C = g.sim.NewCentroid( [g[x].GetVector() for x in sample ] )
    return g.Vertices( condition={'outdegree':(V_GTE,sample_degree)}, vector=C, hits=1, sortby=S_SIM )[0]


M_RNG = None
Q_RNG = None
M_FIND = None
Q_FIND = None
Q_NEAR = None
Q_IMMED = None


n_Q_NEAR = 0
n_Q_IMMED = 0
n_Q_RNG = 0
n_Q_RNG_skip = 0
n_Q_RNG_ham = 0
n_Q_RNG_wrong = 0
n_Q_FIND = 0
n_PRUNE = 0

t0_START = 0.0
t_FIND = 0.0
t_NEAR = 0.0
t_IMMED = 0.0
t_RNG = 0.0
t_CONNECT = 0.0
t_PRUNE = 0.0


def QMINIT(g, shadow=10, bw=64, bc=1.0, depth=1<<30, adaptive=False):
    M_RNG = g.Memory(4)
    Q_RNG = g.NewAdjacencyQuery(
        arc = D_OUT,
        memory = M_RNG,
        filter = "cosine(M.vector, next.vector) > (next.arc.value * r1)"
    )
    M_FIND = g.Memory(32)
    Q_FIND = g.NewNeighborhoodQuery(
                memory  =   M_FIND,
                arc     =   D_OUT,
                sortby  =   S_RVAL,
                fields  =   F_VAL | F_ADDR,
                result  =   R_LIST,
                recursion = {
                    'heap_size'         : 1, # <- prevents large auto heap (becomes hits instead)
                    'shadow_size'       : shadow,
                    'frontier_limit'    : 0,
                    'depth_limit'       : depth,
                    'init_select'       : 0,
                    'beam_width'        : bw,
                    'beam_curve'        : bc,
                    'beam_min'          : 8,
                    'beam_max'          : 256,
                    'alpha'             : -1.0,
                    'adaptive_taper'    : adaptive,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    Q_NEAR = g.NewNeighborhoodQuery(
                memory  =   M_FIND,
                arc     =   D_OUT,
                sortby  =   S_RVAL,
                fields  =   F_VAL | F_ADDR, # <- val=score (cos+1 from ann algo)
                result  =   R_LIST,
                recursion = {
                    'heap_size'         : 1,
                    'depth_limit'       : 3, # <-- nearby surroundings
                    'shadow_size'       : 128,
                    'beam_width'        : 128,
                    'adaptive_taper'    : False,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    Q_IMMED = g.NewNeighborhoodQuery(
                memory  =   M_FIND,
                arc     =   D_OUT,
                sortby  =   S_RVAL,
                fields  =   F_VAL | F_ADDR, # <- val=score (cos+1 from ann algo)
                result  =   R_LIST,
                recursion = {
                    'heap_size'         : 1,
                    'depth_limit'       : 1, # <-- immediate neighborhood
                    'shadow_size'       : 128,
                    'beam_width'        : 128,
                    'adaptive_taper'    : False,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    return M_RNG, Q_RNG, M_FIND, Q_FIND, Q_NEAR, Q_IMMED




def get_hubs(g, min_ideg=200, max_hubs=15000 ):
    hubs = g.Vertices(
            condition = {
                'type':'item',
                'indegree':(V_GTE,min_ideg),
                'outdegree':(V_GTE,32)
            },
            hits   = max_hubs,
            sortby = S_IDEG,
            fields = F_ADDR,
            result = R_SIMPLE
          )
    return hubs



def neighbor_coherence(g, node):
    max_neighbor = g.Neighborhood( node, fields=F_VAL, result=R_SIMPLE, sortby=S_VAL )[0]
    vectors = [g[term].GetVector() for term in g.Neighborhood( node, fields=F_ADDR, result=R_SIMPLE )]
    d = len(vectors)
    if d < 2:
        return (1.0, max_neighbor)
    csum = 0.0
    for i in range(d-1):
        for j in range(i+1, d):
            csum += g.sim.Cosine( vectors[i], vectors[j] )
    coherence = (2/(d*(d-1))) * csum
    return (coherence, max_neighbor)




def update_node_stats(g, all_nodes=None):
    if all_nodes is None:
        all_nodes = g.VerticesType('item')
    print( f"Computing node coherence for {len(all_nodes)} nodes" )
    n = 0
    coherence_list = []
    for node in all_nodes:
        n += 1
        coherence, max_neighbor = neighbor_coherence(g, node)
        coherence_list.append( coherence )
        A = g.OpenVertex(node)
        A.c0 = coherence
        A.c1 = max_neighbor
        A.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    KAPPA = 0.4
    coherence_list.sort( reverse=1 )
    c_max = coherence_list[0]
    c_median = coherence_list[n//2]
    c_baseline = c_median + KAPPA * (c_max - c_median)
    return c_baseline



def reset_node_coherence(g):
    for node in g.VerticesType('item'):
        A = g.OpenVertex(node)
        A.c0 = 0.0
        A.c1 = 1.0
        A.Close()
    


def get_coherence_baseline(g):
    all_nodes = g.VerticesType('item')
    n = 0
    coherence_list = []
    for node in all_nodes:
        n += 1
        A = g.OpenVertex(node)
        coherence_list.append( A.c0 )
        A.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    KAPPA = 0.4
    coherence_list.sort( reverse=1 )
    c_max = coherence_list[0]
    c_median = coherence_list[n//2]
    c_baseline = c_median + KAPPA * (c_max - c_median)
    return c_baseline




def update_node_coarse_cutoff(g):
    arcvals = g.Neighborhood( node, fields=F_VAL, result=R_SIMPLE, sortby=S_RVAL )
    n = len(arcvals)
    if n > 10:
        mean = sum(arcvals) / n
        stdev = sqrt(sum( [(x-mean)**2 for x in arcvals] ) / (n-1))
        cutoff = mean + 0.5 * stdev
    A = g.OpenVertex(node)
    A.Close()





def neighbor_diversity(g, node_addr):
    n = g[node_addr].odeg
    if n < 2:
        return 1.0, 0.0
    M = g.Memory(4)
    M.vector = g[node_addr].GetVector()
    neighbor_cos = g.Neighborhood(
        node_addr,
        memory  = M,
        collect = C_SCAN,
        filter  = "s=cosine(M.vector, next.vector); collect(s); true;",
        fields  = F_VAL,
        result  = R_SIMPLE
    )
    mean = sum( neighbor_cos ) / n
    stdev = sum( [(x-mean)**2 for x in neighbor_cos] ) / (n-1)
    return (mean, stdev)



def robust_enhance_2hop(g, min_hub_ideg=150, max_hubs=5000, robust_arcs=10, simulate=False ):
    connect_attempts = 0
    connected = 0
    sz_hop2 = []
    n = 0
    hubs = get_hubs(g, min_hub_ideg, max_hubs)
    M = g.Memory(4)
    # For all nodes in graph with high indegree (i.e. hubs) 
    for hub in hubs:
        M.Reset()
        n += 1
        H = g.OpenVertex(hub)
        M.vector = H.GetVector()
        mean_neighbor_cos, stdev_neighbor_cos = neighbor_diversity(g, hub)
        #mean_neighbor_cos = sum( neighbor_cos ) / len( neighbor_cos )
        min_cos_robust = max(0.05, mean_neighbor_cos - 0.2)
        max_cos_robust = min(0.90, mean_neighbor_cos + 0.2)
        # Find the top-n neighbors' neighbors for this hub (i.e. two hops away) 
        # Return as list of tuples (cosine, id)
        M.R1 = min_cos_robust
        M.R2 = max_cos_robust
        hop2 = g.Neighborhood(
            hub,
            memory  = M,
            hits    = robust_arcs, # default 10
            collect = C_SCAN,
            sortby  = S_RVAL|S_ASC, # Ascending sort, i.e. we want top-n least similar 2-hop neighbors
            neighbor={
                'collect': C_SCAN,
                'traverse': {
                    'arc': D_OUT,
                    'filter': f"""
                        require( next.address != {hub} );
                        require( vset.add(next)==1 );
                        s=cosine(M.vector, next.vector);
                        require(s < r2);
                        require(s > r1);
                        collect(s)
                        """
                }
            },
            fields = F_VAL|F_ADDR,
            result = R_LIST
        )
        # Reverse-connect the least similar 2-hop neighbors back to the hub node
        for score, farnode_addr in hop2:
            connect_attempts += 1
            F = g.OpenVertex(farnode_addr)
            cos = g.sim.Cosine(H, F) 
            if simulate:
                print( f"{farnode_addr} -({cos:0.4f})-> {hub}" )
            else:
                assert F.address != H.address
                r = g.Connect( F, ('cos', M_FLT|M_FWDONLY, cos), H )
                if r > 0:
                    connected += 1
            F.Close()
        H.Close()
        print( f"{100*n/len(hubs):0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    return connect_attempts, connected



def robust_enhance_3hop(g, min_hub_ideg=200, max_hubs=500, robust_arcs=3, simulate=False ):
    connect_attempts = 0
    connected = 0
    sz_hop2 = []
    n = 0
    hubs = get_hubs(g, min_hub_ideg, max_hubs)
    M = g.Memory(4)
    # For all nodes in graph with high indegree (i.e. hubs) 
    for hub in hubs:
        M.Reset()
        n += 1
        H = g.OpenVertex(hub)
        M.vector = H.GetVector()
        mean_neighbor_cos, stdev_neighbor_cos = neighbor_diversity(g, hub)
        #mean_neighbor_cos = sum( neighbor_cos ) / len( neighbor_cos )
        min_cos_robust3 = max(0.05, mean_neighbor_cos - 0.2)
        max_cos_robust3 = min(0.90, mean_neighbor_cos + 0.2)
        # Find the top-n neighbors' neighbors' neighbors for this hub (i.e. three hops away) 
        # Return as list of tuples (cosine, id)
        M.R1 = min_cos_robust3
        M.R2 = max_cos_robust3
        hop3 = g.Neighborhood(
            hub,
            memory  = M,
            hits    = robust_arcs, # default 10
            collect = C_SCAN,
            sortby  = S_RVAL|S_ASC, # Ascending sort, i.e. we want top-n least similar 3-hop neighbors
            neighbor= { 'traverse': {
                'collect': C_SCAN,
                'arc': D_OUT,
                'filter': "require( vset.add(next)==1 )",
                'neighbor': { 'traverse': {
                    'collect': C_SCAN,
                    'arc': D_OUT,
                    'filter': f"""
                        require( next.address != {hub} );
                        require( vset.add(next)==1 );
                        s=cosine(M.vector, next.vector);
                        require(s < r2);
                        require(s > r1);
                        collect(s)
                        """
                }}
                
            }},
            fields = F_VAL|F_ADDR,
            result = R_LIST
        )
        # Reverse-connect the least similar 3-hop neighbors back to the hub node
        for score, farnode_addr in hop3:
            connect_attempts += 1
            F = g.OpenVertex(farnode_addr)
            cos = g.sim.Cosine(H, F) 
            if simulate:
                print( f"{farnode_addr} -({cos:0.4f})-> {hub}" )
            else:
                assert F.address != H.address
                r = g.Connect( F, ('cos', M_FLT|M_FWDONLY, cos), H )
                if r > 0:
                    connected += 1
            F.Close()
        H.Close()
        print( f"{100*n/len(hubs):0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    return connect_attempts, connected




def rescue_remotes(g, degree, cutoff_ideg=6, cutoff_odeg=6, process_set=None):
    MEM = g.Memory(32)
    Q = g.NewNeighborhoodQuery(
                memory  =   MEM,
                arc     =   D_OUT,
                sortby  =   S_RVAL,
                fields  =   F_VAL|F_ADDR,
                result  =   R_LIST,
                recursion = {
                    'heap_size'         : degree,
                    'shadow_size'       : 256,
                    'beam_width'        : 128,
                    'beam_min'          : 8,
                    'beam_max'          : 256,
                    'beam_curve'        : 0.95,
                    'adaptive_taper'    : True,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    low_inways = set(g.Vertices( condition={'type':'item', 'indegree':(V_LT,cutoff_ideg)}, fields=F_ADDR, result=R_SIMPLE ))
    low_outways = set(g.Vertices( condition={'type':'item', 'outdegree':(V_LT,cutoff_odeg)}, fields=F_ADDR, result=R_SIMPLE ))
    if process_set is not None:
        low_inways = low_inways.intersection( process_set )
        low_outways = low_outways.intersection( process_set )
    if len(low_inways) == 0 and len(low_outways) == 0:
        return 0
    good_odeg = g.Vertices( condition={'type':'item'}, sortby=S_ODEG, fields=F_ODEG, result=R_SIMPLE, hits=1, offset=10000 )[0]
    all_roots = get_random_roots(g, 1024, good_odeg)
    medoid = sample_medoid(g, sz=10000)
    # inways
    Nin = len(low_inways)
    if Nin > 0:
        n = 0
        c = 0
        for remote_addr in low_inways:
            n += 1
            R = g.OpenVertex( remote_addr )
            ideg_boost = R.ideg + (cutoff_ideg - R.ideg)//2
            R.SetProperty('remote', ideg_boost)
            MEM.Reset()
            MEM.vector = R.GetVector()
            MEM.VSetAdd(R)
            roots = random.sample( all_roots, 4 )
            roots.append(medoid)
            initials = []
            for root in roots:
                Q.id = root
                initials.extend( Q.Execute( hits=ideg_boost ) )
            initials.sort(reverse=1)
            for _, init in initials[:ideg_boost]:
                I = g.OpenVertex( init )
                cosine = g.sim.Cosine(R, I)
                assert I.address != R.address
                if not g.Adjacent( I, D_OUT, R ):
                    g.Connect( I, ('cos', M_FLT|M_FWDONLY, cosine), R )
                c += 1
                I.Close()
            # Ensure we can also escape the remote node
            # in case not already included in low_outways already
            if R.odeg < cutoff_odeg:
                low_outways.add( remote_addr )
            R.Close()
            print( f"I: {n}/{Nin} {100*n/Nin:0.2f}% {c}    ", end="\r", flush=1 )
        print( f"I: {n}/{Nin} {100*n/Nin:0.2f}% {c}    " )
    # outways
    Nout = len(low_outways)
    if Nout > 0:
        n = 0
        c = 0
        for remote_addr in low_outways:
            n += 1
            R = g.OpenVertex( remote_addr )
            odeg_boost = R.odeg + (cutoff_odeg - R.odeg)//2
            R.SetProperty('remote', odeg_boost)
            MEM.Reset()
            MEM.vector = R.GetVector()
            MEM.VSetAdd(R)
            roots = random.sample( all_roots, 4 )
            roots.append(medoid)
            terminals = []
            for root in roots:
                Q.id = root
                terminals.extend( Q.Execute( hits=odeg_boost ) )
            terminals.sort(reverse=1)
            for _, term in terminals[:odeg_boost]:
                T = g.OpenVertex( term )
                cosine = g.sim.Cosine(R, T)
                assert T.address != R.address
                if not g.Adjacent( R, D_OUT, T ):
                    g.Connect( R, ('cos', M_FLT|M_FWDONLY, cosine), T )
                c += 1
                T.Close()
            R.Close()
            print( f"O: {n}/{Nout} {100*n/Nout:0.2f}% {c}    ", end="\r", flush=1 )
        print( f"O: {n}/{Nout} {100*n/Nout:0.2f}% {c}    " )
    return Nin + Nout



def inject_white_noise(g, N=1, simcut=0.25):
    for n in range(N):
        print( f"round {n}" )
        initials = g.Vertices( condition={'type':'item'}, sortby=S_RANDOM, fields=F_ADDR, result=R_SIMPLE )
        terminals = g.Vertices( condition={'type':'item'}, sortby=S_RANDOM, fields=F_ADDR, result=R_SIMPLE )
        assert len(initials) == len(terminals)
        c_out = 0
        c_in = 0
        for i in range(len(initials)):
            A = g.OpenVertex( initials[i] )
            B = g.OpenVertex( terminals[i] )
            sim = g.sim.Cosine( A, B )
            if sim > simcut:
                if not g.Adjacent( A, D_OUT, B ):
                    g.Connect( A, ('noise', M_INT|M_FWDONLY, 9999), B )
                    c_out += 1
                if not g.Adjacent( B, D_OUT, A ):
                    g.Connect( B, ('noise', M_INT|M_FWDONLY, 9999), A )
                    c_in += 1
            A.Close()
            B.Close()
        print( f"New Out:{c_out} New In:{c_in}" )
 

def erase_white_noise(g):
    all_nodes = g.Vertices( condition={'type':'item'}, fields=F_ADDR, result=R_SIMPLE )
    for node in all_nodes:
        A = g.OpenVertex( node )
        noise = g.Neighborhood( A, arc=('noise', D_OUT), fields=F_ADDR, result=R_SIMPLE )
        T = g.OpenVertices( noise )
        for term in T:
            g.Disconnect( A, D_OUT, term )
        g.CloseVertices( T )
        A.Close()    



def inject_quality(g, N=4):
    MEM = g.Memory(32)
    Q = g.NewNeighborhoodQuery(
            memory  =   MEM,
            arc     =   D_OUT,
            sortby  =   S_RVAL,
            fields  =   F_ADDR,
            result  =   R_SIMPLE,
            recursion = {
                'bias': 0
            }
    )
    roots = g.Vertices( condition={'type':'item'}, sortby=S_RANDOM, fields=F_ADDR, result=R_SIMPLE, hits=4096 )
    n = 0
    for node in g.Vertices( condition={'type':'item'}, sortby=S_RANDOM, fields=F_ADDR, result=R_SIMPLE ):
        n += 1
        A = g.OpenVertex(node)
        MEM.Reset()
        MEM.vector = A.GetVector()
        S = set()
        for root in random.sample( roots, 2 ):
            Q.id = root
            neighbors = Q.Execute( hits=500 )
            x = N//2
            S.update( neighbors[:x] ) # keep top best
            S.update( random.sample( neighbors[x:], x ) ) # random long tail
        neighbors = list(S)
        T = g.OpenVertices( neighbors )
        for B in T:
            g.Connect( A, ('rank', M_INT|M_FWDONLY, 100), B )
            g.Connect( B, ('rank', M_INT|M_FWDONLY, 100), A )
        g.CloseVertices(T)
        A.Close()
        if not n % 555:
            print( f"{100*n/g.order:0.2f}%", end="\r", flush=1 )
    print( "100.00%" )



def hard_prune(g, degree, slack):
    n = 0
    for node in g.Vertices( condition={'type':'item'}, fields=F_ADDR, result=R_SIMPLE ):
        n += 1
        A = g.OpenVertex(node)
        neighbors = A.Neighborhood( vector=A.GetVector(), sortby=S_SIM, fields=F_ADDR, result=R_SIMPLE )
        keep = set(neighbors[:degree])
        remain = neighbors[degree:]
        keep.update( random.sample( remain, slack if slack < len(remain) else len(remain) ) )
        T = g.OpenVertices( list(keep) )
        g.Disconnect( A, D_OUT )
        i = 0
        for B in T:
            i += 1
            g.Connect( A, ('rank', M_INT|M_FWDONLY, i), B )
        g.CloseVertices(T)
        A.Close()
        if not n % 555:
            print( f"{100*n/g.order:0.2f}%", end="\r", flush=1 )
    print( "100.00%" )





 


#M_FIND.VSetClear()
#g.Evaluate( "vset.add(vertex)", memory=M_FIND, tail=C )
#g.Neighborhood( C.id, pre="vset.len()==1", hits=3, memory=M_FIND, arc=D_OUT, sortby=S_RVAL, fields=F_ADDR, result=R_SIMPLE, recursion={'depth_limit':3, 'shadow_size':64, 'beam_width':64, 'adaptive_taper':False, 'reset_map':False, 'reset_metrics':False } )




def prune_RNG_neighborhood(g, C, degree, alpha, max_odeg_ratio=1.5, recursion=1):
    global n_Q_NEAR
    global t_NEAR
    #
    R = degree
    in_pressure = C.GetProperty('in_pressure', 0)
    s = max(0, min(1, (in_pressure - 2) / 16))
    k = 0.75
    newR = R + round(sqrt(max(0, in_pressure-2)))
    boostR = newR + round(s * k * newR)
    tightalpha = alpha - 0.02 * s
    if newR > R:
        C.SetProperty('R', newR)
    #
    t0 = time.perf_counter()
    M_FIND.Reset()
    M_FIND.vector = C.GetVector()
    Q_NEAR.id = C.id
    scored_neighbors = Q_NEAR.Execute( hits=5*boostR )
    t_NEAR += time.perf_counter() - t0
    n_Q_NEAR += 1
    #for v,a in scored_neighbors:
    #    if a == C.address:
    #        raise Exception( f"BUG!  addr={a}" )
    #g.Disconnect( C, D_OUT )
    connect_RNG_candidates(g, C, scored_neighbors, degree=boostR, alpha=tightalpha, max_odeg_ratio=max_odeg_ratio, recursion=recursion)
    


def connect_RNG_candidates(g, A, candidate_addr_list, degree, alpha, max_odeg_ratio=1.5, recursion=1):
    global n_Q_RNG
    global n_Q_RNG_skip
    global n_Q_RNG_ham
    global n_Q_RNG_wrong
    global n_PRUNE
    global t_RNG
    global t_CONNECT
    t0 = time.perf_counter()
    R = degree
    ODEG_MIN_CUTOFF = int(max_odeg_ratio * R)
    candidates = [((score-1.0), g[candidate_addr].GetVector(), candidate_addr) for score, candidate_addr in candidate_addr_list]
    accepted = []
    #V_q = A.GetVector()
    #Ham = g.sim.HammingDistance
    Cos = g.sim.Cosine
    #maxham_floor = round((24 / (alpha**2)))
    #maxham_delta = round((12 / (alpha**2)))
    delta = 0.1 / (R-1) # start at alpha, end at alpha-0.1
    for c in candidates:
        reject = False
        cos_cq, V_c, _ = c
        axcos_cq = alpha - delta * len(accepted)
        #maxham = min( maxham_floor, Ham( V_q, V_c ) + maxham_delta )
        for _, V_n, _ in accepted:
            #if Ham( V_c, V_n ) > maxham:
            #    n_Q_RNG_ham += 1
            #    if Cos( V_c, V_n ) > cos_cq: # should not be true!
            #        n_Q_RNG_wrong += 1
            #    continue
            n_Q_RNG += 1
            if Cos( V_c, V_n ) > cos_cq:
                reject = True
                break
        if reject:
            n_Q_RNG_skip += 1
            continue
        # accept
        accepted.append( c )
        if len(accepted) >= R:
            break
    #if len(accepted) < R//2:
    #    frame = sys._getframe()
    #   code.interact( banner="Few accepts!", local=dict(frame.f_globals, **frame.f_locals) )
    t1 = time.perf_counter()
    t_RNG += t1 - t0
    # (Re)connect
    t_ignore = 0.0
    g.Disconnect( A, D_OUT )
    A.IncProperty('ntouched')
    for cosine, _, addr in accepted:
        B = g.OpenVertex( addr )
        assert A.address != B.address
        g.Connect( A, ('cos', M_FLT|M_FWDONLY, cosine), B )
        g.Connect( B, ('cos', M_FLT|M_FWDONLY, cosine), A )
        if B.odeg >= ODEG_MIN_CUTOFF and recursion > 0:
            # Effective cutoff is different for in-pressured node with R property (higher than base R)
            odeg_cutoff = int(max_odeg_ratio * B.GetProperty('R',R))
            if B.odeg > odeg_cutoff:
                tp0 = time.perf_counter()
                n_PRUNE += 1
                B.IncProperty('in_pressure')
                prune_RNG_neighborhood(g, B, R, alpha, max_odeg_ratio=max_odeg_ratio, recursion=recursion-1)
                t_ignore += time.perf_counter() - tp0
        B.IncProperty('ntouched')
        B.Close()
    t2 = time.perf_counter()
    t_CONNECT += (t2 - t1) - t_ignore
    return len(accepted)



def measure_candidates_recall(g, A, test_result):
    scan_result = scan(g, A.GetVector(), k=len(test_result), exclude=A.address, usecache=False )
    scanned = set([ id for id, score in scan_result ])
    searched = set([ id for score, id in test_result ])
    r = (len(scanned) - len(scanned - searched)) / len(scanned)
    return len(test_result), r



def VERY_BAD(g, A, roots, C):
    print( f"A: {A.address}" )
    print( f"roots: {roots}" )
    print( f"len(C): {len(C)}" )
    dupes = len(C) - len(set(C))
    print( f"dupes: {dupes}" )
    print( f"A in C? {A.address in C}" )
    D = set()
    for x in C:
        if x in D:
            print( f"duplicate: {x}" )
        D.add(x)
    raise Exception( "error!" )



def find_candidates(g, A, roots, target_hits, recall=False):
    global n_Q_NEAR
    global n_Q_FIND
    global t_NEAR
    global t_FIND
    M_FIND.Reset()
    M_FIND.vector = A.GetVector()
    # Collect candidates via ANN search
    #g.Evaluate( "vset.add(vertex)", memory=M_FIND, tail=A )
    M_FIND.VSetAdd( A ) # <- CRITICAL: never include target in result set
    C = []
    # Collect candidates by searching via different paths (same mem/vset, no duplicates possible)
    t0 = time.perf_counter()
    for root in roots:
        Q_FIND.id = root
        ann = Q_FIND.Execute( hits=target_hits ) # Keep the vset since reset_state is True
        n_Q_FIND += 1
        C.extend(ann)
    t_FIND += time.perf_counter() - t0
    ## Extend with everything already in the near neighborhoods of node if it has neighbors
    #if A.odeg > 0:
    #    t0 = time.perf_counter()
    #    Q_NEAR.id = A.id
    #    ann = Q_NEAR.Execute( hits=target_hits )
    #    n_Q_NEAR += 1
    #    C.extend(ann)
    #    t_NEAR += time.perf_counter() - t0
    # Sort by score, highest to lowest
    if len(C) != len(set(C)):
        VERY_BAD(g, A, roots, C)
    C.sort( reverse=1 )
    if recall:
        k,r = measure_candidates_recall(g, A, C[:5])
        print( f"recall@{k}={r:0.4f}" )
        k,r = measure_candidates_recall(g, A, C[:25])
        print( f"recall@{k}={r:0.4f}" )
        k,r = measure_candidates_recall(g, A, C[:target_hits])
        print( f"recall@{k}={r:0.4f}" )
        k,r = measure_candidates_recall(g, A, C)
        print( f"recall@{k}={r:0.4f}" )
    return C



def process_node(g, A, entry, degree, alpha, max_odeg_ratio, random_roots, nrandom_roots=2):
    probe = A.GetVector()
    selected_roots = []
    if entry is not None:
        selected_roots.append(entry)
    if nrandom_roots > 0:
        selected_roots.extend( random.sample( random_roots, nrandom_roots ) )
    if A.odeg > 0 and A.address not in selected_roots:
        selected_roots.append( A.address )
    C = find_candidates(g, A, selected_roots, degree)
    connect_RNG_candidates(g, A, C, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=1)


    
def prune_all(g, degree, alpha, prunable_mindegree=-1, max_odeg_ratio=1.5):
    if prunable_mindegree < 0:
        prunable_mindegree = degree
    prunable = g.Vertices( condition={'type':'item', 'outdegree':(V_GTE,prunable_mindegree)}, fields=F_ADDR, result=R_SIMPLE )
    N = len(prunable)
    n = 0
    if N == 0: 
        return
    for node in sorted(prunable):
        n += 1
        A = g.OpenVertex(node)
        prune_RNG_neighborhood(g, A, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=0)
        A.Close()
        print( f"{n}/{N} {100*n/N:0.2f}%", end="\r", flush=1 )
    print( f"{n}/{N} {100*n/N:0.2f}%    " )
        


def get_random_roots(g, n, degree):
    # Pick n random item nodes with high outdegree
    m = g.Memory(4)
    m.R1 = 2*degree # starting min out degree
    nodes = []
    while len(nodes) < n and m.R1 > 4:
        m.R1 -= 4
        nodes = g.Vertices( memory=m, condition={ 'filter':"vertex.type == 'item' && vertex.odeg >= r1" }, sortby=S_RANDOM, hits=n )
    return nodes



def get_entry_nodes(g, maxcand=1500000, starsize=500, max_mutual_cos=0.38, min_odeg=32 ):
    # Get neighbor diversity (stdev of neighbors' similarity to node)
    C = [ (neighbor_diversity(g,c)[1], c) for c in g.Vertices( condition={'type':'item', 'outdegree':(V_GTE,min_odeg)}, sortby=S_RANDOM, hits=maxcand, fields=F_ADDR, result=R_SIMPLE ) ]
    C.sort( reverse=1 ) # sort by stdev so the best candidates are likely to make it into star
    # Now build up the diverse star from the collected candidates
    S = set()
    for _, cand in C:
        # Compare candidate with all others accepted into star
        accepted = True # bold assumption
        for node in S:
            if g.sim.Cosine( g[cand], g[node] ) > max_mutual_cos:
                accepted = False
                break
        # Canidate too similar to others in star, skip
        if not accepted:
            continue
        # We made it
        S.add( cand )
        if len(S) >= starsize:
            break
    return S





def get_node_nav_efficiency(g, node, shw=96, sample_size=1024, orth=0.0):
    U = g.OpenVertex(node)
    M = g.Memory(4)
    M.R2 = orth
    M.vector = U.GetVector()
    # Random terminals exactly orthogonal to our node
    orthogonals = g.Vertices( 
                    memory=M,
                    condition = {
                        'type'   : 'item',
                        'filter' : """
                                   store(R1,cosine(M.vector, vertex.vector)) in range(r2-0.002, r2+0.002)
                                   """
                    },
                    rank = "1+r1",
                    sortby = S_RANDOM,
                    hits = sample_size,
                    fields = F_ID,
                    result = R_SIMPLE,
                    select = "title" )
    # Search query setup
    Q = g.NewNeighborhoodQuery(
        memory  = M,
        arc     = D_OUT,
        sortby  = S_RVAL,
        fields  = F_VAL,
        result  = R_SIMPLE,
        recursion = {
            'shadow_size'    : shw,
            'beam_width'     : 3,
            'beam_min'       : 3,
            'beam_max'       : 256,
            'adaptive_taper' : True
        }
    )
    Q.id = node
    bests = []
    evals = []
    ranked = []
    # Measure evals require to get from node to all orthogonal terminals (or close to)
    for t in orthogonals:
        M.Reset()
        T = g.OpenVertex(t)
        M.vector = T.GetVector()
        result = Q.Execute( hits=1 )
        if result:
            cosine = result[0] - 1
        else:
            cosine = -1.0
        base = g.sim.Cosine( U, T )
        delta = cosine - base
        ev = M.counters[0]
        evals.append(ev)
        bests.append(delta)
        T.Close()
    avg_evals = sum( evals ) / len( evals )
    avg_score = sum( bests ) / len( bests )
    efficiency = 10000 * avg_score / avg_evals
    print( f"{efficiency:0.4f} evals:{avg_evals:0.0f} score:{avg_score:0.4f}" )
    U.Close()
    return efficiency, node









def topstar(g, degree, starsize=128, best_nav_efficiency=1.0, shw=96, sample_size=1024, orth=0.0, nrandoms=0 ):
    topname = 'entry'
    g.CloseAll()
    g.ClearGraphReadonly()
    print( f"Collecting candidates" )
    E = set()
    cutoff = 0.28
    max_cutoff = 0.371
    while cutoff <= max_cutoff:
        E_cut = get_entry_nodes(g, starsize=starsize, max_mutual_cos=cutoff, min_odeg=degree)
        print( f"{len(E_cut)} (cos={cutoff:0.2f})" )
        E.update( E_cut )
        cutoff += 0.01
    print( f"{len(E)} union" )
    #print( f"Finding half-close neighbors of candidates" )
    #M = g.Memory(4)
    #C = set()
    #for entry in E:
    #    B = g.OpenVertex(entry)
    #    M.vector = B.GetVector()
    #    B.Close()
    #    opposite = g.Vertices(
    #                memory=M,
    #                condition={ 'type': 'item' },
    #                hits = 1,
    #                fields = F_ADDR,
    #                result = R_SIMPLE,
    #                sortby = S_RANK|S_ASC,
    #                rank = "1+cosine(M.vector, vertex.vector)"
    #              )
    #    C.add(opposite[0])
    #    print( f"{len(C)} opposites" )
    #E.update(C)
    #print( f"{len(E)} total" )
    #coherence_baseline = get_coherence_baseline(g)
    if topname in g:
        g.DeleteVertex(topname)
    medoid = sample_medoid(g, sz=10000)
    A = g.NewVertex( topname, type="entry" )
    #A.c0 = coherence_baseline
    g.Connect( A, ('cos', M_FLT|M_FWDONLY, 0.0), medoid )
    for entry in E:
        B = g.OpenVertex(entry)
        g.Connect( A, ('cos', M_FLT|M_FWDONLY, 0.0), B )
        B.Close()
    A.Close()
    return topname



def get_timing_info():
    t_elapsed = time.perf_counter() - t0_START
    t_immed_pct = 100.0 * t_IMMED / t_elapsed
    t_near_pct = 100.0 * t_NEAR / t_elapsed
    t_find_pct = 100.0 * t_FIND / t_elapsed
    t_rng_pct = 100.0 * t_RNG / t_elapsed
    t_connect_pct = 100.0 * t_CONNECT / t_elapsed
    t_prune_pct = 100.0 * t_PRUNE / t_elapsed
    t_overhead = t_elapsed - t_IMMED - t_NEAR - t_FIND - t_RNG - t_CONNECT - t_PRUNE
    t_overhead_pct = 100.0 * t_overhead / t_elapsed
    timing_info = f"{t_elapsed:.0f}s I={t_immed_pct:.1f}% N={t_near_pct:.1f}% F={t_find_pct:.1f}% R={t_rng_pct:.1f}% C={t_connect_pct:.1f}% P={t_prune_pct:.1f}% O={t_overhead_pct:.1f}%"
    return timing_info



def populate(g, degree, alpha, max_odeg_ratio=1.5, entry=None, qshadow=-1, qbw=3, qbc=1.0, qdepth=1<<30, qadaptive=True, keepdegree=False, process_set=None, sample_population=1.0, actual_set=None, rounds=1, polish=False):
    global M_RNG
    global Q_RNG
    global M_FIND
    global Q_FIND
    global Q_NEAR
    global Q_IMMED
    global n_Q_IMMED
    global n_Q_NEAR
    global n_Q_RNG
    global n_Q_RNG_skip
    global n_Q_RNG_ham
    global n_Q_RNG_wrong
    global n_Q_FIND
    global n_PRUNE
    n_Q_IMMED = 0
    n_Q_NEAR = 0
    n_Q_RNG = 0
    n_Q_RNG_skip = 0
    n_Q_RNG_ham = 0
    n_Q_RNG_wrong = 0
    n_Q_FIND = 0
    n_PRUNE = 0
    if qshadow < 1:
        qshadow = 10*degree
    M_RNG, Q_RNG, M_FIND, Q_FIND, Q_NEAR, Q_IMMED = QMINIT(g, shadow=qshadow, bw=qbw, bc=qbc, depth=qdepth, adaptive=qadaptive)
    # Get all nodes not in the seed graph
    if process_set is None:
        full_set = set( g.Vertices( condition={'type':'item'}, fields=F_ADDR, result=R_SIMPLE ) )
        if "seedroot" in g:
            seed_set = set(g.Neighborhood("seedroot", arc=D_OUT, fields=F_ADDR, result=R_SIMPLE))
            process_set = full_set - seed_set   # exclude the seed set
        else:
            process_set = full_set
    # Process sample (0% - 100% of nodes)
    process_list = random.sample( sorted(process_set), int(sample_population * len(process_set)) )
    if actual_set is not None:
        actual_set.update( process_list )
    refresh_roots_at_n = 0
    search_roots = []
    N = len(process_list)
    rn = 0
    while rn < rounds:
        t0 = time.perf_counter()
        rn += 1
        n = 0
        for node in process_list:
            perform = True
            A = g.OpenVertex(node)
            ntouched = A.GetProperty('ntouched', 0)
            if polish:
                # Process only if not already involved in processing a few times during this phase
                perform = True if ntouched < 3 else False
            if perform:
                if keepdegree is False:
                    node_degree = degree
                else:
                    node_degree = A.odeg
                if n >= refresh_roots_at_n:
                    #medoid = sample_medoid(g, sz=100000, degree=degree)
                    random_roots = set()
                    nindexed = len( g.Vertices(condition={'type':'item', 'outdegree':(V_GT,0)}, fields=F_ADDR) )
                    # top 10%th odeg
                    rdeg = g.Vertices( condition={'type':'item', 'outdegree':(V_GT,0)}, sortby=S_ODEG, hits=1, offset=nindexed//10, fields=F_ODEG, result=R_SIMPLE )[0]
                    E = set()
                    remain_iter = 5
                    while len(random_roots) < 256 and rdeg > 4 and remain_iter > 0:
                        remain_iter -= 1
                        rdeg -= 4
                        cutoff = 0.35
                        max_cutoff = 0.421
                        while cutoff <= max_cutoff:
                            E_cut = get_entry_nodes(g, maxcand=10000, starsize=64, max_mutual_cos=cutoff, min_odeg=rdeg)
                            E.update( E_cut )
                            cutoff += 0.01
                        random_roots = E
                    refresh_roots_at_n += 50000
                    random_roots_list = list(sorted(random_roots)) 
                #if entry is None:
                #    entry = medoid if A.odeg == 0 else None # <- use medoid if A is not yet connected
                if entry is None:
                    nrandom_roots = 4 if A.odeg == 0 else 3
                else:
                    nrandom_roots = 2
                process_node(g, A, entry, node_degree, alpha, max_odeg_ratio, random_roots=random_roots_list, nrandom_roots=nrandom_roots)
            A.Close()
            n += 1
            if not n % 100:
                timing_info = get_timing_info()
                t1 = time.perf_counter()
                t = t1-t0
                nps = n // t if t > 0 else 0.0
                t_rem = (N-n) / nps if nps > 0 else 0.0
                print( f"\r{rn}/{rounds} {n}/{N} {t:.1f}s {nps}/s o={g.order} find={n_Q_FIND} near={n_Q_IMMED}/{n_Q_NEAR} rng={n_Q_RNG_ham}/{n_Q_RNG_wrong}/{n_Q_RNG}/{n_Q_RNG_skip} prune={n_PRUNE} {timing_info} (eta:{t_rem:.0f}s)                    ", end="", flush=True )
        print( f"\r{rn}/{rounds} {n}/{N} {t:.1f}s {nps}/s o={g.order} find={n_Q_FIND} near={n_Q_IMMED}/{n_Q_NEAR} rng={n_Q_RNG_ham}/{n_Q_RNG_wrong}/{n_Q_RNG}/{n_Q_RNG_skip} prune={n_PRUNE} {timing_info} (eta:{t_rem:.0f}s)                    ", flush=True )



def clear_links(g):
    destroy_seed(g)
    for node in g.Vertices():
        A = g.OpenVertex(node)
        A.c0 = 0.0 # neighborhood coherence
        A.c1 = 1.0 # max neighbor cosine
        for delprop in ['remote', 'in_pressure', 'R']:
            if A.HasProperty(delprop):
                A.RemoveProperty(delprop)
        g.Disconnect(A)
        A.Close()



def reset_phase_counters(g, degree):
    PD = {}
    TD = {}
    RD = {}
    for node in g.Vertices( condition={'type':'item'} ):
        A = g.OpenVertex(node)
        in_pressure = A.GetProperty('in_pressure', 0)
        if in_pressure > 0:
            A.SetProperty( 'in_pressure', in_pressure//2 )
        ntouched = A.GetProperty('ntouched', 0)
        if ntouched > 0:
            A.SetProperty( 'ntouched', 0 )
        newR = A.GetProperty('R', 0)
        if newR > degree:
            newR = degree + 2 # Start slightly elevated in the next phase
            A.SetProperty('R', newR) # because A saw elevated in-pressure in previous phase
        A.Close()
        # pressure
        if in_pressure not in PD:
            PD[in_pressure] = 0
        PD[in_pressure] += 1
        # touched
        if ntouched not in TD:
            TD[ntouched] = 0
        TD[ntouched] += 1
        # R
        if newR not in RD:
            RD[newR] = 0
        RD[newR] += 1
    pressure = sorted( [(p,freq) for p,freq in PD.items()], reverse=1 )
    touched = sorted( [(t,freq) for t,freq in TD.items()], reverse=1 )
    elevated = sorted( [(e,freq) for e,freq in RD.items()], reverse=1 )
    pstr = ", ".join( [f"{p}: {freq}" for p,freq in pressure] )
    tstr = ", ".join( [f"{t}: {freq}" for t,freq in touched] )
    estr = ", ".join( [f"{e}: {freq}" for e,freq in elevated] )
    print( f"In-pressure distribution: {pstr}" )
    print( f"Touched distribution: {tstr}" )
    print( f"Elevated-R distribution: {estr}" )
            
           



def finalize_arcs(g, entry, force_dedupe=False):
    Q = g.NewNeighborhoodQuery( sortby=S_VAL, fields=F_ADDR, result=R_SIMPLE )
    A = g.OpenVertex(entry)
    Q.id = A.address 
    neighbors = Q.Execute()    
    T = g.OpenVertices( neighbors )
    g.Disconnect(A, D_OUT)
    for B in T:
        assert A.address != B.address
        g.Connect(A, ('rank', M_INT|M_FWDONLY, 1), B)
    g.CloseVertices(T)
    A.Close()
    n = 0
    for node in g.Vertices( condition={'type':'item'} ):
        n += 1
        A = g.OpenVertex(node)
        Q.id = A.address
        neighbors = Q.Execute()
        if force_dedupe:
            S = set(neighbors)
            if len(S) != len(neighbors):
                #print( f"removing {len(neighbors)-len(S)} duplicates" )
                neighbors = list(set(neighbors))
        T = g.OpenVertices( neighbors )
        g.Disconnect(A, D_OUT)
        i = 0
        for B in T:
            i += 1
            assert A.address != B.address
            g.Connect(A, ('rank', M_INT|M_FWDONLY, i), B)
        g.CloseVertices(T)
        A.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )








def repair_with_lsh32(g):
    n = 0 
    for node in g.VerticesType('item'):
        n += 1
        fixed = [(term,g[term].GetVector().LSH32()) for term in g.Neighborhood( node )]
        g.Disconnect( node, D_OUT )
        for term, lsh32 in fixed:
            g.Connect( node, ('lsh32', M_LSH|M_FWDONLY, lsh32), term )
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )



def repair_with_cosarcs(g):
    n = 0 
    for node in g.VerticesType('item'):
        A = g.OpenVertex( node )
        V = A.GetVector()
        n += 1
        terminals = g.Neighborhood( A, vector=V, fields=F_VAL|F_ADDR, result=R_LIST, collect=C_SCAN, filter="c=cosine(vector, next.vector); collect(c);" )
        g.Disconnect( A, D_OUT )
        for cos, term in terminals:
            T = g.OpenVertex( term )
            g.Connect( A, ('cos', M_FLT|M_FWDONLY, cos), T )
            T.Close()
        A.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )



def check_terminal_sim(g):
    buckets = [0] * 201
    n = 0 
    for node in g.VerticesType('item'):
        A = g.OpenVertex( node )
        V = A.GetVector()
        n += 1
        sims = g.Neighborhood( A, vector=V, fields=F_VAL, result=R_SIMPLE, collect=C_SCAN, filter="c=cosine(vector, next.vector); collect(c);" )
        for cos in sims:
            b = int(round(cos, 2) * 100) + 100
            buckets[b] += 1
        A.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    return buckets
    




def diversity( g, idlist, threshold=0.4 ):
    D = []
    S = [ (g[id].GetVector(), id) for id in idlist ]
    for candidate_vector, candidate in S:
        for diverse_vector, diverse_candidate in D:
            if g.sim.Cosine( candidate_vector, diverse_vector ) > threshold:
                candidate = None
                break
        if candidate:
            D.append( (candidate_vector, candidate) )
    return [node for _,node in D]



def get_diverse_subset( g, node, sz_target=8 ):
    odeg = 55
    threshold = 0.35
    while True:
        D = diversity( g, g.Neighborhood( node, sortby=S_ODEG, neighbor={'outdegree':(V_GTE,odeg)}, hits=10*sz_target ), threshold )
        if len(D) >= sz_target:
            break
        odeg -= 1
        threshold += 0.02
        if threshold >= 1.0:
            break
    if len(D) < 8:
        D = g.Neighborhood( node, sortby=S_ODEG, hits=sz_target )
    return D




def reconnect_with_diverse_subset( g, node, sz_target=8 ):
    T = g.Terminals(node)
    D = get_diverse_subset(g, node, sz_target)
    Nset = set(T) - set(D)
    A = g.OpenVertex(node)
    g.Disconnect(A, D_OUT)
    try: 
        for diverse in D:
            B = g.OpenVertex(diverse)
            g.Connect( A, ('to', M_INT|M_FWDONLY, B.odeg), B )
            B.Close()
        for normal in Nset:
            g.Connect( A, ('to', M_INT|M_FWDONLY, 1), normal )
    except:
        for term in T:
            g.Connect( A, ('to', M_INT|M_FWDONLY, 1), term )
    finally:
        A.Close()
    
    



def repair_with_diverse_subset( g ):
    n = 0
    for node in g.VerticesType('item'):
        n += 1
        reconnect_with_diverse_subset(g, node)
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
        






        
def new_vicinity_mem_query(g):
    M = g.Memory(4)
    Q = g.NewAdjacencyQuery(
            memory   = M,
            arc      = D_OUT,
            filter   = "next.address == r1"
        )
    return M, Q 



def is_vertex_in_vicinity(g, M, Q, node, probe_node):
    M.Reset()
    Q.id = node
    M.R1 = g[probe_node].address
    if Q.Execute():
        return True
    for term in g.Terminals(node):
        if Q.Execute():
            return True
    return False




def add_tail_escape_navigation(g, rate=0.005):
    # top 99th percentile outdegree 
    cutoff_odeg = g.Vertices( condition={ 'type':'item' }, sortby=S_ODEG, hits=1, offset=g.order//100, fields=F_ODEG, result=R_SIMPLE )[0]
    # Get all high-outdegree nodes sorted by indegree
    hubs = g.Vertices(
                    condition = {'type':'item', 'outdegree':(V_GTE,cutoff_odeg) },
                    sortby    = S_IDEG|S_ASC,
                    fields    = F_ADDR,
                    result    = R_SIMPLE
                )
    # Find remote rescues with the highest combination of rescue degree and outdegree
    remotes = g.Vertices( 
                    condition = { 'type':'item', 'property':{'remote':(V_GT,0)} },
                    rank      = "vertex.property('remote') * vertex.odeg",
                    sortby    = S_RANK,
                    fields    = F_ADDR,
                    result    = R_SIMPLE
              )
    print( f"{len(hubs)} hubs, {len(remotes)} remotes" )
    # Find diverse set with low mutual similarity
    hubs_set = set(hubs)
    remotes_set = set(remotes)
    candidates_set = hubs_set.union(remotes_set)
    diverse_set = set()
    MAX_DIVERSE_SET = 2048
    MAX_DIVERSE_SIM = 0.4
    MAX_RESCUED_REMOTES = 1024
    for candidate in candidates_set:
        A = g.OpenVertex(candidate)
        keep = True
        for div in diverse_set:
            D = g.OpenVertex(div)
            sim = g.sim.Cosine(A,D)
            D.Close()
            if sim > MAX_DIVERSE_SIM:
                keep = False # too similar, not a diverse hub
                break
        if keep:
            diverse_set.add(candidate)
        A.Close()
        if len(diverse_set) >= MAX_DIVERSE_SET:
            break
    diverse_list = list(diverse_set)
    print( f"{len(diverse_list)} escape destinations" )
    # Navigation escape points random selection
    navpoints = g.Vertices( condition={'type':'item', 'filter':f'random() < {rate}'}, sortby=S_RANDOM )
    VM, VQ = new_vicinity_mem_query(g)
    MAX_NAV_LEVEL = 15
    n = 0
    for nav in navpoints:
        n += 1
        nav_level = 0
        A = g.OpenVertex(nav)
        for escape in random.sample( diverse_list, 500 ):
            # Escape node already in neighborhood, skip
            if is_vertex_in_vicinity(g, VM, VQ, nav, escape ):
                continue
            #
            B = g.OpenVertex(escape)
            escape_sim = g.sim.Cosine( A, B )
            if escape_sim > 0.1 and escape_sim < 0.3:
                nav_level += 1
                g.Connect( A, ('nav', M_UINT|M_FWDONLY, nav_level), B )
            B.Close()
            if nav_level >= MAX_NAV_LEVEL:
                break
        A.Close()
        print( f"{n} / {len(navpoints)} escape arcs ", end="\r", flush=1 )
    print( f"{n} / {len(navpoints)} escape arcs " )




def enhance_star(g, entry='entry', R=96, a=1.0, recursion=1):
    n = 0
    N = g[entry].odeg
    for node in g.Neighborhood( entry ):
        n += 1
        A = g.OpenVertex( node )
        true_neighborhood = execscan(g, A.GetVector(), k=15*R, use_address_as_id=True)
        Rx5_all = [(score,true_neighbor_addr) for score, true_neighbor_addr in true_neighborhood if true_neighbor_addr != A.address]
        #g.Disconnect( A, D_OUT )
        #connect_RNG_candidates(g, A, candidate_addr_list=Rx5_all, vulnerable_set=None, degree=R, alpha=a, max_odeg_ratio=1.5, recursion=1)
        connect_RNG_candidates(g, A, candidate_addr_list=Rx5_all, degree=R, alpha=a, max_odeg_ratio=1.5, recursion=1)
        prune_RNG_neighborhood(g, A, degree=R, alpha=a, depth=4, max_odeg_ratio=1.5, recursion=recursion)
        A.Close()
        print( f"{100*n/N:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )



def build_proximity_graph(g, degree=48, alpha=1.0, quality=1.0, skip_polish=False):
    global n_Q_IMMED
    global n_Q_NEAR
    global n_Q_RNG
    global n_Q_RNG_skip
    global n_Q_RNG_ham
    global n_Q_RNG_wrong
    global n_Q_FIND
    global n_PRUNE
    global t0_START
    global t_FIND
    global t_IMMED
    global t_NEAR
    global t_RNG
    global t_CONNECT
    global t_PRUNE
    n_Q_IMMED = 0
    n_Q_NEAR = 0
    n_Q_RNG = 0
    n_Q_RNG_skip = 0
    n_Q_RNG_ham = 0
    n_Q_RNG_wrong = 0
    n_Q_FIND = 0
    n_PRUNE = 0
    # ----
    print( f"Clearing existing graph" )
    clear_links(g)
    # ----
    t0 = time.perf_counter()
    t0_START = t0
    t_FIND = 0.0
    t_IMMED = 0.0
    t_NEAR = 0.0
    t_RNG = 0.0
    t_CONNECT = 0.0
    t_PRUNE = 0.0
    print( "=== INITIALIZE ===" )
    # 3% seed via brute force
    seedsize = g.Order("item") // 30 # 3%
    d = 4*degree
    print( f"Creating seed graph (o={seedsize}, d={d})" )
    destroy_seed(g)
    build_seed(g, sz=seedsize, R=d, ham=15)
    skeletons = 16 # just a number, no reason
    sk = 0
    # Another 7% via non-seed subsampling and crude ANN
    seed_set = set(g.Neighborhood( "seedroot", arc=D_OUT, fields=F_ADDR, result=R_SIMPLE ))
    full_set = set(g.Vertices( condition={'type':'item'}, fields=F_ADDR, result=R_SIMPLE ))
    nonseed_set = full_set - seed_set
    skeleton_set = set()
    t0_START = time.perf_counter()
    while sk < skeletons:
        sk += 1
        nR = 2.0
        d, a, s = round(3*degree), 0.8*alpha, 0.07/skeletons
        shw = round(2*quality*d)
        print( f"Adding skeleton {sk}/{skeletons} (o={int(s*len(nonseed_set))}, d={d}, a={a}, nR={nR}, s={s} shw={shw})" )
        populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=shw, qbw=d//4, qbc=0.95, qadaptive=True, process_set=nonseed_set, sample_population=s, actual_set=skeleton_set, rounds=2)
        base_set = skeleton_set.union( seed_set )
        rescue_remotes(g, degree=d, cutoff_ideg=15, cutoff_odeg=13, process_set=base_set )
    print( "Removing seed information" )
    destroy_seed(g)
    del nonseed_set
    del skeleton_set
    del seed_set
    nR = 3.0
    d, a = round(1.5*degree), 0.9*alpha
    shw = round(3*quality*d)
    print( f"Refining base graph (o={len(base_set)}, d={d}, a={a}, nR={nR} shw={shw})" )
    reset_phase_counters(g, d)
    populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=shw, qbw=d//4, qbc=0.95, qadaptive=True, process_set=base_set)
    rescue_remotes(g, degree=d, cutoff_ideg=15, cutoff_odeg=13, process_set=base_set )
    t1 = time.perf_counter()
    print( f"t={int(t1-t0)}" )
    # ----
    print( "=== ROUND 1 ===" )
    rest_set = full_set - base_set
    del base_set
    nR = 2.5
    d, a = round(1.333*degree), 1.0*alpha
    shw = round(2*quality*d)
    print( f"Adding full population to graph (o={len(rest_set)}, d={d}, a={a}, nR={nR} shw={shw})" )
    reset_phase_counters(g, d)
    populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=shw, qbw=d//2, qbc=0.95, qadaptive=True, process_set=rest_set)
    print( "Rescuing remotes" )
    rescue_remotes(g, degree=d, cutoff_ideg=13, cutoff_odeg=11 )
    t2 = time.perf_counter()
    print( f"t={int(t2-t0)}" )
    g.CloseAll() # !!!
    g.Save()     # !!!
    # ----
    if not skip_polish:
        print( "=== ROUND 2 ===" )
        print( "Creating entry point" )
        topname = topstar(g, degree=round( nR*d ))
        nR = 2.0
        d, a = round(1.1*degree), alpha
        shw = round(5*quality*d)
        print( f"Populating graph (d={d}, a={a}, nR={nR} shw={shw})" )
        reset_phase_counters(g, d)
        populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=topname, qshadow=shw, qbw=d//2, qbc=0.95, qadaptive=True, process_set=full_set, polish=True )
        rescue_remotes(g, degree=d, cutoff_ideg=13, cutoff_odeg=11 )
        t3 = time.perf_counter()
        print( f"t={int(t3-t0)}" )
        g.CloseAll() # !!!
        g.Save()     # !!!
    # ----
    print( "=== FINALIZE ===" )
    nR = 2.0
    print( f"Pruning graph (d={degree}, a={alpha}, nR={nR})" )
    #reset_phase_counters(g, degree)
    prune_all(g, degree, alpha, prunable_mindegree=degree//2, max_odeg_ratio=nR)
    rescue_remotes(g, degree=degree, cutoff_ideg=11, cutoff_odeg=9 )
    print( "Creating entry point" )
    topname = topstar(g, degree=round( nR*degree ))
    print( "Finalizing arcs" )
    finalize_arcs(g, entry=topname)
    #enhance_star(g, entry='entry', R=int(3*degree), a=0.95)
    if ROOT:
        medoid = sample_medoid(g, sz=10000, degree=degree)
        g.Connect(ROOT, ("to",M_STAT|M_FWDONLY), medoid)
    t6 = time.perf_counter()
    print( f"t={int(t6-t0)}" )
    print( "=== COMPLETE ===" )




def entry_centroid( init ):
    assert g[init].type == "entry"
    V = []
    for term in g.Neighborhood( init ):
        T = g.OpenVertex(init, 'r')
        V.append( T.GetVector() )
        T.Close()
    C = g.sim.NewCentroid( V )
    A = g.OpenVertex( init )
    A.SetVector(C)
    A.Close()





def misc():
    sum([  1 -  ((len(set(g.Neighborhood(a)))-32)/32)  for a in g['seedroot'].Terminals() ])/25000




def perform_PCA(g, target_dim=64 ):
    import numpy as np
    # Gather all vectors from graph
    print( "extracting vectors" )
    vectors = [g[node].GetVector().external for node in g.VerticesType("item")]
    # Create numpy array of vectors, shape=(n, orig_dim)
    print( "building numpy array" )
    V_orig = np.array( vectors )
    # Compute the centroid, the mean of each column in V_orig
    print( "computing centroid" )
    centroid = np.mean( V_orig, axis=0 )
    # Center all vectors
    print( "centering all vectors" )
    V_centered = V_orig - centroid
    # Compute covariance matrix from the centered vectors
    print( "computing covariance matrix" )
    M_cov = (1/(len(V_centered)-1)) * (V_centered.T @ V_centered)
    # Derive the Eigenvalues and Eigenvectors
    print( "performing eigen decomposition" )
    E, Ve = np.linalg.eigh( M_cov )
    # Sort Eigenvalue array index by Eigenvalue
    print( "sorting by eigenvalue" )
    idx_desc = E.argsort()[::-1]
    # Extract the top column vectors from Eigenvectors, this is our projection matrix
    print( "extracting eigenvectors to projection matrix" )
    Pd = Ve[:, idx_desc[:target_dim]]
    print( "done" )
    return Pd.T, centroid



def project_PCA(g, PdT, centroid):
    import numpy as np
    # Centroid vector
    C_vec = g.sim.NewVector(centroid.tolist())
    for node in g.VerticesType("item"):
        A = g.OpenVertex(node)
        # Center the original vector, as floats
        v_ctr = (A.GetVector() - C_vec).external
        # Project centered vector onto smaller space
        v_reduced = PdT @ np.asarray( v_ctr )
        # Convert to internal bytearray
        v_red = g.sim.NewVector( v_reduced.tolist() ).internal
        # Store reduced vector in vertex
        A['v_red'] = v_red
        A.Close()


def testred(g, id):
    mem = g.Memory(4)
    mem.R1 = g[id]['v_red']
    mem.R2 = g[id].GetVector()
    print("reduced")
    for x in g.Vertices( hits=10, memory=mem, rank="cos_pi8(r1,vertex.property('v_red'))", sortby=S_RANK, fields=F_ID|F_RANK, select="title" ):
        print(x)
    print("normal")
    for x in g.Vertices( hits=10, memory=mem, rank="cosine(r2,vertex.vector)", sortby=S_RANK, fields=F_ID|F_RANK, select="title" ):
        print(x)



def check(g):
    for node in g.Vertices():
        g.OpenVertex(node).Close()






#def INIT(graph, bias=0.0, h=512, shw=0, f=0, bw=256, bc=1.0, init=8, bmin=8, bmax=512, depth=(1<<31)-1, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, epsilon=0.0, zeta=1.0/18, kappa=0, lambd=0, adaptive=True ):
def INIT(graph, bias=0.0, tune={} ):
    recursion = {
        'bias': bias,
    }
    recursion.update( tune )
    MEM = graph.Memory(32)
    Q = graph.NewNeighborhoodQuery(
                                memory  =   MEM,
                                #arc     =   ('to', D_OUT, M_INT, V_GTE, 1),
                                #arc     =   ('*', D_OUT, M_INT),
                                arc     =   D_OUT,
                                #filter  =   "anncollect( 0.0 )",
                                #collect =   C_SCAN,
                                sortby  =   S_RVAL,
                                fields  =   F_VAL | F_ID | F_DEPTH,
                                result  =   R_LIST,
                                recursion = recursion
    )
    return MEM, Q


            
                
#MEM, Q = INIT(g); ptest( MEM, Q, g, PROBES100k[123], root="entry_320_036", recall=1 )

#e="entry_510_038"; threadtest( g, 1, PROBES100k[:2000], entry=e, heaps=[81], shwfactor=0, fronts=[170], beam=[100], bcs=[0.9], inits=[8], adaptive=True )

#['entry_107_031', 'entry_699_038', 'entry_100_030', 'entry_017_020', 'entry_040_818', 'entry_233_034', 'entry_320_036', 'entry_510_038', 'entry_476_038', 'entry_146_032', 'entry_688_040', 'entry_278_035', 'entry_1474_045', 'entry_414_037', 'entry_179_033']

#x=32
#while x < 5000:
#    threadtest( g, 12, PROBES100k[:36000], 'entry_510_038', heaps=[x], shwfactor=0.0, fronts=[int(x*1.5)], beam=[4+int(x/10)], bcs=[0.65], inits=[6], adaptive=True )
#    x = int(round(x*(2**0.5))) if x >= 16 else x+1

#for h in range(16,17): threadtest( g, 1, PROBES100k[:1000], 'entry', heaps=[h], shadows=[10*h], fronts=[int(10*h)], beams=[int(2*h)], bcs=[0.5], inits=[4], bmin=4, bmax=32, adaptive=True )
#threadtest( g, 1, PROBES100k[:2000], 'entry', heaps=[10], shadows=[x for n in range(4,14) for x in (2**n, 2**n+2**(n-1)) ], fronts=[0], beams=[3], bcs=[1.0], inits=[3], bmin=3, bmax=256, adaptive=True, perfonly=1 )




def search( MEM, Q, graph, probe, k, start):
    MEM.vector = probe
    Q.id = start
    return Q.Execute( hits=k )





def ptest(MEM, Q, graph, probe, k=10, root=None, recall=False, recall_only=False, recall_with_timing_and_depth=False, fname=None ):
    start = root if root is not None else graph[ROOT].Terminals()[0]
    if type(probe) is not Vector:
        raise TypeError("probe must be vector")
    t0 = time.perf_counter_ns()
    result = search( MEM, Q, graph, probe, k, start=start )
    t1 = time.perf_counter_ns()
    t_ms = (t1-t0)/1000000.0
    if not recall and not recall_only:
        for score, id, depth in result:
            print( f"{score:0.3f}  {depth:4d}  {graph[id]['title']}" )
    else:
        fname = fname if fname is not None else graph[result[0][1]]['fname']
        scan_result = scan(graph, probe, k=k, fname=fname)
        scanned = set([ id for id, score in scan_result ])
        searched = set([ id for score, id, depth in result ])
        n = 0
        if not recall_only:
            for id, score in scan_result:
                n += 1
                if id in searched:
                    print( f"{n:3d}. {score:0.3f}   {id}  {graph[id]['title']}" )
                else:
                    print( f"{n:3d}. {score:0.3f} ! {id} ({graph[id]['title'] if id in graph else '?'})" )
        r = (len(scanned) - len(scanned - searched)) / len(scanned)
        if not recall_only:
            print( f"RECALL={100*r:0.1f}" )
    if recall_only:
        if recall_with_timing_and_depth:
            depths = [depth for score, id, depth in result]
            max_topk_depth = max(depths) if depths else 0
            return r, t_ms, max_topk_depth
        else:
            return r
    else:
        print( f"{t_ms:0.5f} ms" )





SCAN_CACHE = {}

def scan(g, probe, k=10, sortdir=S_DESC, fname=None, exclude=None, usecache=True):
    if usecache:
        for kx in (100, 10, k):
            key = f"{probe.ident}_{kx}"
            result = SCAN_CACHE.get( key )
            if result is not None:
                return result[:k]
        SCAN_CACHE[key] = execscan(g, probe, k, sortdir, fname)
        return SCAN_CACHE[key]
    #####
    mem = g.Memory(4)
    mem.R1 = probe.internal if type(probe) is Vector else graph.sim.NewVector(probe).internal
    cond = "true"
    if fname:
        cond += f" && vertex.property('fname') == '{fname}'"
    if exclude:
        if type(exclude) is int:
            cond += f" && vertex.address != {exclude}"
        else:
            cond += f" && vertex.id != '{exclude_id}'"
    result = g.Vertices(
        memory = mem,
        condition = { 'type':'item', 'filter': cond },
        sortby = S_RANK|sortdir,
        rank = "1 + cos_pi8( r1, vertex.vector)",
        hits = k,
        fields = F_ID|F_RANK,
        result = R_LIST
    )
    if usecache:
        SCAN_CACHE[key] = result
    return result



def execscan(g, probe, k=10, sortdir=S_DESC, fname=None, use_address_as_id=False):
    mem = g.Memory(4)
    mem.vector = probe if type(probe) is Vector else graph.sim.NewVector(probe)
    fields = F_RANK|F_ADDR if use_address_as_id else F_ID|F_RANK
    cond = "true"
    if fname:
        cond += f" && vertex.property('fname') == '{fname}'"
    result = g.Vertices(
        memory = mem,
        condition = { 'type':'item', 'filter': cond },
        sortby = S_RANK|sortdir,
        rank = "1 + cosine( M.vector, vertex.vector)",
        hits = k,
        fields = fields,
        result = R_LIST
    )
    return result



# k=10 probes=2000
RAW_PERF_REFERENCE = """
bias recall qps evals score_contrib beam_accepts result_accepts neighbor_expands topk_depth query_depth
-100 0.00790 35245.8 331 224 20 53 1 1.00 1.00 -0.1% -0.1%
-99 0.24010 20157.9 618 37 26 37 5 4.33 4.72 -1.0% -0.5%
-98 0.30120 20364.8 676 40 27 40 6 4.84 5.41 -1.1% -0.7%
-97 0.41780 12095.9 797 53 31 53 8 5.60 6.42 -0.4% -0.6%
-96 0.44255 14956.1 823 56 32 56 9 5.75 6.65 -0.4% -0.6%
-95 0.51105 15069.8 900 67 35 67 10 6.12 7.09 -0.4% -0.5%
-94 0.52800 14234.6 919 71 36 70 10 6.21 7.20 -0.6% -0.6%
-93 0.54700 12067.5 942 75 37 74 11 6.33 7.33 -0.6% -0.6%
-92 0.59575 7848.5 1004 88 39 84 12 6.62 7.64 -2.5% -0.8%
-91 0.61240 10640.5 1028 94 40 88 12 6.73 7.77 -3.8% -1.2%
-90 0.64365 12167.9 1085 109 42 95 13 6.98 8.09 -5.6% -1.7%
-89 0.68885 9131.0 1251 101 48 92 16 6.83 7.95 -0.8% -1.6%
-88 0.68730 7316.3 1246 101 48 92 16 6.82 7.93 -0.9% -1.6%
-87 0.70860 8561.3 1301 108 50 96 17 6.89 8.04 -1.6% -1.6%
-86 0.70655 9641.8 1295 108 50 96 17 6.88 8.02 -1.6% -1.6%
-85 0.72235 9035.0 1347 115 52 98 18 6.93 8.11 -2.0% -1.6%
-84 0.72115 7000.4 1344 114 52 98 18 6.92 8.10 -1.9% -1.6%
-83 0.73605 6034.2 1389 121 53 100 19 6.97 8.17 -2.9% -1.7%
-82 0.73480 8745.0 1386 121 53 100 19 6.96 8.17 -2.7% -1.8%
-81 0.74810 7632.0 1435 127 54 101 19 7.01 8.26 -3.3% -1.9%
-80 0.74745 7712.9 1431 127 54 101 19 7.00 8.24 -3.4% -2.0%
-79 0.75725 7171.3 1478 134 56 103 20 7.04 8.32 -3.1% -2.1%
-78 0.76525 6276.1 1521 140 57 103 21 7.08 8.42 -2.8% -2.1%
-77 0.77370 7076.0 1567 146 59 104 22 7.15 8.53 -2.7% -2.1%
-76 0.77340 6703.8 1565 146 59 104 22 7.15 8.53 -2.7% -2.2%
-75 0.78150 5956.1 1603 152 60 105 22 7.20 8.61 -3.1% -2.2%
-74 0.78810 6481.4 1638 158 61 105 23 7.22 8.68 -3.2% -2.3%
-73 0.79495 5607.9 1679 164 62 105 24 7.27 8.77 -3.0% -2.3%
-72 0.80095 5981.6 1718 169 64 106 24 7.30 8.84 -2.8% -2.3%
-71 0.81165 6283.7 1791 180 66 106 26 7.37 9.00 -2.7% -2.3%
-70 0.81515 5358.6 1826 186 67 106 26 7.40 9.08 -2.2% -2.3%
-69 0.81950 5720.4 1859 191 68 107 27 7.44 9.15 -2.1% -2.3%
-68 0.82900 5972.8 1931 202 70 107 28 7.53 9.31 -2.0% -2.3%
-67 0.83305 5841.7 1962 207 71 107 29 7.56 9.38 -2.1% -2.3%
-66 0.83850 5477.4 2016 217 73 107 30 7.59 9.49 -1.6% -2.3%
-65 0.84470 4937.1 2076 226 75 107 31 7.66 9.62 -1.9% -2.3%
-64 0.84855 5378.2 2107 231 76 107 31 7.70 9.70 -2.1% -2.3%
-63 0.85380 5323.4 2167 241 78 108 33 7.78 9.85 -1.7% -2.2%
-62 0.86025 4944.6 2227 250 80 108 34 7.83 9.97 -1.9% -2.2%
-61 0.86590 4459.4 2311 264 82 108 35 7.90 10.13 -0.8% -2.2%
-60 0.87035 4671.8 2360 272 84 108 36 7.94 10.23 -1.3% -2.1%
-59 0.87560 4468.0 2414 281 86 108 37 7.96 10.33 -2.2% -2.1%
-58 0.89525 3962.3 2700 306 104 109 42 7.23 9.63 -2.1% -2.1%
-57 0.89860 3996.0 2762 315 105 109 43 7.27 9.72 -2.0% -2.1%
-56 0.90290 3836.0 2849 328 108 110 45 7.27 9.83 -1.6% -2.1%
-55 0.90655 3545.8 2931 341 111 110 47 7.29 9.97 -1.0% -2.1%
-54 0.91150 3595.1 3016 354 113 110 48 7.31 10.05 -1.5% -2.0%
-53 0.91525 3471.9 3101 367 116 110 50 7.33 10.13 -1.4% -2.0%
-52 0.91840 3363.3 3184 379 119 110 51 7.35 10.22 -0.9% -2.0%
-51 0.92340 3453.6 3296 395 122 110 53 7.36 10.34 -1.0% -1.9%
-50 0.92655 3016.0 3374 407 125 110 55 7.38 10.37 -1.2% -1.9%
-49 0.93520 2952.3 3704 433 144 111 61 6.87 9.83 -0.2% -1.8%
-48 0.93850 2834.7 3793 446 147 111 63 6.86 9.88 -1.0% -1.8%
-47 0.94300 2700.0 3911 461 150 111 65 6.83 9.93 -2.7% -1.8%
-46 0.94475 2510.4 4028 477 154 111 67 6.82 10.00 -1.4% -1.8%
-45 0.94800 2705.9 4138 492 157 111 69 6.80 10.05 -2.6% -1.9%
-44 0.95145 2591.0 4249 508 162 111 72 6.81 10.09 -4.0% -1.9%
-43 0.95435 2372.7 4408 526 167 111 75 6.76 10.08 -3.5% -2.0%
-42 0.95530 2332.2 4520 541 171 111 77 6.73 10.14 -2.1% -2.0%
-41 0.95705 2411.9 4654 560 176 111 80 6.71 10.16 -1.1% -2.0%
-40 0.95920 2274.6 4768 575 179 111 82 6.70 10.20 -1.8% -2.0%
-39 0.96240 2041.9 5166 604 202 112 90 6.25 9.72 1.7% -1.8%
-38 0.96420 1999.4 5327 623 208 112 93 6.22 9.71 2.2% -1.7%
-37 0.96605 1926.2 5495 642 213 112 96 6.19 9.71 1.1% -1.6%
-36 0.96765 1866.4 5620 660 218 112 98 6.16 9.73 -0.1% -1.5%
-35 0.96840 1900.9 5751 677 223 112 101 6.15 9.80 0.6% -1.5%
-34 0.97080 1807.6 5844 693 227 112 103 6.19 9.87 -1.4% -1.5%
-33 0.97260 1791.5 5953 711 231 112 105 6.21 9.91 -2.9% -1.5%
-32 0.97325 1767.1 6064 728 234 112 108 6.19 9.96 -2.5% -1.5%
-31 0.97375 1748.1 6175 745 239 112 110 6.20 10.06 -1.7% -1.5%
-30 0.97560 1703.4 6281 762 243 112 112 6.21 10.08 -2.6% -1.6%
-29 0.97635 1674.0 6418 782 248 112 115 6.20 10.12 -1.4% -1.6%
-28 0.97765 1622.9 6538 799 253 112 117 6.21 10.16 -2.5% -1.6%
-27 0.97845 1584.7 6644 816 257 112 120 6.19 10.21 -3.1% -1.7%
-26 0.98030 1458.1 7001 843 279 112 127 5.91 9.81 -3.4% -1.7%
-25 0.98105 1471.9 7118 860 284 112 129 5.93 9.86 -3.1% -1.8%
-24 0.98205 1433.6 7229 876 288 112 132 5.92 9.90 -5.6% -1.9%
-23 0.98190 1443.3 7331 891 292 112 134 5.91 9.92 -3.6% -1.9%
-22 0.98275 1439.3 7433 906 296 112 136 5.90 9.94 -4.7% -2.0%
-21 0.98345 1388.5 7539 918 300 112 138 5.90 9.95 -5.4% -2.1%
-20 0.98340 1387.3 7641 931 304 112 140 5.89 9.96 -3.9% -2.2%
-19 0.98430 1325.6 7736 944 307 113 142 5.88 10.06 -5.8% -2.3%
-18 0.98410 1330.9 7828 955 311 113 144 5.89 10.07 -3.3% -2.3%
-17 0.98475 1341.6 7891 965 314 113 145 5.89 10.10 -4.9% -2.4%
-16 0.98505 1280.8 7972 976 317 113 147 5.87 10.14 -4.8% -2.5%
-15 0.98555 1247.9 8079 987 321 113 149 5.88 10.14 -4.8% -2.5%
-14 0.98590 1283.3 8197 999 324 113 152 5.87 10.16 -5.1% -2.6%
-13 0.98555 1285.5 8260 1007 327 113 153 5.84 10.15 -2.7% -2.6%
-12 0.98620 1282.8 8326 1015 330 113 154 5.86 10.16 -4.5% -2.6%
-11 0.98585 1255.0 8402 1024 333 113 156 5.83 10.15 -2.6% -2.6%
-10 0.98680 1246.3 8475 1032 337 113 158 5.83 10.13 -3.4% -2.7%
-9 0.98705 1234.7 8567 1040 340 113 160 5.81 10.18 -3.4% -2.7%
-8 0.98730 1192.6 8623 1046 342 113 161 5.81 10.14 -3.8% -2.7%
-7 0.98755 1212.9 8659 1051 343 113 161 5.82 10.18 -5.2% -2.8%
-6 0.98760 1216.0 8701 1056 344 113 162 5.83 10.21 -5.0% -2.8%
-5 0.98790 1176.5 8744 1059 345 113 163 5.83 10.21 -5.3% -2.9%
-4 0.98785 1206.6 8780 1064 347 113 164 5.82 10.22 -4.8% -2.9%
-3 0.98815 1193.3 8816 1067 348 113 165 5.84 10.25 -5.2% -3.0%
-2 0.98820 1187.9 8839 1068 348 113 165 5.84 10.26 -5.1% -3.0%
-1 0.98820 1177.0 8871 1070 349 113 166 5.84 10.25 -4.8% -3.1%
0 0.98850 1172.7 8894 1071 349 113 166 5.84 10.26 -5.9% -3.1%
1 0.98840 1184.7 8911 1071 350 113 167 5.84 10.26 -5.4% -3.2%
2 0.98860 1178.1 8937 1074 351 113 167 5.84 10.28 -5.8% -3.2%
3 0.98845 1176.3 8953 1074 351 113 168 5.84 10.29 -4.4% -3.2%
4 0.98840 1173.3 8987 1077 352 113 168 5.84 10.32 -4.6% -3.3%
5 0.98835 1147.5 9033 1082 353 113 169 5.84 10.36 -3.8% -3.3%
6 0.98855 1136.2 9050 1085 354 113 170 5.84 10.35 -3.5% -3.3%
7 0.98865 1159.3 9088 1090 355 113 171 5.85 10.36 -4.2% -3.3%
8 0.98885 1129.5 9134 1095 356 113 172 5.86 10.41 -4.7% -3.3%
9 0.98920 1143.3 9180 1102 357 113 173 5.86 10.42 -4.7% -3.4%
10 0.98910 1121.3 9218 1109 359 113 173 5.88 10.46 -4.0% -3.4%
11 0.98915 1112.0 9246 1116 360 113 174 5.87 10.47 -3.8% -3.4%
12 0.98890 1105.0 9283 1123 362 113 175 5.87 10.48 -2.7% -3.4%
13 0.98950 1092.3 9605 1146 383 113 182 5.68 10.23 -1.3% -3.3%
14 0.98965 1060.6 9653 1156 385 113 183 5.70 10.21 -2.1% -3.3%
15 0.99000 1058.6 9724 1168 388 113 184 5.71 10.27 -3.8% -3.3%
16 0.99000 1067.7 9796 1180 391 113 186 5.70 10.27 -3.1% -3.3%
17 0.99070 1038.7 9845 1192 394 113 187 5.69 10.33 -6.4% -3.4%
18 0.99035 1063.0 9942 1207 398 113 189 5.69 10.32 -3.3% -3.4%
19 0.99060 1015.4 10048 1224 401 113 191 5.69 10.37 -4.3% -3.4%
20 0.99035 1024.0 10099 1240 405 113 192 5.69 10.38 -1.7% -3.4%
21 0.99045 990.5 10194 1257 409 113 194 5.69 10.35 -1.3% -3.3%
22 0.99085 978.6 10269 1276 412 113 196 5.69 10.44 -1.0% -3.3%
23 0.99120 1000.4 10359 1295 416 113 198 5.71 10.49 -3.3% -3.3%
24 0.99120 979.1 10445 1317 421 113 199 5.70 10.51 -2.5% -3.3%
25 0.99210 936.2 10619 1342 428 113 203 5.69 10.50 -5.0% -3.3%
26 0.99200 915.2 10751 1366 434 113 206 5.68 10.52 -3.3% -3.3%
27 0.99220 917.9 10867 1394 439 113 209 5.68 10.60 -3.5% -3.3%
28 0.99225 936.3 10888 1416 440 113 209 5.71 10.65 -3.5% -3.3%
29 0.99220 946.2 10963 1444 445 113 211 5.74 10.79 -2.6% -3.3%
30 0.99250 954.5 11067 1474 449 113 213 5.75 10.82 -3.0% -3.3%
31 0.99265 899.1 11186 1504 455 113 215 5.77 10.90 -4.2% -3.3%
32 0.99315 897.7 11342 1541 462 113 219 5.77 10.92 -4.7% -3.3%
33 0.99295 854.9 11711 1594 488 113 227 5.59 10.66 -2.0% -3.3%
34 0.99290 866.6 11876 1634 496 113 230 5.57 10.72 -0.1% -3.3%
35 0.99380 804.1 12077 1676 505 114 234 5.60 10.74 -4.1% -3.3%
36 0.99425 826.8 12162 1717 510 113 236 5.62 10.86 -6.2% -3.3%
37 0.99380 844.2 12252 1763 517 113 238 5.63 10.95 -2.7% -3.3%
38 0.99410 816.3 12445 1813 527 113 242 5.62 10.97 -3.7% -3.3%
39 0.99430 791.5 12603 1863 536 114 245 5.65 11.09 -3.2% -3.3%
40 0.99485 789.2 12863 1922 549 114 251 5.64 11.13 -6.5% -3.4%
41 0.99455 780.4 12998 1977 557 114 254 5.64 11.20 -2.3% -3.3%
42 0.99500 781.2 13166 2035 568 114 257 5.67 11.27 -6.7% -3.4%
43 0.99530 762.3 13496 2103 584 114 264 5.63 11.29 -6.5% -3.4%
44 0.99590 687.4 13983 2199 617 114 275 5.50 11.08 -11.1% -3.6%
45 0.99575 656.1 14210 2272 630 114 280 5.51 11.17 -5.8% -3.6%
46 0.99585 638.8 14530 2353 648 114 287 5.50 11.19 -7.1% -3.7%
47 0.99640 657.3 14899 2441 669 114 295 5.49 11.22 -9.6% -3.8%
48 0.99610 655.7 15154 2531 685 114 301 5.46 11.28 -5.6% -3.8%
49 0.99635 645.7 15552 2631 706 114 309 5.46 11.27 -5.2% -3.8%
50 0.99685 620.0 15956 2742 728 114 318 5.44 11.35 -8.7% -3.9%
51 0.99695 620.0 16288 2853 747 114 326 5.46 11.40 -8.5% -4.0%
52 0.99695 587.0 16538 2960 770 114 332 5.46 11.54 -7.1% -4.0%
53 0.99735 587.4 16861 3063 792 114 339 5.48 11.60 -10.3% -4.1%
54 0.99745 541.2 17602 3249 842 114 356 5.36 11.31 -7.4% -4.2%
55 0.99780 534.7 17974 3366 868 114 365 5.37 11.46 -12.1% -4.3%
56 0.99775 527.3 18382 3483 899 114 375 5.37 11.52 -8.9% -4.4%
57 0.99790 510.8 18804 3598 932 114 385 5.37 11.60 -10.4% -4.5%
58 0.99835 497.2 19233 3720 966 114 395 5.39 11.70 -19.3% -4.8%
59 0.99835 475.2 19731 3853 1001 114 407 5.37 11.70 -17.2% -5.1%
60 0.99850 475.4 20128 3994 1036 114 417 5.38 11.70 -17.8% -5.3%
61 0.99860 445.6 21166 4271 1122 114 443 5.18 11.43 -18.2% -5.6%
62 0.99885 430.8 21625 4430 1165 114 454 5.18 11.45 -27.0% -6.1%
63 0.99885 432.7 22038 4586 1206 114 464 5.18 11.60 -25.7% -6.5%
64 0.99880 420.6 22402 4758 1248 114 474 5.18 11.58 -20.6% -6.8%
65 0.99880 410.6 22883 4945 1297 114 486 5.18 11.63 -18.8% -7.1%
66 0.99905 395.8 23380 5149 1356 114 498 5.18 11.72 -31.9% -7.7%
67 0.99905 377.5 24028 5365 1432 114 515 5.18 11.76 -30.0% -8.2%
68 0.99910 363.0 25092 5758 1538 114 543 5.01 11.51 -26.2% -8.6%
69 0.99905 344.2 25925 6016 1642 114 565 5.01 11.48 -24.5% -9.0%
70 0.99915 337.6 26510 6291 1742 114 581 5.01 11.52 -26.3% -9.4%
71 0.99930 323.8 27217 6595 1835 114 599 5.01 11.62 -32.3% -10.0%
72 0.99930 313.9 27876 6931 1922 114 615 5.01 11.62 -30.7% -10.5%
73 0.99940 306.6 28608 7312 2035 114 635 5.01 11.73 -42.4% -11.4%
74 0.99960 286.1 30048 7864 2215 115 674 4.89 11.53 -56.2% -13.2%
75 0.99950 280.3 30838 8325 2368 115 696 4.89 11.58 -43.6% -14.1%
76 0.99940 276.5 31666 8831 2553 115 719 4.88 11.59 -36.2% -14.7%
77 0.99955 259.0 32399 9383 2739 115 739 4.89 11.67 -44.1% -15.6%
78 0.99955 256.7 33217 9985 2930 115 763 4.89 11.67 -42.7% -16.4%
79 0.99955 249.9 34025 10647 3147 115 788 4.88 11.78 -41.3% -17.1%
80 0.99965 233.6 35762 11534 3422 115 837 4.78 11.62 -53.1% -18.4%
81 0.99970 224.3 36829 12348 3694 115 869 4.78 11.81 -66.2% -20.8%
82 0.99975 218.0 37732 13251 4009 115 898 4.78 11.69 -85.3% -27.5%
83 0.99975 208.2 38896 14264 4373 115 935 4.78 11.76 -84.9% -32.9%
84 0.99980 199.8 39968 15386 4774 115 969 4.78 11.79 -90.2% -40.3%
85 0.99975 188.1 41457 16649 5224 115 1020 4.77 11.76 -83.9% -43.6%
86 0.99970 179.6 43391 18133 5746 115 1081 4.67 11.78 -60.2% -44.1%
87 0.99975 172.8 44756 19666 6290 115 1131 4.67 11.76 -82.6% -46.8%
88 0.99975 163.0 46549 21361 6792 115 1194 4.67 11.86 -81.9% -49.0%
89 0.99975 156.2 48121 23232 7278 115 1254 4.67 11.80 -81.3% -51.0%
90 0.99980 149.3 49910 25319 7859 115 1322 4.67 11.91 -87.7% -54.2%
91 0.99980 139.6 52050 27763 8679 115 1405 4.59 11.78 -87.2% -56.8%
92 0.99980 132.4 54192 30377 9422 115 1493 4.59 11.72 -86.7% -59.0%
93 0.99980 125.2 56685 33306 10445 115 1594 4.59 11.74 -86.0% -60.9%
94 0.99980 118.0 58854 36572 11679 115 1696 4.59 11.53 -85.5% -62.5%
95 0.99980 107.2 62149 40364 12983 115 1837 4.51 11.64 -84.7% -63.8%
96 0.99980 98.8 64983 44495 14598 115 1977 4.51 11.47 -84.0% -65.0%
97 0.99985 91.8 68171 49191 16442 115 2145 4.53 11.54 -87.7% -66.6%
98 0.99985 84.1 71585 54481 18389 115 2338 4.51 11.67 -87.1% -68.0%
99 0.99985 79.3 75765 60431 20177 115 2569 4.51 11.75 -86.3% -69.1%
100 0.99990 72.1 81229 67204 22617 115 2869 4.46 11.90 -88.4% -70.6%
"""



PERF_REFERENCE = []


def init_perf_reference():
    low = "-100 0.0 1e5 0 0 0 0 0 0.0 0.0"
    high = "100 1.0 0.0 1000000 1000000 1000000 1000000 1000000 1e5 1e5"
    raw = RAW_PERF_REFERENCE.strip().replace("\t", " ").split("\n")[1:]
    data = [entry.split() for entry in [low] + raw + [high]]
    # recall evals
    mapping = {}
    for entry in data:
        recall = round( float(entry[1]), 5 )
        evals = int(entry[3])
        if not recall in mapping:
            mapping[recall] = evals
    pairs = list(mapping.items())
    pairs.sort()
    PERF_REFERENCE.clear()
    PERF_REFERENCE.extend( pairs )
 


init_perf_reference()


def perf_reference( recall ):
    recall = round(recall, 5) # to match rounded raw data
    for i in range(len(PERF_REFERENCE)-1):
        r0, e0 = PERF_REFERENCE[i]
        r1, e1 = PERF_REFERENCE[i+1]
        if recall < r0 or recall > r1:
            continue
        a = (e1-e0)/(r1-r0)
        b = e0
        r = recall - r0
        evals = a * r + b
        return evals
    return 0.0
        
        
        


EPQ = 0
REF_EPQ = 0




#def work(g, PROBES, entry, k, bias, h, shw, f, bw, bc, init, bmin, bmax, depth, alpha, beta, gamma, delta, epsilon, zeta, kappa, lambd, r_result, adaptive=True, show=False):
    #MEM, Q = INIT(g, bias=bias, h=h, shw=shw, f=f, bw=bw, bc=bc, init=init, bmin=bmin, bmax=bmax, depth=depth, alpha=alpha, beta=beta, gamma=gamma, delta=delta, epsilon=epsilon, zeta=zeta, kappa=kappa, lambd=lambd, adaptive=adaptive)
def work(g, PROBES, entry, k, bias, tune, r_result, show=False):
    MEM, Q = INIT(g, bias=bias, tune=tune )
    testrecall(MEM, Q, g, k, P=PROBES, entry=entry, show=show, r_result=r_result)


#def threadwork( g, N, PROBES, entry, k, bias, h, shw, f, bw, bc, init, bmin, bmax, depth, alpha, beta, gamma, delta, epsilon, zeta, kappa, lambd, adaptive=True, perfonly=False, key=None ):
def threadwork( g, N, PROBES, entry, k, bias, tune, perfonly=False, key=None ):
    global EPQ
    global REF_EPQ
    #if bw > 0:
    #    if bmax < bmin: bmax = bmin
    #    if bw < bmin: bw = bmin
    #    elif bw > bmax: bw = bmax
    T = []
    if N > 0:
        sz = len(PROBES) // N
        i = 0
        for n in range(N):
            sample = PROBES[i:i+sz]
            r_result = {}
            #args = (g, sample, entry, k, bias, h, shw, f, bw, bc, init, bmin, bmax, depth, alpha, beta, gamma, delta, epsilon, zeta, kappa, lambd, r_result, adaptive)
            args = (g, sample, entry, k, bias, tune, r_result)
            t = threading.Thread( target=work, args=args )
            T.append( (t, r_result) )
            i += sz
        t0 = time.perf_counter()
        for t,_ in T:
            t.start()
        alive = len(T)
        while alive:
            alive = sum([1 for t,_ in T if t.is_alive()])
            time.sleep(0.001)
        t1 = time.perf_counter()
        for t,_ in T:
            t.join(timeout=1.0) # just in case
    # Run in main thread
    else:
        T.append( (None,{}) )
        t0 = time.perf_counter()
        work(g, PROBES, entry, k, bias, tune, r_result=T[0][1] )
        t1 = time.perf_counter()
    total_queries = sum([r_result['count'] for _, r_result in T])
    wall_time = t1 - t0
    avg_thread_exec_time = sum([r_result['thread_exec_time'] for _, r_result in T]) / len(T)
    total_sum_latency_ms = sum([r_result['sum_latency_ms'] for _, r_result in T])
    total_sum_latency_seconds = total_sum_latency_ms / 1000.0
    avg_sum_latency_seconds = total_sum_latency_seconds / len(T)
    recall = sum([r_result['avg_recall'] for _, r_result in T]) / len(T)
    avg_latency_ms = total_sum_latency_ms / total_queries
    avg_thread_overhead_sec = avg_thread_exec_time - avg_sum_latency_seconds
    qps = total_queries / (wall_time - avg_thread_overhead_sec)
    qps_wall = total_queries / wall_time
    total_sum_max_topk_depths = sum([r_result['sum_max_topk_depths'] for _, r_result in T])
    total_sum_evals = sum([r_result['sum_evals'] for _, r_result in T])
    total_sum_contributes = sum([r_result['sum_contributes'] for _, r_result in T])
    total_sum_frontiers = sum([r_result['sum_frontiers'] for _, r_result in T])
    total_sum_accepts = sum([r_result['sum_accepts'] for _, r_result in T])
    total_sum_qdepths = sum([r_result['sum_qdepths'] for _, r_result in T])
    total_sum_nexpand = sum([r_result['sum_nexpand'] for _, r_result in T])
    tkdpq = total_sum_max_topk_depths / total_queries
    epq = total_sum_evals / total_queries
    cpq = total_sum_contributes / total_queries
    fpq = total_sum_frontiers / total_queries
    apq = total_sum_accepts / total_queries
    qdpq = total_sum_qdepths / total_queries
    xpq = total_sum_nexpand / total_queries
    evalrate = (epq / (avg_latency_ms/1000)) / 1000000 # million evals per second
    contributerate = 100*cpq/epq
    frontierrate = 100*fpq/epq
    acceptrate = 100*apq/epq
    #is_adaptive_taper = "(a)" if adaptive else ""
    total_sum_already = sum([r_result['sum_already'] for _, r_result in T ])
    total_sum_visited = sum([r_result['sum_visited'] for _, r_result in T ])
    vpq = total_sum_visited // total_queries
    hpq = total_sum_already // total_queries
    if not perfonly:
        pass
    #    config = f"e={entry} t={N} heap={h} shadow={shw} front={f} beam={bw} range=({bmin}-{bmax}) taper={bc}{is_adaptive_taper} init={init} a={alpha:0.4f} b={beta:0.4f} c={gamma:0.4f} d={delta:0.4f}"
    #    result = f"qps={qps:0.1f} recall={recall:0.4f}@{k} latency={avg_latency_ms:0.2f}ms ev={epq} con={cpq} fr={fpq} acc={apq} x={xpq} tkd={tkdpq:0.2f} qd={qdpq:0.2f} stop={hpq} visit={vpq}"
    #    print( f"{config} --> {result} qps_wall={qps_wall:0.1f}" )
    else:
        keyval = ""
        if key:
            val = eval(key)
            if type(val) is int:
                keyval = f"{val} "
            else:
                keyval = f"{val:0.4f} "
        ref_evals = perf_reference( recall )
        evals_delta = 100.0 * (epq - ref_evals) / ref_evals
        EPQ += epq
        REF_EPQ += ref_evals
        total_evals_delta = 100.0 * (EPQ - REF_EPQ) / REF_EPQ
        print( f"{keyval}{recall:0.5f} {qps:0.1f} {epq:0.2f} {cpq:0.2f} {fpq:0.2f} {apq:0.2f} {xpq:0.2f} {tkdpq:0.2f} {qdpq:0.2f} {evals_delta:0.1f}% {total_evals_delta:0.1f}%" )
    return recall, qps



def threadtest( g, N, PROBES, entry, k=10, bias=0.0, tune={}, perfonly=False, key=None ):
    global EPQ
    global REF_EPQ
    if bias < -100.0:
        EPQ = 0
        REF_EPQ = 0
        return
    r_PROBES = random.sample( PROBES, len(PROBES) )
    recall, qps = threadwork( g, N, PROBES, entry, k=k, bias=bias, tune=tune, perfonly=perfonly, key=key )




def OLD_threadtest( g, N, PROBES, entry, k=10, bias=0.0, heaps=None, shadows=None, fronts=None, beams=None, bcs=None, inits=None, bmin=8, bmax=512, depth=(1<<31)-1, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, epsilon=0.0, zeta=1.0/18, kappa=0, lambd=0, adaptive=True, perfonly=False, key=None ):
    global EPQ
    global REF_EPQ
    if shadows == [-1]:
        EPQ = 0
        REF_EPQ = 0
        return
    if heaps is None:
        heaps = [1024, 768, 512, 384, 256, 192, 128, 96, 64, 48, 32, 24]
    auto_shadows = []
    if shadows is None:
        auto_shadows = [2*h for h in heaps]
    auto_fronts = []
    auto_beams = []
    if beams is None:
        auto_beams = [max(int(0.5*f), 1) for f in fronts]
    if bcs is None:
        bcs = [0.75]
    if inits is None:
        inits = [8]
    for i in range(len(heaps)):
        h = heaps[i]
        if auto_shadows:
            shadows = auto_shadows[i:i+1]        
        for shw in shadows:
            if auto_fronts:
                fronts = auto_fronts[i:i+1]
            for f in fronts:
                if auto_beams:
                    beams = auto_beams[i:i+1]
                for bw in beams:
                    for bc in bcs:
                        for init in inits:
                            r_PROBES = random.sample( PROBES, len(PROBES) )
                            recall, qps = threadwork( g, N, PROBES, entry, k=k, bias=bias, h=h, shw=shw, f=f, bw=bw, bc=bc, init=init, bmin=bmin, bmax=bmax, depth=depth, alpha=alpha, beta=beta, gamma=gamma, delta=delta, epsilon=epsilon, zeta=zeta, kappa=kappa, lambd=lambd, adaptive=adaptive, perfonly=perfonly, key=key )



def testrecall( MEM, Q, g, k=25, N=1500, P=None, entry="entry", show=True, r_result=None ):
    t0 = time.perf_counter()
    R = []
    T = []
    T_OVER = []
    cnt = 0
    tot_max_topk_depths = 0
    tot_neval = 0
    tot_ncontribute = 0
    tot_nfrontier = 0
    tot_naccept = 0
    tot_qdepths = 0
    tot_nexpand = 0
    tot_nvisited = 0
    tot_nalready = 0
    fname = g[g[ROOT].Terminals()[0]]['fname']
    for p in P:
        cnt += 1
        r, t_ms, max_topk_depth = ptest(MEM, Q, g, p, k=k, root=entry, recall_only=1, recall_with_timing_and_depth=1, fname=fname)
        R.append(r)
        T.append(t_ms)
        neval, ncontribute, nfrontier, naccept, qdepth, nexpand, nvisited, nalready = MEM.counters
        tot_max_topk_depths += max_topk_depth
        tot_neval += neval
        tot_ncontribute += ncontribute
        tot_nfrontier += nfrontier
        tot_naccept += naccept
        tot_qdepths += qdepth
        tot_nexpand += nexpand
        tot_nvisited += nvisited
        tot_nalready += nalready
        if show:
            eval_per_query = tot_neval // cnt
            pct_contribute = 100.0*tot_ncontribute / tot_neval
            pct_frontier = 100.0*tot_nfrontier / tot_neval
            pct_accept = 100.0*tot_naccept / tot_neval
            avg_recall = sum(R) / cnt
            avg_latency = sum(T) / cnt
            print( f"{cnt}/{len(P)} {r:0.3f} {t_ms:0.2f}ms  avg:{avg_recall:0.3f} {avg_latency:0.2f}ms  {eval_per_query}e/q  {pct_contribute:0.1f}%c/e  {pct_frontier:0.1f}%f/e  {pct_accept:0.1f}%a/e    ", end="\r", flush=1 )
    if show:
        print( f"{cnt}/{len(P)} {r:0.3f} {t_ms:0.2f}ms  avg:{avg_recall:0.3f} {avg_latency:0.2f}ms  {eval_per_query}e/q  {pct_contribute:0.1f}%c/e {pct_frontier:0.1f}%f/e  {pct_accept:0.1f}%a/e    " )
    else:
        sum_latency_ms = sum(T)
        avg_latency_ms = sum_latency_ms / cnt
        avg_recall = sum(R) / cnt
        if r_result is None:
            return f"recall={avg_recall:0.3f}@{k} latency={avg_latency:0.2f}ms"
        if type(r_result) is dict:
            r_result['count'] = cnt
            r_result['avg_recall'] = avg_recall
            r_result['avg_latency_ms'] = avg_latency_ms
            r_result['sum_latency_ms'] = sum_latency_ms
            r_result['sum_max_topk_depths'] = tot_max_topk_depths
            r_result['sum_evals'] = tot_neval
            r_result['sum_contributes'] = tot_ncontribute
            r_result['sum_frontiers'] = tot_nfrontier
            r_result['sum_accepts'] = tot_naccept
            r_result['sum_qdepths'] = tot_qdepths
            r_result['sum_nexpand'] = tot_nexpand
            r_result['sum_visited'] = tot_nvisited
            r_result['sum_already'] = tot_nalready
            t1 = time.perf_counter()
            r_result['thread_exec_time'] = t1 - t0
            return cnt, avg_recall, avg_latency_ms



def out2test( output, N=1, np=-1 ):
    import re
    #e=entry t=1 heap=10 shadow=0 front=7 beam=8 range=(3-12) taper=0.5 init=3 --> qps=11358.9 recall=0.5389@10 latency=0.09ms evals=1117 (12.7M/s/t 12.7M/s) accepts=100 (9.0%) qps_wall=11074.0
    #('entry_510_038', '1536', '0', '2000', '300', '0.9')
    entry, sheap, sshadow, sfront, sbeam, sbmin, sbmax, staper, sadaptive, sinit = re.search( r"e=(\S+).+heap=(\d+)\s+shadow=(\d+)\s+front=(\d+)\s+beam=(\d+)\s+range=\((\d+)-(\d+)\)\s+taper=([0-9\.]+)(\(a\))?\s+init=(\d+).*", output ).groups()
    heap = int(sheap)
    shw =  int(sshadow)
    front = int(sfront)
    beam = int(sbeam)
    bmin = int(sbmin)
    bmax = int(sbmax)
    init = int(sinit)
    bc = float(staper)
    adaptive = True if sadaptive == "(a)" else False
    runthis = f"threadtest( g, {N}, PROBES100k[:{np}], '{entry}', heaps=[{heap}], shadows=[{shw}], fronts=[{front}], beams=[{beam}], bcs=[{bc}], inits=[{init}], bmin={bmin}, bmax={bmax}, adaptive={adaptive} )"
    return runthis



def out2perf( output ):
    import re
    #e=entry_233_034 t=16 heap=10 shadow=0 front=3 beam=0 taper=0.5(a) --> qps=101589.6 recall=0.015@10 latency=0.15ms evals=277 (1.9M/s/t 30.1M/s) accepts=41 (14.8%) qps_wall=79497.0
    st, sqps, srecall = re.search( r"\.*t=(\d+)[^q]+qps=([0-9.]+)\s+recall=([0-9.]+)", output ).groups()
    t = int(st)
    qps = int(round(float(sqps)))
    recall = round(float(srecall), 4)
    print( recall, qps, t )






if not system.IsInitialized():
    if sys.platform.startswith("win"):
        system.Initialize( r"H:/TEMP/dump/annindex", http=9000 )
    else:
        system.Initialize( "annindex", http=9000 )

if system.ServerPorts()['base'] < 0:
    system.StartHTTP( 9000 )

g = Graph("ann")


M_RNG, Q_RNG, M_FIND, Q_FIND, Q_NEAR, Q_IMMED = QMINIT(g)

    
MEM, Q = INIT(g)
 

PROBES100k = [ g.sim.NewVector(p, cosine_mode=1) for p in g['cache']['probes100k'] ]

SCAN_CACHE = g['cache']['SCAN_CACHE']


ROOT = "root-part1.dump"
medoid = sample_medoid(g, sz=10000, degree=32)
g.Connect(ROOT, ("to",M_STAT|M_FWDONLY), medoid)



#if __name__ == "__main__":
#    fname = sys.argv[1]
#    run( fname )

#MEM, Q = INIT(g, h=10, shw=600, f=0, bw=3, bc=1.0, init=3, bmin=3, bmax=256, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, adaptive=True )

#ptest( MEM, Q, g, PROBES100k[0], root='entry' )

#g.ClearGraphReadonly()
#g.Connect(ROOT, ("to",M_STAT|M_FWDONLY), medoid)
#g.SetGraphReadonly()


#threadtest( g, 1, PROBES100k[:2000], 'entry', heaps=[10], shadows=[ int(2**(x/3)) for x in range(4,48) ], fronts=[0], beams=[3], bcs=[1.0], inits=[3], bmin=3, bmax=256, depth=1000000, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, adaptive=True, perfonly=1, key="shw" )

# Idea: for high recall regime increase the width of beam and init. It seems to improve qps at high recall
#
#
#for shw, bsz in [ (y, int(3+round(log2( y-256 if y > 256 else 1 )))) for y in [ int(2**(x/3)) for x in range(4,38) ]]: threadtest( g, 1, PROBES100k[:2000], 'entry', heaps=[10], shadows=[shw], fronts=[0], beams=[bsz], bcs=[1.0], inits=[bsz], bmin=bsz, bmax=256, depth=1000000, alpha=0.0, beta=0.0, gamma=0.0, delta=1.0, adaptive=True, perfonly=1, key="shw" )

def bias2s( bias ):
    return round( exp2( 8 + 8 * bias * abs(bias) ) )


def s2bw(s):
    if s < 1: return 2
    bw = int(round( sqrt(2) * log2(s)))-5
    return bw if bw > 2 else 2


def clamp( x, lo, hi):
  return lo if x < lo else hi if x > hi else x



def sh2delta(s, s0=175):
    if s < 1: return -0.7
    w = 2
    A = 1.6
    B = 0.6
    d_min = -0.7
    d_max = 1.0
    delta = A * exp((-(log2(s) - log2(s0))**2)/w) - B
    return clamp(delta, d_min, d_max)


#
#
#
# for shw in [ int(2**(x/3)) for x in range(4,48) ]: threadtest( g, 1, PROBES100k[:2000], 'entry', heaps=[10], shadows=[shw], fronts=[0], beams=[s2bw(shw)], bcs=[1.0], inits=[3], bmin=1, bmax=10*s2bw(shw), depth=1000000, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, adaptive=True, perfonly=1, key="shw" )
#
#for shw in [1,1,2,3,4,5,6,7,8] + [ int(2**(x/5)) for x in range(16,76) ]: threadtest( g, 1, PROBES100k[:2000], 'entry', bias=0.0, heaps=[10], shadows=[shw], fronts=[0], beams=[s2bw(shw)], bcs=[0.99], inits=[s2bw(shw)], bmin=2, bmax=(4+s2bw(shw))**2, depth=100000, alpha=-0.7, beta=0.0, gamma=0.0, delta=sh2delta(shw), epsilon=0.0, zeta=1.0/18, kappa=0, lambd=0, adaptive=True, perfonly=1, key="shw" )
#ok2

#
#>>> for i in range(2): threadtest( g, 1, PROBES100k[:5000], 'entry', k=10, heaps=[1], shadows=[1], fronts=[2], beams=[0], bcs=[0.99], inits=[2], bmin=2, bmax=256, depth=100000, alpha=-0.5, beta=0.0, gamma=0.0, delta=0.5, epsilon=0.0, zeta=1.0/18, kappa=0, lambd=0, adaptive=True, perfonly=1, key="shw" )
#... 
#1 0.52774 5629.1 1917 271 271 112 32 15.31 17.36 107.5%
#1 0.52774 5703.1 1917 271 271 112 32 15.31 17.36 107.5%
#>>> 
#>>> for i in range(2): threadtest( g, 1, PROBES100k[:5000], 'entry', k=10, heaps=[1], shadows=[0], fronts=[0], beams=[0], bcs=[0.99], inits=[1], bmin=2, bmax=256, depth=100000, alpha=-0.5, beta=0.0, gamma=0.0, delta=0.5, epsilon=0.0, zeta=1.0/18, kappa=0, lambd=0, adaptive=True, perfonly=1, key="shw" )
#... 
#Segmentation fault: 11
#  D={};work(g, PROBES100k[:1], 'entry', k=10, h=1, shw=0, f=1, bw=0, bc=1.0, init=1, bmin=1, bmax=1, depth=1000, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, epsilon=0.0, zeta=1.0/18, kappa=0, lambd=0, r_result=D, adaptive=True, show=False)

 
# for v,a in g.Neighborhood( medoid, memory=M, recursion={ 'init_select':0, 'shadow_size':100000, 'beam_width':0, 'frontier_limit':1000000 }, filter="collect(r1); store(R1,r1-0.00001) ", fields=F_VAL|F_ADDR, sortby=S_RVAL, result=R_LIST, hits=25 ): print(f"{v:0.4f}",g[a]['title'])

# for bias in [-200,-100] + list(range(-100,101,1)): threadtest( g, 1, PROBES100k[:2000], 'entry', bias=bias, tune={}, perfonly=1, key="bias" )


