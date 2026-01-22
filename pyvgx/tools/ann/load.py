from pyvgx import *
import json
import sys
import random
import time
import struct
import base64
import threading
from math import log2, sqrt, exp


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
    MEM.ClearSet()
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
    Q = g.NewNeighborhoodQuery(
            arc    = ('lsh', D_OUT, M_LSH, V_LTE, (0,ham)),
            memory = M,
            filter = "next.address != r2",
            rank   = "1+cosine(M.vector, next.vector)",
            sortby = S_RANK,
            fields = F_ADDR,
            result = R_SIMPLE )
    Q.id = "seedroot"
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
        M.R2 = A.address
        Q.arclsh = (A['lsh32'], ham)
        nearest = Q.Execute( hits=2000 ) 
        # How close is the best neighbor? Boost degree if best neighbor is not good
        B = g.OpenVertex( nearest[0] )
        cos_top = g.sim.Cosine( probe, B.GetVector() )
        B.Close()
        R_node = int(R + (cos_target-cos_top)/(cos_target-cos_min) * (R_max-R))
        R_node = R if R_node < R else R_max if R_node > R_max else R_node
        for near in nearest[:R_node]:
            B = g.OpenVertex( near )
            cosine = g.sim.Cosine( probe, B.GetVector() )
            j += 1
            g.Connect( A, ("cos", M_FLT|M_FWDONLY, cosine), B )
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
            cosine = g.sim.Cosine(A,B)
            if cosine > cutoff:
                cutoff = cosine
                g.Connect( A, ("nav", M_FLT|M_FWDONLY, cosine), B )
                g.Connect( B, ("nav", M_FLT|M_FWDONLY, cosine), A )
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
        inners = QX.Execute(hits=R//16)
        for inner in inners:
            B = g.OpenVertex(inner)
            g.Connect( A, ("nav", M_FLT|M_FWDONLY, cosine), B )
            g.Connect( B, ("nav", M_FLT|M_FWDONLY, cosine), A )
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
Q_IMMED = None

n_Q_IMMED = 0
n_Q_RNG = 0
n_Q_FIND = 0
n_PRUNE = 0

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
                    'shadow_size'       : shadow,
                    'frontier_limit'    : 0,
                    'depth_limit'       : depth,
                    'init_select'       : 0,
                    'beam_width'        : bw,
                    'beam_curve'        : bc,
                    'beam_min'          : 1,
                    'beam_max'          : 120,
                    'adaptive_taper'    : adaptive,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    Q_IMMED = g.NewNeighborhoodQuery(
                memory  =   M_FIND,
                arc     =   D_OUT,
                sortby  =   S_RVAL,
                fields  =   F_VAL | F_ADDR,
                result  =   R_LIST,
                recursion = {
                    'depth_limit'       : 3, # <-- nearby surroundings
                    'shadow_size'       : 64,
                    'beam_width'        : 64,
                    'adaptive_taper'    : False,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    return M_RNG, Q_RNG, M_FIND, Q_FIND, Q_IMMED




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
    M = g.Memory(4)
    n = g[node_addr].odeg
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
                r = g.Connect( F, ('cos', M_FLT|M_FWDONLY, cos), H )
                if r > 0:
                    connected += 1
            F.Close()
        H.Close()
        print( f"{100*n/len(hubs):0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    return connect_attempts, connected




def rescue_remotes(g, degree, cutoff_ideg=6, process_set=None):
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
                    'beam_width'        : 100,
                    'beam_curve'        : 0.8,
                    'adaptive_taper'    : False,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )
    low_inways = set(g.Vertices( condition={'type':'item', 'indegree':(V_LT,cutoff_ideg)}, fields=F_ADDR, result=R_SIMPLE ))
    if process_set is not None:
        low_inways = low_inways.intersection( process_set )
    N = len(low_inways)
    if N == 0:
        return 0
    n = 0
    c = 0
    good_odeg = g.Vertices( condition={'type':'item'}, sortby=S_ODEG, fields=F_ODEG, result=R_SIMPLE, hits=1, offset=10000 )[0]
    all_roots = get_random_roots(g, 1024, good_odeg)
    medoid = sample_medoid(g, sz=10000)
    # inways
    for remote_addr in low_inways:
        n += 1
        R = g.OpenVertex( remote_addr )
        ideg_boost = R.ideg + (cutoff_ideg - R.ideg)//2
        R.SetProperty('remote', ideg_boost)
        MEM.Reset()
        MEM.vector = R.GetVector()
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
            g.Connect( I, ('cos', M_FLT|M_FWDONLY, cosine), R )
            c += 1
            I.Close()
        # Ensure we can also escape the remote node
        for _, init in initials:
            I = g.OpenVertex( init )
            cosine = g.sim.Cosine(R, I)
            g.Connect( R, ('cos', M_FLT|M_FWDONLY, cosine), I )
            I.Close()
            if R.odeg >= 2*degree:
                break
        R.Close()
        print( f"I: {n}/{N} {100*n/N:0.2f}% {c}", end="\r", flush=1 )
    print( f"{n}/{N} {100*n/N:0.2f}% {c}    " )
    return N


#M_FIND.ClearSet()                                                                                                                                                                                         196
#g.Evaluate( "vset.add(vertex)", memory=M_FIND, tail=C )
#g.Neighborhood( C.id, pre="vset.len()==1", hits=3, memory=M_FIND, arc=D_OUT, sortby=S_RVAL, fields=F_ADDR, result=R_SIMPLE, recursion={'depth_limit':3, 'shadow_size':64, 'beam_width':64, 'adaptive_taper':False, 'reset_map':False, 'reset_metrics':False } )




def prune_RNG_neighborhood(g, C, degree, alpha, max_odeg_ratio=1.5, recursion=1):
    global n_Q_IMMED
    M_FIND.Reset()
    M_FIND.vector = C.GetVector()
    g.Evaluate( "vset.add(vertex)", memory=M_FIND, tail=C )
    Q_IMMED.id = C.id
    scored_neighbors = Q_IMMED.Execute( hits=2*degree )
    n_Q_IMMED += 1
    for v,a in scored_neighbors:
        if a == C.address:
            raise Exception( f"BUG!  addr={a}" )
    g.Disconnect( C, D_OUT )
    connect_RNG_candidates(g, C, scored_neighbors, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=recursion)
    


def connect_RNG_candidates(g, A, candidate_addr_list, degree, alpha, max_odeg_ratio=1.5, recursion=1):
    global n_Q_RNG
    global n_PRUNE
    ODEG_CUTOFF = int(max_odeg_ratio * degree)
    MIN_RNG = degree // 2 # keep top 1/2 un-pruned (allow infinite alpha)
    n = 0
    #M_RNG.Reset()
    #M_RNG.R1 = alpha
    invalpha = 1.0/alpha
    candidates = [(alpha*(score-1.0), g[candidate_addr].GetVector(), candidate_addr) for score, candidate_addr in candidate_addr_list]
    neighbors = []
    q_vector = A.GetVector()
    for c in candidates:
        keep = True
        c_axcos, c_vector, c_addr = c
        if c_addr == A.address:
            raise Exception( f"Are you nuts?! c={c_addr} " )
        for n_axcos, n_vector, n_addr in neighbors:
            if g.sim.Cosine( c_vector, n_vector ) > c_axcos:
                keep = False
                break
        if not keep:
            continue
        neighbors.append( c )
        if len(neighbors) >= degree:
            break
    for n_axcos, n_vector, n_addr in neighbors:
        B = g.OpenVertex( n_addr )
        cosine = invalpha * n_axcos
        g.Connect( A, ('cos', M_FLT|M_FWDONLY, cosine), B )
        g.Connect( B, ('cos', M_FLT|M_FWDONLY, cosine), A )
        if B.odeg >= ODEG_CUTOFF and recursion > 0:
            n_PRUNE += 1
            prune_RNG_neighborhood(g, B, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=recursion-1)
        B.Close()
    return len(neighbors)



def FORGET_THIS():
    for score, candidate_addr in candidate_addr_list:
        if candidate_addr == A.address: # Fix this in the query generating candidate_addr_list
            continue
        C = g.OpenVertex(candidate_addr)
        # Pruning filter after half filled and alpha not yet peak aggressive
        if n > MIN_RNG and M_RNG.R1 > 0.7:
            Q_RNG.id = A
            M_RNG.vector = C.GetVector()
            # candidate too close to existing neighbor of, skip
            n_Q_RNG += 1
            if Q_RNG.Execute():
                C.Close()
                continue
            M_RNG.R1 -= 0.01 # experiment with incresingly aggressive alpha
        n += 1
        # candidate accepted
        cosine = score - 1
        g.Connect( A, ('cos', M_FLT|M_FWDONLY, cosine), C )
        if C.odeg >= ODEG_CUTOFF and recursion > 0:
            n_PRUNE += 1
            prune_RNG_neighborhood(g, C, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=recursion-1)
        g.Connect( C, ('cos', M_FLT|M_FWDONLY, cosine), A )
        C.Close()
        if n >= degree:
            break
    return n



def measure_candidates_recall(g, A, test_result):
    scan_result = scan(g, A.GetVector(), k=len(test_result), exclude=A.address, usecache=False )
    scanned = set([ id for id, score in scan_result ])
    searched = set([ id for score, id in test_result ])
    r = (len(scanned) - len(scanned - searched)) / len(scanned)
    return len(test_result), r



def find_candidates(g, A, roots, target_hits, recall=False):
    global n_Q_IMMED
    global n_Q_FIND
    M_FIND.Reset()
    M_FIND.vector = A.GetVector()
    # Collect candidates via ANN search
    g.Evaluate( "vset.add(vertex)", memory=M_FIND, tail=A )
    # First get everything already in the immediate neighborhoods of node if it has neighbors
    C = []
    if A.odeg >= 4:
        Q_IMMED.id = A.id
        ann = Q_IMMED.Execute( hits=target_hits )
        n_Q_IMMED += 1
        C.extend(ann)
    # Extend candidates by searching via other paths (same mem/vset, no duplicates possible)
    for root in roots:
        Q_FIND.id = root
        ann = Q_FIND.Execute( hits=target_hits ) # Keep the vset since reset_state is True
        n_Q_FIND += 1
        C.extend(ann)
    # Sort by score, highest to lowest
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
        for _, root in sorted([ (g.sim.Cosine(A,g[r]), r) for r in random_roots ], reverse=1)[:nrandom_roots]:
            selected_roots.append( root )
    C = find_candidates(g, A, selected_roots, degree)
    # Fresh start
    if A.odeg > 0:
        g.Disconnect(A, D_OUT)
    connect_RNG_candidates(g, A, C, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=1)
         


def prune_all(g, degree, alpha, prunable_mindegree=-1, max_odeg_ratio=1.5):
    reset_node_coherence(g)
    if prunable_mindegree < 0:
        prunable_mindegree = degree
    prunable = g.Vertices( condition={'type':'item', 'outdegree':(V_GTE,prunable_mindegree)}, fields=F_ADDR, result=R_SIMPLE )
    N = len(prunable)
    n = 0
    if N == 0:
        return
    coherence_baseline = update_node_stats(g, all_nodes=prunable)
    for node in sorted(prunable):
        n += 1
        A = g.OpenVertex(node)
        if A.c0 > coherence_baseline:
            prune_RNG_neighborhood(g, A, degree, alpha, max_odeg_ratio=max_odeg_ratio, recursion=1)
        elif A.c0 > coherence_baseline-0.1:
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
    coherence_baseline = get_coherence_baseline(g)
    if 'entry' in g:
        g.DeleteVertex('entry')
    medoid = sample_medoid(g, sz=10000)
    A = g.NewVertex( "entry", type="entry" )
    A.c0 = coherence_baseline
    g.Connect( A, ('cos', M_FLT|M_FWDONLY, 0.0), medoid )
    for entry in E:
        B = g.OpenVertex(entry)
        g.Connect( A, ('cos', M_FLT|M_FWDONLY, 0.0), B )
        B.Close()
    A.Close()



def populate(g, degree, alpha, max_odeg_ratio=1.5, entry=None, qshadow=-1, qbw=3, qbc=1.0, qdepth=1<<30, qadaptive=True, keepdegree=False, process_set=None, sample_population=1.0, actual_set=None, rounds=1):
    global M_RNG
    global Q_RNG
    global M_FIND
    global Q_FIND
    global Q_IMMED
    if qshadow < 1:
        qshadow = 10*degree
    M_RNG, Q_RNG, M_FIND, Q_FIND, Q_IMMED = QMINIT(g, shadow=qshadow, bw=qbw, bc=qbc, depth=qdepth, adaptive=qadaptive)
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
            A = g.OpenVertex(node)
            if keepdegree is False:
                node_degree = degree
            else:
                node_degree = A.odeg
            if n >= refresh_roots_at_n:
                medoid = sample_medoid(g, sz=100000, degree=degree)
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
            if entry is None:
                entry = medoid if A.odeg == 0 else None # <- use medoid if A is not yet connected
            process_node(g, A, entry, node_degree, alpha, max_odeg_ratio, random_roots, nrandom_roots=2)
            A.Close()
            n += 1
            if not n % 100:
                t1 = time.perf_counter()
                t = t1-t0
                nps = n // t if t > 0 else 0.0
                t_rem = (N-n) / nps if nps > 0 else 0.0
                print( f"\r{rn}/{rounds} {n}/{N} {t:.1f}s {nps}/s  o={g.order}  find={n_Q_FIND} immed={n_Q_IMMED} rng={n_Q_RNG} prune={n_PRUNE} (eta:{t_rem:.0f}s)    ", end="", flush=True )
        print( f"\r{rn}/{rounds} {n}/{N} {t:.1f}s {nps}/s  o={g.order}  find={n_Q_FIND} immed={n_Q_IMMED} rng={n_Q_RNG} prune={n_PRUNE} (eta:{t_rem:.0f}s)            ", flush=True )



def clear_links(g):
    destroy_seed(g)
    for node in g.Vertices():
        A = g.OpenVertex(node)
        A.c0 = 0.0 # neighborhood coherence
        A.c1 = 1.0 # max neighbor cosine
        for delprop in ['remote']:
            if A.HasProperty(delprop):
                A.RemoveProperty(delprop)
        g.Disconnect(A)
        A.Close()



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
        g.Disconnect( A, D_OUT )
        #connect_RNG_candidates(g, A, candidate_addr_list=Rx5_all, vulnerable_set=None, degree=R, alpha=a, max_odeg_ratio=1.5, recursion=1)
        connect_RNG_candidates(g, A, candidate_addr_list=Rx5_all, degree=R, alpha=a, max_odeg_ratio=1.5, recursion=1)
        prune_RNG_neighborhood(g, A, degree=R, alpha=a, depth=4, max_odeg_ratio=1.5, recursion=recursion)
        A.Close()
        print( f"{100*n/N:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )



def build_proximity_graph(g, degree=48, alpha=1.0):
    global n_Q_IMMED
    global n_Q_RNG
    global n_Q_FIND
    global n_PRUNE
    n_Q_IMMED = 0
    n_Q_RNG = 0
    n_Q_FIND = 0
    n_PRUNE = 0
    t0 = time.time()
    # ----
    print( "=== INITIALIZE ===" )
    print( f"Clearing existing graph" )
    clear_links(g)
    # 2% seed via brute force
    seedsize = g.Order("item") // 50 # 2%
    d = 3*degree
    print( f"Creating seed graph (o={seedsize}, d={d})" )
    destroy_seed(g)
    build_seed(g, sz=seedsize, R=d, ham=15)
    skeletons = 20 # just a number, no reason
    sk = 0
    # Another 5% via non-seed subsampling and crude ANN
    seed_set = set(g.Neighborhood( "seedroot", arc=D_OUT, fields=F_ADDR, result=R_SIMPLE ))
    full_set = set(g.Vertices( condition={'type':'item'}, fields=F_ADDR, result=R_SIMPLE ))
    nonseed_set = full_set - seed_set
    skeleton_set = set()
    while sk < skeletons:
        sk += 1
        d, a, nR, s = int(2*degree), 0.8, 2, 0.05/skeletons
        print( f"Adding skeleton {sk}/{skeletons} (o={int(s*len(nonseed_set))}, d={d}, a={a}, nR={nR}, s={s})" )
        populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=100, qbw=100, qbc=0.7, qadaptive=False, process_set=nonseed_set, sample_population=s, actual_set=skeleton_set, rounds=2)
        base_set = skeleton_set.union( seed_set )
        rescue_remotes(g, degree=d, cutoff_ideg=12, process_set=base_set )
    print( "Removing seed information" )
    destroy_seed(g)
    del nonseed_set
    del skeleton_set
    del seed_set
    d, a, nR = int(1.666*degree), 0.9, 1.8
    print( f"Refining base graph (o={len(base_set)}, d={d}, a={a}, nR={nR})" )
    populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=200, qbw=3, qbc=1.0, qadaptive=True, process_set=base_set)
    rescue_remotes(g, degree=d, cutoff_ideg=11, process_set=base_set )
    t1 = time.time()
    print( f"t={int(t1-t0)}" )
    # ----
    print( "=== ROUND 1 ===" )
    rest_set = full_set - base_set
    del base_set
    d, a, nR = int(1.333*degree), alpha, 1.7
    print( f"Adding full population to graph (o={len(rest_set)}, d={d}, a={a}, nR={nR})" )
    populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=150, qbw=100, qbc=0.9, qadaptive=False, process_set=rest_set)
    print( "Rescuing remotes" )
    rescue_remotes(g, degree=d, cutoff_ideg=10 )
    t2 = time.time()
    print( f"t={int(t2-t0)}" )
    g.CloseAll() # !!!
    g.Save()     # !!!
    # ----
    print( "=== ROUND 2 ===" )
    d, a, nR = int(1.2*degree), alpha * 1.05, 1.6
    print( f"Populating graph (d={d}, a={a}, nR={nR})" )
    populate(g, degree=d, alpha=a, max_odeg_ratio=nR, entry=None, qshadow=300, qbw=3, qbc=1.0, qadaptive=True, process_set=full_set )
    rescue_remotes(g, degree=d, cutoff_ideg=9)
    t3 = time.time()
    print( f"t={int(t3-t0)}" )
    g.CloseAll() # !!!
    g.Save()     # !!!
    # ----
    print( "=== FINALIZE ===" )
    nR = 1.5
    print( f"Pruning graph (d={degree}, a={alpha}, nR={nR})" )
    prune_all(g, degree, alpha, prunable_mindegree=degree//2, max_odeg_ratio=nR)
    rescue_remotes(g, degree=degree, cutoff_ideg=8)
    print( "Creating entry point" )
    topstar(g, degree=int((nR-0.1)*degree))
    #enhance_star(g, entry='entry', R=int(3*degree), a=0.95)
    if ROOT:
        medoid = sample_medoid(g, sz=10000, degree=degree)
        g.Connect(ROOT, ("to",M_STAT|M_FWDONLY), medoid)
    t6 = time.time()
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






def INIT(graph, h=512, shw=0, f=0, bw=256, bc=1.0, init=8, bmin=8, bmax=512, depth=(1<<31)-1, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, epsilon=0.0, lambd=0.0, adaptive=True ):
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
                                recursion = {
                                    'heap_size': h,
                                    'shadow_size': shw,
                                    'frontier_limit': f,
                                    'depth_limit': depth,
                                    'beam_width': bw,
                                    'beam_curve': bc,
                                    'beam_min': bmin,
                                    'beam_max': bmax,
                                    'adaptive_taper': adaptive,
                                    'alpha' : alpha,
                                    'beta' : beta,
                                    'gamma' : gamma,
                                    'delta' : delta,
                                    'epsilon' : epsilon,
                                    'lambda': lambd,
                                    'init_select': init
                                }
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




old_R52_raw = """
shadow recall qps evals score_contrib beam_accepts result_accepts neighbor_expands topk_depth query_depth
1 0.7847 7992.2 1374 180 55 105 20 7.23 8.81
2 0.7877 7992.3 1389 184 55 105 20 7.26 8.86
3 0.7906 7407.9 1411 188 56 106 20 7.29 8.95
4 0.7963 7640.5 1435 193 57 106 21 7.35 9.04
5 0.8021 7715.8 1459 198 57 106 21 7.4 9.11
6 0.8044 7392 1481 203 58 106 22 7.43 9.18
8 0.81 7044 1523 212 60 106 22 7.49 9.31
9 0.8149 6997 1548 216 60 106 23 7.55 9.41
11 0.821 6947.1 1592 225 62 106 24 7.63 9.54
13 0.8277 6517.1 1635 234 63 107 24 7.7 9.69
16 0.8361 6464.3 1703 248 65 107 26 7.79 9.89
19 0.8438 6250.3 1773 261 68 107 27 7.91 10.12
22 0.8531 6008.9 1849 274 70 107 28 8.05 10.34
26 0.8602 5640.7 1941 291 73 107 30 8.13 10.58
32 0.874 5278.3 2092 316 78 107 33 8.29 10.97
38 0.8862 4838.7 2277 342 83 108 36 8.3 11.23
45 0.9126 4093.4 2677 379 105 109 44 7.4 10.34
53 0.9228 3733.2 2937 413 112 109 49 7.47 10.65
64 0.9365 3284.2 3323 459 123 109 56 7.47 10.95
76 0.9494 2746.2 3950 518 154 110 69 6.71 10.27
90 0.9595 2416.3 4486 578 171 110 80 6.64 10.45
107 0.9688 2008 5365 659 209 111 98 6.18 10.11
128 0.9751 1742.3 6136 747 235 111 114 6.17 10.37
152 0.9793 1503.8 7003 844 265 111 133 6.13 10.71
181 0.9855 1227.7 8632 1118 351 126 167 5.6 9.99
215 0.9877 1104.9 9568 1245 388 126 188 5.62 10.38
256 0.9893 1000.9 10497 1383 427 127 209 5.71 10.92
304 0.991 867.2 11966 1609 503 128 242 5.48 10.9
362 0.9923 804.8 12973 1790 547 128 265 5.63 11.55
430 0.9936 734.8 14124 1990 597 128 291 5.75 12.38
512 0.9947 650.6 15857 2301 687 129 331 5.63 12.55
608 0.9955 592.4 17314 2554 761 129 365 5.76 13.47
724 0.9966 539.9 18910 2828 851 129 404 5.89 14.28
861 0.9972 473.8 21243 3252 989 131 461 5.73 14.38
1024 0.9978 435.6 22925 3599 1109 131 503 5.8 15.24
1217 0.9982 398.3 24748 3988 1252 131 550 5.86 15.84
1448 0.9984 351.2 27637 4562 1464 131 626 5.69 16.01
1722 0.9987 317.4 29984 5057 1657 131 688 5.71 16.68
2048 0.9988 283.7 33295 5778 1936 132 779 5.49 16.78
2435 0.999 262.2 35595 6409 2159 132 841 5.49 17.4
2896 0.9989 244.4 37882 7152 2406 132 903 5.49 17.93
3444 0.9993 218.9 41678 8185 2791 132 1010 5.32 18.27
4096 0.9995 203.7 44090 9180 3093 132 1076 5.33 18.73
4870 0.9995 190.4 46983 10378 3442 132 1157 5.33 19.63
5792 0.9996 173.7 50917 11865 3931 133 1272 5.16 19.56
6888 0.9996 160.2 54690 13449 4387 133 1379 5.16 20.16
8192 0.9995 146.7 58864 15241 4868 133 1502 5.16 20.88
9741 0.9998 132.8 63737 17465 5496 133 1652 5.04 20.72
11585 0.9996 121.7 68559 19819 6000 133 1802 5.03 21.09
13777 0.9998 110.9 73968 22661 6534 133 1967 5.03 21.43
16384 0.9999 104.7 80242 25996 7334 134 2173 4.91 20.83
19483 0.9997 96.2 86821 29909 7973 134 2372 4.91 20.53
23170 0.9998 87.4 93828 34244 8885 134 2607 4.81 19.8
27554 1 79.9 101457 39387 9711 134 2861 4.81 19.44
""".replace("\t", " ").strip().split("\n")[1:]



RAW_PERF_REFERENCE = """
shadow recall qps evals score_contrib beam_accepts result_accepts neighbor_expands topk_depth query_depth qps_delta
1 0.7695 8685.7 1247 180 56 105 20 7.39 8.98 -7.50%
2 0.7715 8830.7 1257 183 56 105 20 7.41 9.02 -6.90%
3 0.7748 8287.5 1278 188 57 105 21 7.46 9.11 -5.80%
4 0.7783 8292.2 1299 193 58 106 21 7.51 9.21 -4.70%
5 0.7828 8321.4 1320 197 58 106 21 7.56 9.29 -3.70%
6 0.7877 8188.2 1341 202 59 106 22 7.63 9.38 -3.50%
7 0.7896 7793.6 1361 206 60 106 22 7.67 9.46 -3.00%
8 0.7941 7833.8 1382 211 61 106 23 7.71 9.54 -3.10%
9 0.7993 7649.3 1402 216 61 106 23 7.78 9.63 -3.10%
10 0.8012 7791 1420 221 62 106 23 7.81 9.7 -2.40%
12 0.8057 7322.2 1459 229 63 106 24 7.89 9.85 -2.20%
13 0.8085 7474.7 1477 234 64 106 25 7.92 9.93 -2.30%
16 0.8194 6886.6 1540 247 66 107 26 8.08 10.15 -2.60%
18 0.8266 6830.9 1585 256 68 107 27 8.19 10.33 -2.60%
21 0.8362 6494.7 1653 269 70 107 28 8.31 10.58 -3.00%
24 0.8441 6238.3 1720 282 73 107 29 8.44 10.81 -3.10%
27 0.8518 6097 1791 295 75 107 31 8.56 11.04 -2.60%
32 0.8668 5394.1 1916 316 79 108 33 8.79 11.4 -4.80%
36 0.8784 5256.1 2037 333 83 108 36 8.85 11.59 -5.60%
42 0.8988 4561.4 2332 371 103 110 42 7.65 10.51 -5.50%
48 0.906 4320.2 2494 396 109 110 46 7.75 10.78 -3.20%
55 0.9151 3942.5 2691 425 115 110 50 7.83 11.06 -1.80%
64 0.9275 3604 2963 462 124 110 56 7.92 11.41 -3.50%
73 0.9402 3076.2 3453 514 152 112 66 6.97 10.51 -1.40%
84 0.9481 2785.5 3813 561 164 112 74 6.97 10.75 -2.00%
97 0.9568 2506.2 4269 616 180 112 85 6.99 11.05 -1.80%
111 0.9649 2142.4 4981 695 215 113 101 6.46 10.55 -0.30%
128 0.9705 1910.7 5556 766 235 113 114 6.42 10.83 -0.30%
147 0.9758 1714.6 6194 842 258 113 129 6.44 11.11 -1.40%
168 0.9793 1545.3 6851 919 283 113 145 6.45 11.4 -2.20%
194 0.9832 1326.7 7913 1038 336 114 171 6.05 11.04 -1.40%
222 0.9861 1206 8688 1132 365 115 190 6.09 11.38 -2.20%
256 0.988 1089.7 9571 1245 399 115 212 6.17 11.8 -2.00%
294 0.9891 964.1 10776 1404 459 116 242 5.9 11.69 3.80%
337 0.9907 883.4 11770 1545 499 116 266 5.98 12.07 0.90%
388 0.9924 809.3 12814 1706 544 116 292 6.08 12.63 -1.60%
445 0.9936 739.8 13914 1873 596 116 320 6.2 13.13 -1.50%
512 0.9945 653.3 15604 2117 678 116 364 6.02 13.22 -0.10%
588 0.9952 601.2 17020 2304 744 116 401 6.12 13.86 1.50%
675 0.9957 549.5 18483 2503 815 116 440 6.25 14.38 5.00%
776 0.9964 486.5 20703 2817 924 117 501 6.09 14.43 11.60%
891 0.9965 446.4 22470 3065 1023 117 550 6.14 14.89 19.30%
1024 0.9971 409.3 24236 3342 1131 117 601 6.21 15.36 15.10%
1176 0.9975 378.4 26003 3641 1251 117 652 6.25 15.76 17.70%
1351 0.9982 335.4 28876 4076 1424 117 737 6.08 15.56 16.70%
1552 0.9984 313.1 30686 4440 1567 117 791 6.09 15.92 14.00%
1782 0.9986 291.6 32557 4854 1728 117 847 6.11 16.39 11.50%
2048 0.999 260.3 36001 5441 1967 118 954 5.91 16.01 2.80%
"""

PERF_REFERENCE = []


def init_perf_reference():
    low = "0 0.0 1e5 0 0 0 0 0 0.0 0.0"
    high = "1000000 1.0 0.0 1000000 1000000 1000000 1000000 1000000 1e5 1e5"
    raw = RAW_PERF_REFERENCE.strip().replace("\t", " ").split("\n")[1:]
    data = [entry.split() for entry in [low] + raw + [high]]
    # recall evals
    pairs = [ (float(entry[1]), int(entry[3])) for entry in data]
    PERF_REFERENCE.clear()
    PERF_REFERENCE.extend( pairs )
 


init_perf_reference()


def perf_reference( recall ):
    recall = round(recall, 4) # to match rounded raw data
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
        
        
        




def work(g, PROBES, entry, k, h, shw, f, bw, bc, init, bmin, bmax, depth, alpha, beta, gamma, delta, epsilon, lambd, r_result, adaptive=True, show=False):
    MEM, Q = INIT(g, h=h, shw=shw, f=f, bw=bw, bc=bc, init=init, bmin=bmin, bmax=bmax, depth=depth, alpha=alpha, beta=beta, gamma=gamma, delta=delta, epsilon=epsilon, lambd=lambd, adaptive=adaptive)
    testrecall(MEM, Q, g, k, P=PROBES, entry=entry, show=show, r_result=r_result)


def threadwork( g, N, PROBES, entry, k, h, shw, f, bw, bc, init, bmin, bmax, depth, alpha, beta, gamma, delta, epsilon, lambd, adaptive=True, perfonly=False, key=None ):
    if bmax < bmin: bmax = bmin
    if bw < bmin: bw = bmin
    elif bw > bmax: bw = bmax
    T = []
    sz = len(PROBES) // N
    i = 0
    for n in range(N):
        sample = PROBES[i:i+sz]
        r_result = {}
        args = (g, sample, entry, k, h, shw, f, bw, bc, init, bmin, bmax, depth, alpha, beta, gamma, delta, epsilon, lambd, r_result, adaptive)
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
    epq = total_sum_evals // total_queries
    cpq = total_sum_contributes // total_queries
    fpq = total_sum_frontiers // total_queries
    apq = total_sum_accepts // total_queries
    qdpq = total_sum_qdepths / total_queries
    xpq = total_sum_nexpand // total_queries
    evalrate = (epq / (avg_latency_ms/1000)) / 1000000 # million evals per second
    contributerate = 100*cpq/epq
    frontierrate = 100*fpq/epq
    acceptrate = 100*apq/epq
    is_adaptive_taper = "(a)" if adaptive else ""
    total_sum_already = sum([r_result['sum_already'] for _, r_result in T ])
    total_sum_visited = sum([r_result['sum_visited'] for _, r_result in T ])
    vpq = total_sum_visited // total_queries
    hpq = total_sum_already // total_queries
    if not perfonly:
        config = f"e={entry} t={N} heap={h} shadow={shw} front={f} beam={bw} range=({bmin}-{bmax}) taper={bc}{is_adaptive_taper} init={init} a={alpha:0.4f} b={beta:0.4f} c={gamma:0.4f} d={delta:0.4f}"
        result = f"qps={qps:0.1f} recall={recall:0.4f}@{k} latency={avg_latency_ms:0.2f}ms ev={epq} con={cpq} fr={fpq} acc={apq} x={xpq} tkd={tkdpq:0.2f} qd={qdpq:0.2f} stop={hpq} visit={vpq}"
        print( f"{config} --> {result} qps_wall={qps_wall:0.1f}" )
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
        print( f"{keyval}{recall:0.4f} {qps:0.1f} {epq} {cpq} {fpq} {apq} {xpq} {tkdpq:0.2f} {qdpq:0.2f} {evals_delta:0.1f}%" )
    return recall, qps




def threadtest( g, N, PROBES, entry, k=10, heaps=None, shadows=None, fronts=None, beams=None, bcs=None, inits=None, bmin=8, bmax=512, depth=(1<<31)-1, alpha=0.0, beta=0.0, gamma=0.0, delta=0.0, epsilon=0.0, lambd=0.0, adaptive=True, perfonly=False, key=None ):
    if heaps is None:
        heaps = [1024, 768, 512, 384, 256, 192, 128, 96, 64, 48, 32, 24]
    auto_shadows = []
    if shadows is None:
        auto_shadows = [2*h for h in heaps]
    auto_fronts = []
    if fronts is None:
        auto_fronts = [int(1.2*h*(1+shwfactor)) for h in heaps]
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
                            recall, qps = threadwork( g, N, PROBES, entry, k=k, h=h, shw=shw, f=f, bw=bw, bc=bc, init=init, bmin=bmin, bmax=bmax, depth=depth, alpha=alpha, beta=beta, gamma=gamma, delta=delta, epsilon=epsilon, lambd=lambd, adaptive=adaptive, perfonly=perfonly, key=key )



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






system.Initialize( "annindex", http=9000 )
g = Graph("ann")


M_RNG, Q_RNG, M_FIND, Q_FIND, Q_IMMED = QMINIT(g)

    
MEM, Q = INIT(g)
 

PROBES100k = [ g.sim.NewVector(p, cosine_mode=1) for p in g['cache']['probes100k'] ]

SCAN_CACHE = g['cache']['SCAN_CACHE']

ENTRIES = ['entry']*len(PROBES) 


ROOT = "root-part1.dump"
medoid = g[ROOT].Terminals()[0] # 4fa8ff21-6154-4485-b539-8a1ebf8fac00



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

def s2bw(s):
    bw = int(round(log2(s**sqrt(2))))-5
    return bw if bw > 2 else 2


def clamp( x, lo, hi):
  return lo if x < lo else hi if x > hi else x



def sh2delta(s):
    s0 = 145
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
#for shw in [1,1,2,3,4,5,6,7,8] + [ int(2**(x/5)) for x in range(16,56) ]: threadtest( g, 1, PROBES100k[:2000], 'entry', heaps=[10], shadows=[shw], fronts=[0], beams=[s2bw(shw)], bcs=[0.99], inits=[s2bw(shw)], bmin=2, bmax=(4+s2bw(shw))**2, depth=100000, alpha=-0.7, beta=0.0, gamma=0.0, delta=sh2delta(shw), epsilon=0.0, lambd=0.05, adaptive=True, perfonly=1, key="shw" )

