from pyvgx import *
import json
import sys
import random
import time
import struct
import base64
import threading


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
    # Get random nodes
    t0 = time.perf_counter()
    print( f"Getting {sz} random nodes" )
    nodes = g.Vertices( sortby=S_RANDOM, hits=sz )
    # Connect temp root to nodes
    T = g.NewVertex( "seedroot", type="temp" )
    for node in nodes:
        A = g.OpenVertex( node )
        lsh32 = A.GetVector().LSH32()
        A['lsh32'] = lsh32
        g.Connect( T, ("lsh", M_LSH|M_FWDONLY, lsh32), A )
        A.Close()
    # Connect all seed nodes to their closest neighbors
    M = g.Memory(4)
    Q = g.NewNeighborhoodQuery( "seedroot", arc=('lsh', D_OUT, M_LSH, V_LTE, (0,ham)), memory=M, filter="next != r2", rank="cosine(r1, next.vector)", sortby=S_RANK, fields=F_ID )
    i = 0
    j = 0
    for node in nodes:
        i += 1
        A = g.OpenVertex( node )
        probe = A.GetVector()
        M.R1 = probe
        M.R2 = A.address
        Q.arclsh = (A['lsh32'], ham)
        nearest = Q.Execute( hits=R )
        j += len(nearest)
        for near in nearest:
            B = g.OpenVertex( near )
            asim = 1.1 * probe.Cosine( B.GetVector() ) # alpha * cos
            g.Connect( A, ("asim", M_FLT|M_FWDONLY, asim), B )
            g.Connect( B, ("asim", M_FLT|M_FWDONLY, asim), A )
            B.Close()
        A.Close()
        print( f"\r{i}  ", end="", flush=True )
    print()
    t1 = time.perf_counter()
    print( f"Seed graph o={i} s={j} (R={j/i:.1f}) in {t1-t0:.1f} sec" )






def destroy_seed(g):
    for node in g.Terminals( "seedroot" ):
        g.Disconnect( node )
    g.DeleteVertex( "seedroot" )

        


def sample_medoid(g, sz=50000):
    C = g.sim.NewCentroid( [g[x].GetVector() for x in g.Vertices( condition={ 'type':'item', 'outdegree':(V_GTE,32) }, sortby=S_RANDOM, hits=sz )  ] )
    return g.Vertices( condition={'outdegree':(V_GTE,32)}, vector=C, hits=1, sortby=S_SIM )[0]


M_PRUNE = None
Q_PRUNE = None
M_FIND = None
Q_FIND = None

def QMINIT(g):
    global M_PRUNE
    global Q_PRUNE
    global M_FIND
    global Q_FIND
    M_PRUNE = g.Memory(4)
    Q_PRUNE = g.NewAdjacencyQuery(
        memory = M_PRUNE,
        filter = "cosine(r1, next.vector) > next.arc.value"
    )
    M_FIND = g.Memory(32)
    Q_FIND = g.NewNeighborhoodQuery(
                memory  =   M_FIND,
                arc     =   D_OUT,
                filter  =   "anncollect( 0.0 )",
                collect =   C_SCAN,
                sortby  =   S_RVAL,
                fields  =   F_VAL | F_ID,
                result  =   R_LIST,
                recursion = {
                    'heap_size'         : 130,
                    'shadow_size'       : 39,
                    'frontier_limit'    : 390,
                    'beam_width'        : 390,
                    'beam_curve'        : 0.6,
                    'beam_min'          : 6,
                    'reset_map'         : False,
                    'reset_metrics'     : True
                }
    )




def get_hubs(g, min_ideg=200, max_hubs=15000 ):
    hubs = g.Vertices(
            condition={
                'type':'item',
                'indegree':(V_GTE,min_ideg)
            },
            hits=max_hubs,
            sortby=S_IDEG )
    return hubs



def neighbor_coherence(g, node):
    M = g.Memory(4)
    M.vector = g[node].GetVector()
    g.Neighborhood(
        node,
        memory  = M,
        collect = C_SCAN,
        filter  = "s=cosine(M.vector, next.vector); storeif(s>r1, R1, s);",
        hits    = 0
    )
    max_neighbor = M.R1
    vectors = [g[term].GetVector() for term in g.Terminals( node )]
    d = len(vectors)
    if d < 2:
        return (1.0, max_neighbor)
    csum = 0.0
    for i in range(d-1):
        for j in range(i+1, d):
            csum += g.sim.Cosine( vectors[i], vectors[j] )
    coherence = (2/(d*(d-1))) * csum
    return (coherence, max_neighbor)



def update_node_stats(g):
    n = 0
    for node in g.VerticesType('item'):
        n += 1
        coherence, max_neighbor = neighbor_coherence(g, node)
        A = g.OpenVertex(node)
        A.c0 = coherence
        A.c1 = max_neighbor
        A.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )





def neighbor_diversity(g, node):
    M = g.Memory(4)
    n = g[node].odeg
    M.vector = g[node].GetVector()
    neighbor_cos = g.Neighborhood(
        node,
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
        mean_neighbor_cos = sum( neighbor_cos ) / len( neighbor_cos )
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
                        require( next.id != "{hub}" );
                        require( vset.add(next)==1 );
                        s=cosine(M.vector, next.vector);
                        require(s < r2);
                        require(s > r1);
                        collect(s)
                        """
                }
            },
            fields = F_VAL|F_ID,
            result = R_LIST
        )
        # Reverse-connect the least similar 2-hop neighbors back to the hub node
        for score, farnode in hop2:
            connect_attempts += 1
            F = g.OpenVertex(farnode)
            cos = g.sim.Cosine(H, F) 
            if simulate:
                print( f"{farnode} -({cos:0.4f})-> {hub}" )
            else:
                r = g.Connect( F, ('cos', M_FLT|M_FWDONLY, cos), H )
                if r > 0:
                    connected += 1
            F.Close()
        H.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
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
        mean_neighbor_cos = sum( neighbor_cos ) / len( neighbor_cos )
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
                        require( next.id != "{hub}" );
                        require( vset.add(next)==1 );
                        s=cosine(M.vector, next.vector);
                        require(s < r2);
                        require(s > r1);
                        collect(s)
                        """
                }}
                
            }},
            fields = F_VAL|F_ID,
            result = R_LIST
        )
        # Reverse-connect the least similar 3-hop neighbors back to the hub node
        for score, farnode in hop3:
            connect_attempts += 1
            F = g.OpenVertex(farnode)
            cos = g.sim.Cosine(H, F) 
            if simulate:
                print( f"{farnode} -({cos:0.4f})-> {hub}" )
            else:
                r = g.Connect( F, ('cos', M_FLT|M_FWDONLY, cos), H )
                if r > 0:
                    connected += 1
            F.Close()
        H.Close()
        if not n % 1000:
            print( f"{100*n/g.order:0.1f}%", end="\r", flush=1 )
    print( "100.0%" )
    return connect_attempts, connected




def rescue_remotes(g, cutoff_ideg=6, max_rescue_degree=20 ):
    remotes = g.Vertices( condition={'type':'item', 'indegree':(V_LTE,cutoff_ideg)} )
    N = len(remotes)
    if N == 0:
        return 0
    n = 0
    c = 0
    for remote in remotes:
        n += 1
        R = g.OpenVertex( remote )
        ideg_boost = max_rescue_degree - R.ideg
        probe = R.GetVector()
        for true_neighbor, score in execscan(g, probe, k=ideg_boost):
            T = g.OpenVertex( true_neighbor )
            cos = g.sim.Cosine(R, T)
            g.Connect( T, ('cos', M_FLT|M_FWDONLY, cos), R )
            c += 1
            T.Close()
        R.Close()
        print( f"{n}/{N} {100*n/N:0.2f}% {c}", end="\r", flush=1 )
    print( f"{n}/{N} {100*n/N:0.2f}% {c}" )
    return N






def prune_RNG_neighborhood(g, A):
    C = g.Neighborhood(A, fields=F_VAL|F_ID, result=R_LIST, sortby=S_VAL)
    g.Disconnect( A, D_OUT )
    M_PRUNE.Reset()
    for asim, c in C:
        if c == A.id: # Fix this in the query generating C
            continue
        B = g.OpenVertex(c)
        Q_PRUNE.id = A
        M_PRUNE.vector = B.GetVector()
        # c too close to existing neighbor of, skip
        if Q_PRUNE.Execute():
            B.Close()
            continue
        # c accepted
        g.Connect( A, ('asim', M_FLT|M_FWDONLY, asim), B )
        B.Close()
        if A.odeg >= 40:
            return




def connect_RNG_candidates(g, A, C):
    # TODO: FIX that we're connecting to ourselves
    if A.odeg > 0:
        g.Disconnect(A, D_OUT)
    M_PRUNE.Reset()
    for score, c in C:
        if c == A.id: # Fix this in the query generating C
            continue
        B = g.OpenVertex(c)
        Q_PRUNE.id = A
        M_PRUNE.vector = B.GetVector()
        # c too close to existing neighbor of, skip
        if Q_PRUNE.Execute():
            B.Close()
            continue
        # c accepted
        asim = 1.2 * (score-1) # alpha * (cos+1-1)
        g.Connect( A, ('asim', M_FLT|M_FWDONLY, asim), B )
        g.Connect( B, ('asim', M_FLT|M_FWDONLY, asim), A )
        if B.odeg > 80:
            prune_RNG_neighborhood(g, B)
        B.Close()
        if A.odeg >= 40:
            return




def find_candidates(g, A, roots):
    C = []
    M_FIND.Reset()
    M_FIND.vector = A.GetVector()
    for root in roots:
        Q_FIND.id = root
        C.extend( Q_FIND.Execute( hits=40 ) ) # Keep the vset since reset_state is True
    return sorted(C, reverse=1)



def process_node(g, node, medoid, random_roots):
    A = g.OpenVertex(node)
    probe = A.GetVector()
    selected_roots = [medoid]
    selected_roots.extend( random.sample( random_roots, 5 ) )
    C = find_candidates(g, A, selected_roots)
    connect_RNG_candidates(g, A, C)
    A.Close()
         


def prune_all(g):
    for node in g.Vertices( condition={'type':'item', 'outdegree':(V_GT,32)} ):
        A = g.OpenVertex(node)
        prune_RNG_neighborhood(g, A)
        A.Close()



def get_random_roots(g, n):
    # Pick n random nodes that are part of the graph
    return g.Vertices( condition={ 'filter':"vertex.type == 'item' && vertex.odeg >= 32" }, sortby=S_RANDOM, hits=n )   



def populate(g):
    t0 = time.perf_counter()
    # Get all nodes not in the seed graph
    if "seedroot" in g:
        seed_set = set(g.Terminals("seedroot"))
        g.DeleteVertex( "seedroot" )
        full_set = set(g.VerticesType('item'))
        process_set = full_set - seed_set
    else:
        process_set = set(g.VerticesType('item'))
    process_list = random.sample( sorted(process_set), len(process_set) )
    N = len(process_list)
    n = 0
    refresh_roots_at_n = 0
    search_roots = []
    for node in process_list:
        if n >= refresh_roots_at_n:
            medoid = sample_medoid(g,100000)
            random_roots = get_random_roots(g, 256)
            refresh_roots_at_n += N//8
        process_node(g, node, medoid, random_roots)
        n += 1
        if not n % 100:
            t1 = time.perf_counter()
            t = t1-t0
            nps = n // t
            print( f"\r{n}/{N} {t:.1f}s {nps}/s  o={g.order}   ", end="", flush=True )
    print( f"\r{n}/{N} {t:.1f}s {nps}/s  o={g.order}   ", flush=True )



def build_proximity_graph(g):
    build_seed(g, sz=50000, R=32, ham=15)
    populate(g)



def clear_links(g):
    for node in g.Vertices():
        g.Disconnect(node)



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
        terminals = g.Neighborhood( A, vector=V, fields=F_VAL|F_ID, result=R_LIST, collect=C_SCAN, filter="c=cosine(vector, next.vector); collect(c);" )
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



def get_diverse_subset( g, node ):
    odeg = 55
    threshold = 0.5
    while True:
        D = diversity( g, g.Neighborhood( node, sortby=S_ODEG, neighbor={'outdegree':(V_GTE,odeg)}, hits=16 ), threshold )
        if len(D) >= 8:
            break
        odeg -= 1
        threshold += 0.05
        if threshold >= 1.0:
            break
    if len(D) < 8:
        D = g.Neighborhood( node, sortby=S_ODEG, hits=8 )
    return D




def reconnect_with_diverse_subset( g, node ):
    T = g.Terminals(node)
    D = get_diverse_subset(g, node)
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
        
        
        




def get_entry_nodes(g, maxcand=1500000, starsize=500, max_mutual_cos=0.38, min_odeg=32 ):
    # Get neighbor diversity (stdev of neighbors' similarity to node)
    C = [ (neighbor_diversity(g,c)[1], c) for c in g.Vertices( condition={'type':'item', 'outdegree':(V_GTE,min_odeg)}, sortby=S_RANDOM, hits=maxcand ) ]
    C.sort( reverse=1 ) # sort by stdev so the best candidates are likely to make it into star
    # Now build up the diverse star from the collected candidates
    S = []
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
        S.append( cand )
        if len(S) >= starsize:
            break
    return S




def topstar(g, maxcand=1500000, starsize=500, max_mutual_cos=0.38, min_odeg=32 ):
    E = get_entry_nodes(g, maxcand, starsize, max_mutual_cos, min_odeg)
    A = g.NewVertex( "entry", type="entry" )
    for entry in E:
        g.Connect( A, ('cos', M_FLT|M_FWDONLY, 0.0), entry )
    A.Close()



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






def INIT(graph, h=512, shw=0, f=0, bw=256, bc=1.0, init=8, bmin=8, bmax=512, alpha=1.0/64, beta=0.0, gamma=1.0, adaptive=True ):
    MEM = graph.Memory(32)
    Q = graph.NewNeighborhoodQuery(
                                memory  =   MEM,
                                #arc     =   ('to', D_OUT, M_INT, V_GTE, 1),
                                #arc     =   ('*', D_OUT, M_INT),
                                arc     =   D_OUT,
                                #filter  =   "anncollect( 0.0 )",
                                #collect =   C_SCAN,
                                sortby  =   S_RVAL,
                                fields  =   F_VAL | F_ID,
                                result  =   R_LIST,
                                recursion = {
                                    'heap_size': h,
                                    'shadow_size': shw,
                                    'shadow_alpha': alpha,
                                    'shadow_beta': beta,
                                    'frontier_limit': f,
                                    'beam_width': bw,
                                    'beam_curve': bc,
                                    'beam_gamma': gamma,
                                    'beam_min': bmin,
                                    'beam_max': bmax,
                                    'adaptive_taper': adaptive,
                                    #'arc_prune_until': 2,
                                    #'arc_prune_score': 5.0,
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
                    print( f"{n:3d}. {score:0.3f}   {id}  {graph[id]['title']}" )
                else:
                    print( f"{n:3d}. {score:0.3f} ! {id} ({graph[id]['title']})" )
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





SCAN_CACHE = {}

def scan(g, probe, k=10, sortdir=S_DESC, fname=None):
    key = f"{probe.ident}_{k}"
    result = SCAN_CACHE.get( key )
    if result is not None:
        return result
    SCAN_CACHE[key] = execscan(g, probe, k, sortdir, fname)
    return SCAN_CACHE[key]
    #####
    mem = g.Memory(4)
    mem.R1 = probe.internal if type(probe) is Vector else graph.sim.NewVector(probe).internal
    cond = "true"
    if fname:
        cond += f" && vertex.property('fname') == '{fname}'"
    result = g.Vertices(
        memory = mem,
        condition = { 'type':'item', 'filter': cond },
        sortby = S_RANK|sortdir,
        rank = "1 + cos_pi8( r1, vertex.vector)",
        hits = k,
        fields = F_ID|F_RANK,
        result = R_LIST
    )
    SCAN_CACHE[key] = result
    return result



def execscan(g, probe, k=10, sortdir=S_DESC, fname=None):
    mem = g.Memory(4)
    mem.vector = probe if type(probe) is Vector else graph.sim.NewVector(probe)
    cond = "true"
    if fname:
        cond += f" && vertex.property('fname') == '{fname}'"
    result = g.Vertices(
        memory = mem,
        condition = { 'type':'item', 'filter': cond },
        sortby = S_RANK|sortdir,
        rank = "1 + cosine( M.vector, vertex.vector)",
        hits = k,
        fields = F_ID|F_RANK,
        result = R_LIST
    )
    return result





def work(g, PROBES, entry, k, h, shw, f, bw, bc, init, bmin, bmax, alpha, beta, gamma, r_result, adaptive=True, show=False):
    MEM, Q = INIT(g, h=h, shw=shw, f=f, bw=bw, bc=bc, init=init, bmin=bmin, bmax=bmax, alpha=alpha, beta=beta, gamma=gamma, adaptive=adaptive)
    testrecall(MEM, Q, g, k, P=PROBES, entry=entry, show=show, r_result=r_result)


def threadwork( g, N, PROBES, entry, k, h, shw, f, bw, bc, init, bmin, bmax, alpha, beta, gamma, adaptive=True, perfonly=False ):
    if bmax < bmin: bmax = bmin
    if bw < bmin: bw = bmin
    elif bw > bmax: bw = bmax
    T = []
    sz = len(PROBES) // N
    i = 0
    for n in range(N):
        sample = PROBES[i:i+sz]
        r_result = {}
        args = (g, sample, entry, k, h, shw, f, bw, bc, init, bmin, bmax, alpha, beta, gamma, r_result, adaptive)
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
    total_sum_evals = sum([r_result['sum_evals'] for _, r_result in T])
    total_sum_contributes = sum([r_result['sum_contributes'] for _, r_result in T])
    total_sum_frontiers = sum([r_result['sum_frontiers'] for _, r_result in T])
    total_sum_accepts = sum([r_result['sum_accepts'] for _, r_result in T])
    epq = total_sum_evals // total_queries
    cpq = total_sum_contributes // total_queries
    fpq = total_sum_frontiers // total_queries
    apq = total_sum_accepts // total_queries
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
        config = f"e={entry} t={N} heap={h} shadow={shw} front={f} beam={bw} range=({bmin}-{bmax}) taper={bc}{is_adaptive_taper} init={init} a={alpha:0.4f} b={beta:0.4f} c={gamma:0.4f}"
        #result = f"qps={qps:0.1f} recall={recall:0.4f}@{k} latency={avg_latency_ms:0.2f}ms evals={epq} ({evalrate:0.1f}M/s/t {N*evalrate:0.1f}M/s) accepts={apq} ({acceptrate:0.1f}%)"
        result = f"qps={qps:0.1f} recall={recall:0.4f}@{k} latency={avg_latency_ms:0.2f}ms ev={epq} con={cpq} fr={fpq} acc={apq} stop={hpq} visit={vpq}"
        print( f"{config} --> {result} qps_wall={qps_wall:0.1f}" )
    else:
        print( f"{recall:0.4f} {qps:0.1f} {epq} {cpq} {fpq} {apq}" )
    return recall, qps




def threadtest( g, N, PROBES, entry, heaps=None, shadows=None, fronts=None, beams=None, bcs=None, inits=None, bmin=8, bmax=512, alpha=1.0/64, beta=0.0, gamma=1.0, adaptive=True, perfonly=False ):
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
                            recall, qps = threadwork( g, N, PROBES, entry, k=10, h=h, shw=shw, f=f, bw=bw, bc=bc, init=init, bmin=bmin, bmax=bmax, alpha=alpha, beta=beta, gamma=gamma, adaptive=adaptive, perfonly=perfonly )



def testrecall( MEM, Q, g, k=25, N=1500, P=None, entry="entry", show=True, r_result=None ):
    t0 = time.perf_counter()
    R = []
    T = []
    T_OVER = []
    cnt = 0
    tot_neval = 0
    tot_ncontribute = 0
    tot_nfrontier = 0
    tot_naccept = 0
    tot_nalready = 0
    tot_nvisited = 0
    fname = g[g[ROOT].Terminals()[0]]['fname']
    for p in P:
        cnt += 1
        r, t_ms = ptest(MEM, Q, g, p, k=k, root=entry, recall_only=1, recall_with_timing=1, fname=fname)
        R.append(r)
        T.append(t_ms)
        neval, ncontribute, nfrontier, naccept, nvisited, nalready = MEM.counters
        tot_neval += neval
        tot_ncontribute += ncontribute
        tot_nfrontier += nfrontier
        tot_naccept += naccept
        tot_nalready += nalready
        tot_nvisited += nvisited
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
            r_result['sum_evals'] = tot_neval
            r_result['sum_contributes'] = tot_ncontribute
            r_result['sum_frontiers'] = tot_nfrontier
            r_result['sum_accepts'] = tot_naccept
            r_result['sum_already'] = tot_nalready
            r_result['sum_visited'] = tot_nvisited
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

QMINIT(g)

    
MEM, Q = INIT(g)
 

PROBES100k = [ g.sim.NewVector(p, cosine_mode=1) for p in g['cache']['probes100k'] ]

SCAN_CACHE = g['cache']['SCAN_CACHE']

ENTRIES = ['entry']*len(PROBES) 


ROOT = "root-part1.dump"
medoid = g[ROOT].Terminals()[0] # 4fa8ff21-6154-4485-b539-8a1ebf8fac00



#if __name__ == "__main__":
#    fname = sys.argv[1]
#    run( fname )

#MEM, Q = INIT(g, h=10, shw=600, f=0, bw=3, bc=1.0, init=3, bmin=3, bmax=256, alpha=1/18, beta=0.0, gamma=0.0, adaptive=True )

#ptest( MEM, Q, g, PROBES100k[0], root='entry' )

#threadtest( g, 1, PROBES100k[:2000], 'entry', heaps=[10], shadows=[ int(2**(x/3)) for x in range(4,40) ], fronts=[0], beams=[3], bcs=[1.0], inits=[3], bmin=3, bmax=256, alpha=1/18, beta=1/180, gamma=1.0, adaptive=True, perfonly=1 )








