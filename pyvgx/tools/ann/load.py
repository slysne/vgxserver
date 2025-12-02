from pyvgx import *
import json
import sys
import random
import time
import struct
import base64


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
    M_FIND = g.Memory(4)
    Q_FIND = g.NewNeighborhoodQuery(
                memory  =   M_FIND,
                arc     =   D_OUT,
                filter  =   "anncollect()",
                collect =   C_SCAN,
                sortby  =   S_VAL,
                fields  =   F_VAL | F_ID,
                result  =   R_LIST,
                recursion = {
                    'heap_size'         : 150,
                    'beam_width'        : 150,
                    'beam_curve'        : 0.8,
                    'reset_state'       : False
                }
    )




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
        



def prune_RNG_neighborhood(g, A):
    C = g.Neighborhood(A, fields=F_VAL|F_ID, result=R_LIST, sortby=S_VAL)
    g.Disconnect( A, D_OUT )
    M_PRUNE.Reset()
    for asim, c in C:
        if c == A.id: # Fix this in the query generating C
            continue
        B = g.OpenVertex(c)
        Q_PRUNE.id = A
        M_PRUNE.R1 = B.GetVector() # WARNING!!! This accumulates vector objects until we Reset() the mem object!!!
        # c too close to existing neighbor of, skip
        if Q_PRUNE.Execute():
            B.Close()
            continue
        # c accepted
        g.Connect( A, ('asim', M_FLT|M_FWDONLY, asim), B )
        B.Close()
        if A.odeg >= 32:
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
        M_PRUNE.R1 = B.GetVector() # WARNING!!! This accumulates vector objects until we Reset() the mem object!!!
        # c too close to existing neighbor of, skip
        if Q_PRUNE.Execute():
            B.Close()
            continue
        # c accepted
        asim = 1.1 * (score-1) # alpha * (cos+1-1)
        g.Connect( A, ('asim', M_FLT|M_FWDONLY, asim), B )
        g.Connect( B, ('asim', M_FLT|M_FWDONLY, asim), A )
        if B.odeg > 64:
            prune_RNG_neighborhood(g, B)
        B.Close()
        if A.odeg >= 32:
            return




def find_candidates(g, A, roots):
    C = []
    M_FIND.Reset()
    M_FIND.R1 = A.GetVector() # WARNING!!! This accumulates vector objects until we Reset() the mem object!!!
    M_FIND.R4 = 1.1 # ham filter score activation
    #M_FIND.ClearSet()
    for root in roots:
        M_FIND.R2 = 0.0 # min score threshold init
        Q_FIND.id = root
        C.extend( Q_FIND.Execute( hits=32 ) ) # Keep the vset since reset_state is True
    return sorted(C, reverse=1)



def process_node(g, node, medoid, random_roots):
    A = g.OpenVertex(node)
    probe = A.GetVector()
    selected_roots = [medoid]
    selected_roots.extend( random.sample( random_roots, 4 ) )
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
            random_roots = get_random_roots(g, 128)
            refresh_roots_at_n += N//10
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









system.Initialize( "annindex", http=9000 )
g = Graph("ann")
QMINIT(g)


#if __name__ == "__main__":
#    fname = sys.argv[1]
#    run( fname )










