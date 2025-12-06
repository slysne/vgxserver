


from pyvgx import *
g=Graph("test")

M = g.Memory(4)
Q = g.NewNeighborhoodQuery( memory=M, sortby=S_VAL, collect=C_SCAN, filter="require(random()>0.75); collect()", recursion={'depth_limit':3, 'reset_state':False} )
QA = g.NewAdjacencyQuery( filter="random()>0.75" )


nodes = g.Vertices()
M.Reset();g.Neighborhood( nodes[0], hits=5, memory=M, sortby=S_VAL, fields=F_ID|F_VAL, result=R_LIST, collect=C_SCAN, filter="anncollect()", recursion={'depth_limit':3, 'beam_width':100 } )


M.R1 = g[nodes[1234]].GetVector()
M.R2=0.0;M.ClearSet();g.Neighborhood( nodes[0], hits=5, memory=M, sortby=S_VAL, fields=F_ID|F_VAL|F_DEPTH, result=R_LIST, collect=C_SCAN, filter="anncollect()", recursion={'heap_shadow': 512, 'depth_limit':300, 'beam_width':100 } )


def make(g, N=10000, R=32):
    names = [f"v_{n:05d}" for n in range(N)]
    for node in names:
        A = g.NewVertex( node )
        A.SetVector( g.sim.rvec(128) )
        A.Close()
    n = 0
    for init in names:
        n += 1
        A = g.OpenVertex(init)
        T = g.OpenVertices( [term for term in random.sample(names,R)] )
        for term in T:
            lsh32 = term.GetVector().LSH32()
            g.Connect(A, ("lsh32",M_LSH|M_FWDONLY,lsh32), term)
        g.CloseVertices(T)
        A.Close()
        if not n % 100:
            print( f"{n}  ", end="\r",  flush=1 )
    print( f"{n}  " )
            



def check(g, i=0, msg=""):
    try:
        g.CloseVertices( g.OpenVertices(g.Vertices()) )
    except Exception as err:
        raise Exception( f"Lock error at iteration {i}: {msg} (Original exception:{err})" )


def find(g, A, nroots=10):
    M.Reset()
    C = []
    for root in random.sample( g.Vertices(), nroots ):
        #print( f"find: root={root}")
        Q.id = root
        C.extend( Q.Execute( hits=25 ) )
    return sorted(C, reverse=1)


def connect(g, A, C):
    if A.odeg > 0:
        g.Disconnect(A, D_OUT)
    for node in C:
        #print( f"connect: node={node}")
        if node == A.id:
            continue
        B = g.OpenVertex(node)
        QA.id = A
        if QA.Execute():
            B.Close()
            continue
        g.Connect( A, ("to",M_FLT|M_FWDONLY,random.random()), B )
        B.Close()

def process(g, node):
    A = g.OpenVertex(node)
    C = find(g, A)
    #check(g, 0, "Just after find()")
    connect(g, A, C)
    A.Close()





