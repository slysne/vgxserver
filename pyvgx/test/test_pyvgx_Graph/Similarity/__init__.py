import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import RunModules


from . import NewVector
from . import VertexVector
from . import GetSimilarity
from . import Similarity
from . import Cosine
from . import Jaccard
from . import HammingDistance


modules = [
  NewVector,
  VertexVector,
  GetSimilarity,
  Similarity,
  Cosine,
  Jaccard,
  HammingDistance
]


def Run( g ):
    RunModules( modules, g )
