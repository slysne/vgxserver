import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets


import pyvgx

from . import SetProperty
from . import IncProperty
from . import HasProperty
from . import GetProperty
from . import RemoveProperty
from . import SetProperties
from . import HasProperties
from . import NumProperties
from . import GetProperties
from . import RemoveProperties
from . import dict_items
from . import dict_keys
from . import dict_values


modules = [
  SetProperty,
  IncProperty,
  HasProperty,
  GetProperty,
  RemoveProperty,
  SetProperties,
  HasProperties,
  NumProperties,
  GetProperties,
  RemoveProperties,
  dict_items,
  dict_keys,
  dict_values
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )
