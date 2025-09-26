###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    INTERNAL_ast_validator.py
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

import pyvgx
import ast

class Validator( ast.NodeTransformer ):

    ALLOWED_NODE_CLASSES = set([
        ast.Add,
        ast.And,
            #ast.AnnAssign,
            #ast.Assert,
            #ast.Assign,
            #ast.AsyncFor,
            #ast.AsyncFunctionDef,
            #ast.AsyncWith,
        ast.Attribute,
            #ast.AugAssign,
        ast.AugLoad,
        ast.AugStore,
            #ast.Await,
        ast.BinOp,
        ast.BitAnd,
        ast.BitOr,
        ast.BitXor,
        ast.BoolOp,
            #ast.Break,
        ast.Call,
            #ast.ClassDef,
        ast.Compare,
        ast.Constant,
            #ast.Continue,
            #ast.Del,
            #ast.Delete,
        ast.Dict,
        ast.DictComp,
        ast.Div,
            #ast.Ellipsis,
        ast.Eq,
            #ast.ExceptHandler,
        ast.Expr,
        ast.Expression,
        ast.ExtSlice,
        ast.FloorDiv,
            #ast.For,
        ast.FormattedValue,
            #ast.FunctionDef,
        ast.FunctionType,
        ast.GeneratorExp,
        ast.Global,
        ast.Gt,
        ast.GtE,
            #ast.If,
        ast.IfExp,
            #ast.Import,
            #ast.ImportFrom,
        ast.In,
        ast.Index,
            #ast.Interactive,
        ast.Invert,
        ast.Is,
        ast.IsNot,
        ast.JoinedStr,
        ast.LShift,
        ast.Lambda,
        ast.List,
        ast.ListComp,
        ast.Load,
        ast.Lt,
        ast.LtE,
        ast.MatMult,
        ast.Mod,
            #ast.Module,
        ast.Mult,
        ast.Name,
            #ast.NamedExpr,
            #ast.NodeTransformer,
            #ast.NodeVisitor,
            #ast.Nonlocal,
        ast.Not,
        ast.NotEq,
        ast.NotIn,
        ast.Or,
        ast.Param,
            #ast.Pass,
        ast.Pow,
            #ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
            #ast.PyCF_ONLY_AST,
            #ast.PyCF_TYPE_COMMENTS,
        ast.RShift,
            #ast.Raise,
            #ast.Return,
        ast.Set,
        ast.SetComp,
        ast.Slice,
        ast.Starred,
        ast.Store,
        ast.Sub,
        ast.Subscript,
            #ast.Suite,
            #ast.Try,
        ast.Tuple,
            #ast.TypeIgnore,
        ast.UAdd,
        ast.USub,
        ast.UnaryOp,
            #ast.While,
            #ast.With,
            #ast.Yield,
            #ast.YieldFrom,
            #ast.alias,
        ast.arg,
        ast.arguments,
            #ast.auto,
            #ast.boolop,
            #ast.cmpop,
        ast.comprehension,
            #ast.contextmanager,
            #ast.copy_location,
            #ast.dump,
            #ast.excepthandler,
            #ast.expr,
            #ast.expr_context,
            #ast.fix_missing_locations,
            #ast.get_docstring,
            #ast.get_source_segment,
            #ast.increment_lineno,
            #ast.iter_child_nodes,
            #ast.iter_fields,
        ast.keyword,
            #ast.literal_eval,
            #ast.main,
            #ast.mod,
            #ast.nullcontext,
            #ast.operator,
            #ast.parse,
            #ast.slice,
            #ast.stmt,
            #ast.sys,
            #ast.type_ignore,
            #ast.unaryop,
            #ast.unparse,
            #ast.walk,
            #ast.withitem
        ])

    ALLOWED_BUILTIN_FUNCTIONS = set([
            #'__import__',
        'abs',
        'all',
        'any',
        'ascii',
        'bin',
        'bool',
            #'breakpoint',
        'bytearray',
        'bytes',
            #'callable',
        'chr',
            #'classmethod',
            #'compile',
        'complex',
            #'delattr',
        'dict',
        'dir',
        'divmod',
        'enumerate',
            #'eval',
            #'exec',
        'filter',
        'float',
        'format',
        'frozenset',
        'getattr',
            #'globals',
        'hasattr',
        'hash',
            #'help',
        'hex',
            #'id',
            #'input',
        'int',
        'isinstance',
        'issubclass',
        'iter',
        'len',
        'list',
            #'locals',
        'map',
        'max',
            #'memoryview',
        'min',
        'next',
            #'object',
        'oct',
            #'open',
        'ord',
        'pow',
            #'print',
            #'property',
        'range',
        'repr',
        'reversed',
        'round',
        'set',
            #'setattr',
        'slice',
        'sorted',
            #'staticmethod',
        'str',
        'sum',
            #'super',
        'tuple',
        'type',
            #'vars',
        'zip'
    ])

    FORBIDDEN = set([
        '__import__',
        'breakpoint',
        'callable',
        'classmethod',
        'compile',
        'delattr',
        'eval',
        'exec',
        'globals',
        'help',
        'id',
        'input',
        'locals',
        'memoryview',
        'object',
        'open',
        'print',
        'property',
        'setattr',
        'staticmethod',
        'super',
        'vars',
        'AddPlugin'
    ])

    EVAL_GLOBALS = dict( pyvgx=pyvgx )

    def visit_Name( self, node ):
        if node.id in self.FORBIDDEN or node.id.startswith("__"):
            raise SyntaxError( "Forbidden name: %s" % node.id )
        try:
            return ast.copy_location( ast.Constant( getattr( pyvgx, node.id ) ), node )
        except Exception as err:
            return node
            

    def visit_Call( self, node ):
        func = node.func
        if type(func) is ast.Name and func.id in self.ALLOWED_BUILTIN_FUNCTIONS:
            return node
        else:
            obj = func
            while type(obj) in [ast.Attribute, ast.Call]:
                if type(obj) is ast.Call:
                    obj = obj.func
                else:
                    if obj.attr in self.FORBIDDEN:
                        break
                    obj = obj.value
            if type(obj) is ast.Name and obj.id == "pyvgx":
                return node
        name = None
        if type(obj) is ast.Name:
            name = obj.id
        elif type(obj) is ast.Attribute:
            name = obj.attr
        raise SyntaxError( "Forbidden call: %s()" % (name if name else "<func>") )
          

    def generic_visit( self, node ):
        if node.__class__ not in self.ALLOWED_NODE_CLASSES:
            raise SyntaxError( "'%s' not allowed" % type(node).__name__ )
        return ast.NodeTransformer.generic_visit( self, node )


    def SafeEval( self, expr ):
        if type(expr) is str :
            tree = ast.parse( expr, mode='eval' )
            self.visit( tree )
            code = compile( tree, '<AST>', 'eval' )
            return eval( code, self.EVAL_GLOBALS )
        else:
            return expr

sysplugin__VALIDATOR = Validator()

def sysplugin__UpdateQueryDict( Q, http_params=None, http_content=None ):
    data = []
    if http_params:
        data.append( http_params )
    if http_content:
        if type( http_content ) is memoryview:
            http_content = http_content.tobytes()
        if type( http_content ) is bytes:
            http_content = http_content.decode()
        if type( http_content ) is str:
            data.append( http_content )

    for P in data:
        try:
            Q.update( sysplugin__VALIDATOR.SafeEval( P ) )
        except Exception as ex:
            norm = re.sub( r"\s+", " ", P )
            raise ValueError( 'Bad query: %s (original exception=%s)' % (norm, ex) )

    return Q

def sysplugin__GetGraphObject( graph_name ):
    try:
        return pyvgx.system.GetGraph( graph_name ), False
    except:
        if graph_name in pyvgx.system.Registry():
            return pyvgx.Graph( graph_name ), True
        else:
            raise
