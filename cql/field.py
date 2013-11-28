__author__ = 'cnelson'

from cStringIO import StringIO
from common import convert_to_sqlite3, convert_to_monetdb
from expression import *

import types

class Field(object):
   def __init__(self, **kw):
      self.name = kw.get("name", None)
      self.model = kw.get("model", None)
      self.is_required = kw.get("required", False)
      self.is_unique = kw.get("unique", False)
      self.default_value = kw.get("default", None)
      self.value = None

      t = type(self.default_value)
      if t==types.FunctionType:
         self.has_compatible_default_constraint = False
      else:
         self.has_compatible_default_constraint = True

   def set_name(self, name):
      self.name = name

   def set_model(self, model):
      self.model = model

   def gen_common_sqlite3(self, o):
      if self.is_unique:
         o.write(" UNIQUE")
      if self.is_required:
         o.write(" NOT NULL")
      if self.default_value is not None and \
         self.has_compatible_default_constraint:
         o.write(" DEFAULT ")
         o.write(convert_to_sqlite3(self.default_value))

   def gen_common_monetdb(self, o):
      if self.is_unique:
         o.write(" UNIQUE")
      if self.is_required:
         o.write(" NOT NULL")
      if self.default_value is not None and \
         self.has_compatible_default_constraint:
         o.write(" DEFAULT ")
         o.write(convert_to_monetdb(self.default_value))

   def gen_create_field_sqlite3(self):
      o = StringIO()
      o.write(self.name)
      o.write(" ")
      o.write(self.gen_type_sqlite3())
      self.gen_common_sqlite3(o)

      return o.getvalue()

   def gen_create_field_monetdb(self):
      o = StringIO()
      o.write(self.name)
      o.write(" ")
      o.write(self.gen_type_monetdb())
      self.gen_common_monetdb(o)

      return o.getvalue()

   def _wrap_expr(self, e):
      if isinstance(e, Field):
         return FieldNameExpression(e)
      return e
   def __eq__(self, other):
      return Expression(FieldNameExpression(self), "=", self._wrap_expr(other))
   def __ne__(self, other):
      return Expression(FieldNameExpression(self), "<>", self._wrap_expr(other))
   def __gt__(self, other):
      return Expression(FieldNameExpression(self), ">", self._wrap_expr(other))
   def __lt__(self, other):
      return Expression(FieldNameExpression(self), "<", self._wrap_expr(other))
   def __ge__(self, other):
      return Expression(FieldNameExpression(self), ">=", self._wrap_expr(other))
   def __le__(self, other):
      return Expression(FieldNameExpression(self), "<=", self._wrap_expr(other))
   def between(self, min_value, max_value):
      return BetweenExpression(FieldNameExpression(self), self._wrap_expr(
         min_value), self._wrap_expr(max_value))
   def found_in(self, expr):
      return InExpression(FieldNameExpression(self), expr)

class IntegerField(Field):
   def __init__(self, **kw):
      Field.__init__(self, **kw)

   @staticmethod
   def gen_type_sqlite3():
      return "INTEGER"

   @staticmethod
   def gen_type_monetdb():
      return "BIGINT"

   def convert_to_python(self):
      return int(self.value)

class StringField(Field):
   def __init__(self, **kw):
      Field.__init__(self, **kw)

   @staticmethod
   def gen_type_sqlite3():
      return "TEXT"

   @staticmethod
   def gen_type_monetdb():
      return "STRING"

   def convert_to_python(self):
      return str(self.value)

class FloatField(Field):
   def __init__(self, **kw):
      Field.__init__(self, **kw)

   @staticmethod
   def gen_type_sqlite3():
      return "REAL"

   @staticmethod
   def gen_type_monetdb():
      return "FLOAT"

   def convert_to_python(self):
      return float(self.value)


class OneToManyField(Field):
   def __init__(self, other_model_field, **kw):
      Field.__init__(self, **kw)
      self.other_model_field = other_model_field

   @staticmethod
   def gen_type_sqlite3():
      return "INTEGER"

   @staticmethod
   def gen_type_monetdb():
      return "BIGINT"

   def convert_to_python(self):
      return int(self.value)

   def gen_join_field_sqlite3(self, **kw):
      alias = kw.get("alias", None)
      local_alias = kw.get("local_alias", None)
      o = StringIO()
      o.write("INNER JOIN ")
      o.write(self.other_model_field.model.get_table_name())
      if local_alias:
         o.write(" AS ")
         o.write(local_alias)
      o.write(" ON ")
      if alias:
         o.write(alias)
         o.write(".")
      o.write(self.name)
      o.write("=")
      if local_alias:
         o.write(local_alias)
         o.write(".")
      o.write(self.other_model_field.name)
      return o.getvalue()