from cStringIO import StringIO

import types
import unittest

pred_map = {
   "eq" : "=",
   "ne" : "<>",
   "lt" : "<",
   "gt" : ">",
   "lte" : "<=",
   "gte" : ">="
}

ENGINE_SQLITE3 = 0
ENGINE_MONETDB = 1

def convert_to_sqlite3(value):
   t = type(value)
   if t in types.StringTypes:
      return "'%s'" % (value.replace("'", "''"))
   if t in (types.IntType, types.LongType, types.FloatType):
      return str(value)
   raise TypeError("Unable to convert '%s' to sqlite3 value." % type(value))

def convert_to_monetdb(value):
   t = type(value)
   if t in types.StringTypes:
      return "'%s'" % (value.replace("'", "''"))
   if t in (types.IntType, types.LongType, types.FloatType):
      return str(value)
   raise TypeError("Unable to convert '%s' to monetdb value." % type(value))

def convert_to_sql(value, engine):
   if engine==ENGINE_SQLITE3:
      return convert_to_sqlite3(value)
   else:
      return convert_to_monetdb(value)

class Expression(object):
   def __init__(self, left, op, right):
      self.left = left
      self.op = op
      self.right = right

   def gen(self, engine, **kw):
      s = StringIO()
      self.gen_simple_expression(engine, self.left, s, **kw)
      s.write(self.op)
      self.gen_simple_expression(engine, self.right, s, **kw)
      return s.getvalue()

   @classmethod
   def gen_simple_expression(cls, engine, value, stream, **kw):
      if isinstance(value, Expression):
         stream.write(value.gen(engine, **kw))
      else:
         stream.write(convert_to_sql(value, engine))

   def __or__(self, other):
      return BooleanExpression(self, "OR", other)

   def __and__(self, other):
      return BooleanExpression(self, "AND", other)


class BooleanExpression(Expression):
   def __init__(self, left, op, right):
      Expression.__init__(self, left, op, right)

   def gen(self, engine, **kw):
      s = StringIO()
      s.write("(")
      self.gen_simple_expression(engine, self.left, s, **kw)
      s.write(") ")
      s.write(self.op)
      s.write(" (")
      self.gen_simple_expression(engine, self.right, s, **kw)
      s.write(")")
      return s.getvalue()


class FieldNameExpression(Expression):
   def __init__(self, field):
      self.field = field

   def gen(self, engine, **kw):
      is_field = isinstance(self.field, Field)
      # If the field attached to this expression is an Alias we will use the alias value
      # from that object, otherwise we will use our given alias.
      alias = kw.get("alias", None) if is_field else self.field.alias
      name = self.field.name if is_field else self.field.field.name
      if alias is None:
         return name
      else:
         return alias+"."+name

class InExpression(Expression):
   def __init__(self, left, query_or_collection):
      if not isinstance(query_or_collection, SelectQuery) and\
         type(query_or_collection) not in (types.ListType, types.TupleType, type(set())):
         raise TypeError("To qualify for an in() operator, the parameter must be a SelectQuery or a simple Python collection like list or tuple.")

      self.left = left
      self.right = query_or_collection

   def gen(self, engine, **kw):
      o = StringIO()
      o.write(self.left.gen(engine, **kw))
      o.write(" IN (")
      if isinstance(self.right, SelectQuery):
         o.write(self.right.gen(engine, **kw))
      else:
         o.write(",".join([convert_to_sql(x,engine) for x in self.right]))
      o.write(")")
      return o.getvalue()

class BetweenExpression(Expression):
   def __init__(self, left, min_value, max_value):
      self.left = left
      self.min_value = min_value
      self.max_value = max_value

   def gen(self, engine, **kw):
      s = StringIO()
      s.write(self.left.gen(engine, **kw))
      s.write(" BETWEEN ")
      self.gen_simple_expression(engine, self.min_value, s, **kw)
      s.write(" AND ")
      self.gen_simple_expression(engine, self.max_value, s, **kw)
      return s.getvalue()

class CorrelatedExpression(Expression):
   def __init__(self, expr):
      self.expr = expr

   def gen(self, engine, **kw):
      s = StringIO()
      


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

class SelectQuery(object):
   def __init__(self, base_table):
      self.base_table = base_table
      self.alias = "_" + base_table.get_table_name() + str(id(self))
      self.fields = []
      self.expr = None
      self.joins = []
      self.outer_query = None

   def gen_sqlite3(self):
      o = StringIO()
      o.write("SELECT ")
      if not self.fields:
         o.write("* ")
      o.write("FROM ")
      o.write(self.base_table.get_table_name())
      o.write(" AS ")
      o.write(self.alias)
      for join_expr, local_alias in self.joins:
         o.write(" ")
         o.write(join_expr.gen_join_field_sqlite3(alias=self.alias,
                                                  local_alias=local_alias,
                                                  query=self,
                                                  outer_query=self.outer_query))
         o.write(" ")
      if self.expr is not None:
         o.write(" WHERE ")
         o.write(self.expr.gen(ENGINE_SQLITE3, alias=self.alias,
                                               query=self,
                                               outer_query=self.outer_query))
      return o.getvalue()

   def gen(self, engine, **kw):
      self.outer_query = kw.get("outer_query", None)
      if engine==ENGINE_SQLITE3:
         return self.gen_sqlite3()

   def where(self, expr):
      if self.expr is not None:
         self.expr = BooleanExpression(self.expr, "AND", expr)
      else:
         self.expr = expr

      return self

   def join(self, join_field):
      fields = self.base_table.get_fields()
      join_on = join_field
      if isinstance(join_field, Alias):
         join_field = join_on.field
         join_alias = join_on.alias
      else:
         join_alias = "_" + join_field.model.get_table_name() + str(id(self))

      for field in fields.values():
         if isinstance(field, OneToManyField):
            if field.other_model_field == join_field:
               self.joins.append((field, join_alias))
               return self

      raise ReferenceError()


class Model(object):
   @classmethod
   def get_fields(cls):
      if hasattr(cls, "_model_fields"):
         return getattr(cls, "_model_fields")

      fields = {}
      for attr in dir(cls):
         a = getattr(cls, attr)
         if isinstance(a, Field):
            fields[attr]=a
            a.set_name(attr)
            a.set_model(cls)

      setattr(cls, "_model_fields", fields)
      return fields

   @classmethod
   def get_table_name(cls):
      return cls.__name__.lower()

   @classmethod
   def gen_create_table_sqlite3(cls):
      o = StringIO()
      o.write("CREATE TABLE ")
      o.write(cls.get_table_name())
      o.write(" (")
      o.write(", ".join([field.gen_create_field_sqlite3() \
                          for field in cls.get_fields().values()]))
      o.write(")")
      return o.getvalue()

   @classmethod
   def gen_create_table_monetdb(cls):
      o = StringIO()
      o.write("CREATE TABLE ")
      o.write(cls.get_table_name())
      o.write(" (")
      o.write(", ".join([field.gen_create_field_monetdb() \
                          for field in cls.get_fields().values()]))
      o.write(")")
      return o.getvalue()

   @classmethod
   def gen_create_table(cls, engine):
      if engine==ENGINE_SQLITE3:
         return cls.gen_create_table_sqlite3()
      else:
         return cls.gen_create_table_monetdb()

   @classmethod
   def select(cls, **kw):
      return SelectQuery(cls)

   @classmethod
   def filter(cls, **kw):
      fields = cls.get_fields()
      predicates = []
      for arg, value in kw.iteritems():
         if "__" in arg:
            field, predicate = arg.split("__",1)

            # Check to see if the requested field exists.
            f = fields[field]

            if predicate=="between":
               predicates.append("%s BETWEEN %s AND %s" %\
                     (field,
                      convert_to_sqlite3(value[0]),
                      convert_to_sqlite3(value[1]))
               )
            else:
               predicates.append("%s%s%s" % (field, pred_map[predicate],
                                               convert_to_sqlite3(value)))
         else:
            if arg in fields:
               predicates.append("%s=%s" % (arg,convert_to_sqlite3(value)))

      return predicates

   @classmethod
   def new(cls, session=None, **kwargs):
      '''
      Create a new instance of a model to hold actual data.

      :param session: The session to connect to.
      :param kwargs: Initializers for each field, or other options.
      :return: A plain old data object initialized as requested and configured
               to have the same fields as the model.
      '''
      return Pod(session, cls, **kwargs)


class Pod(object):
   def __init__(self, session, model, **kwargs):
      self.session_ = session
      self.fields_ = model.get_fields()
      for field in self.fields_:
         setattr(self, field, kwargs.get(field))

   def load(self, row, col_spec):
      '''
      Loads this object from the row.

      :param row: The row of data to return.
      :param col_spec: A tuple containing the names of the columns in the same
                      order as the row.
      :return: None
      '''
      for i, v in enumerate(col_spec):
         setattr(self, v, row[i])

   def save(self):
      '''
      Saves this object to the persistent store.

      :return: None
      '''
      pass



class Alias(object):
   def __init__(self, field):
      self.field = field
      self.alias = "_" + field.model.get_table_name() + str(id(self))
      self.model_fields = field.model.get_fields()

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

   def __getattr__(self, item):
      """
      Proxy the access to the model.
      """
      if item in self.model_fields:
         return self
      else:
         return getattr(self.field.model, item)

if __name__ == "__main__":
   fi = IntegerField(name="test_int_field", default=5, required=True)
   print fi.gen_create_field_sqlite3()
   print fi.gen_create_field_monetdb()

   class Address(Model):
      id = IntegerField(required=True)
      addr_type = IntegerField()

   class Person(Model):
      id = IntegerField(required=True)
      first_name = StringField(required=True)
      last_name = StringField(required=True)
      age = IntegerField()
      address_id = OneToManyField(Address.id)

   tm = Person()
   am = Address()
   print tm.gen_create_table(ENGINE_SQLITE3)
   print tm.gen_create_table(ENGINE_MONETDB)
   print am.gen_create_table(ENGINE_SQLITE3)
   print am.gen_create_table(ENGINE_MONETDB)
   print tm.filter(first_name='jessica', last_name__ne='thweatt')
   print (Person.first_name == 'jessica').gen(ENGINE_SQLITE3)
   print ((Person.first_name == 'jessica') & (Person.last_name == 'nelson')).gen(ENGINE_SQLITE3)

   print Person.address_id.gen_join_field_sqlite3(alias="p1", local_alias="a1")
   print Person.select().where((Person.first_name == 'jessica') & (Person.last_name == 'nelson')).gen(ENGINE_SQLITE3)
   print Person.select()\
               .join(Address.id)\
               .where(Person.first_name == 'jessica').gen(ENGINE_SQLITE3)

   ma1 = Alias(Address.id)
   print Person.select()\
               .join(ma1)\
               .where((Person.first_name == 'jessica') & (ma1.addr_type == 1)).gen(ENGINE_SQLITE3)

   q1 = Person.select()\
               .join(Address.id)\
               .where(Person.first_name == 'jessica')
   print Person.select().where(Person.id.found_in(q1)).gen(ENGINE_SQLITE3)

   # Testing correlated queries which don't work yet.
   ma2 = Alias(Person.address_id)
   q2 = Address.select()\
               .where((Address.addr_type==1) &\
                      (ma2.address_id==Address.id))

   print ma2.select()\
               .where(Person.address_id.found_in(q2)).gen(ENGINE_SQLITE3)
   #############################################################################

   print dir(Person.new(first_name='jessica'))
