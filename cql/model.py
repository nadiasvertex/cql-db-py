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

def convert_to_monetdb(value):
   t = type(value)
   if t in types.StringTypes:
      return "'%s'" % (value.replace("'", "''"))
   if t in (types.IntType, types.LongType, types.FloatType):
      return str(value)

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
      if isinstance(self.left, Expression):
         s.write(self.left.gen(engine, **kw))
      else:
         s.write(convert_to_sql(self.left, engine))
      s.write(self.op)
      if isinstance(self.right, Expression):
         s.write(self.right.gen(engine, **kw))
      else:
         s.write(convert_to_sql(self.right, engine))

      return s.getvalue()

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
      if isinstance(self.left, Expression):
         s.write(self.left.gen(engine, **kw))
      else:
         s.write(convert_to_sql(self.left, engine))
      s.write(") ")
      s.write(self.op)
      s.write(" (")
      if isinstance(self.right, Expression):
         s.write(self.right.gen(engine, **kw))
      else:
         s.write(convert_to_sql(self.right, engine))
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

   def __eq__(self, other):
      return Expression(FieldNameExpression(self), "=", other)
   def __ne__(self, other):
      return Expression(FieldNameExpression(self), "<>", other)
   def __gt__(self, other):
      return Expression(FieldNameExpression(self), ">", other)
   def __lt__(self, other):
      return Expression(FieldNameExpression(self), "<", other)
   def __ge__(self, other):
      return Expression(FieldNameExpression(self), ">=", other)
   def __le__(self, other):
      return Expression(FieldNameExpression(self), "<=", other)
   def between(self, min_value, max_value):
      pass

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

   def convert_to_sql(self):
      return str(self.value)

   def gen_create_field_sqlite3(self):
      o = StringIO()
      o.write(self.name)
      o.write(" ")
      o.write(IntegerField.gen_type_sqlite3())
      self.gen_common_sqlite3(o)

      return o.getvalue()

   def gen_create_field_monetdb(self):
      o = StringIO()
      o.write(self.name)
      o.write(" ")
      o.write(IntegerField.gen_type_monetdb())
      self.gen_common_monetdb(o)

      return o.getvalue()

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

   @staticmethod
   def convert_to_python(value):
      return int(value)

   @staticmethod
   def convert_to_sql(value):
      return str(value)

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

   def gen_create_field_sqlite3(self):
      o = StringIO()
      o.write(self.name)
      o.write(" ")
      o.write(IntegerField.gen_type_sqlite3())
      self.gen_common_sqlite3(o)

      return o.getvalue()

   def gen_create_field_monetdb(self):
      o = StringIO()
      o.write(self.name)
      o.write(" ")
      o.write(IntegerField.gen_type_monetdb())
      self.gen_common_monetdb(o)

      return o.getvalue()

class SelectQuery(object):
   def __init__(self, base_table):
      self.base_table = base_table
      self.alias = "_" + base_table.get_table_name() + str(id(self))
      self.fields = []
      self.expr = None
      self.joins = []

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
                                                  local_alias=local_alias))
         o.write(" ")
      if self.expr is not None:
         o.write(" WHERE ")
         o.write(self.expr.gen(ENGINE_SQLITE3, alias=self.alias))
      return o.getvalue()

   def gen(self, engine):
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

   def gen_create_table_sqlite3(self):
      t = type(self)
      o = StringIO()
      o.write("CREATE TABLE ")
      o.write(t.get_table_name())
      o.write(" (")
      o.write(", ".join([field.gen_create_field_sqlite3() \
                          for field in type(self).get_fields().values()]))
      o.write(")")
      return o.getvalue()

   def __getattribute__(self, name):
      field = type(self).get_fields().get(name, None)
      if field:
         return field.convert_to_python()
      else:
         # Default behaviour
         return object.__getattribute__(self, name)

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

class Alias(object):
   def __init__(self, field):
      self.field = field
      self.alias = "_" + field.model.get_table_name() + str(id(self))
      self.model_fields = field.model.get_fields()

   def __eq__(self, other):
      return Expression(FieldNameExpression(self), "=", other)
   def __ne__(self, other):
      return Expression(FieldNameExpression(self), "<>", other)
   def __gt__(self, other):
      return Expression(FieldNameExpression(self), ">", other)
   def __lt__(self, other):
      return Expression(FieldNameExpression(self), "<", other)
   def __ge__(self, other):
      return Expression(FieldNameExpression(self), ">=", other)
   def __le__(self, other):
      return Expression(FieldNameExpression(self), "<=", other)

   def __getattr__(self, item):
      """
      Proxy the access to the model.
      """
      if item in self.model_fields:
         return self
      raise AttributeError("%s is not a field in the %s model." % (item, self.field.model.get_table_name()))

if __name__ == "__main__":
   fi = IntegerField(name="test_int_field", default=5, required=True)
   print fi.gen_create_field_sqlite3()
   print fi.gen_create_field_monetdb()

   class Address(Model):
      id = IntegerField(required=True)
      addr_type = IntegerField()

   class Person(Model):
      first_name = IntegerField(required=True)
      last_name = IntegerField(required=True)
      age = IntegerField()
      address_id = OneToManyField(Address.id)

   tm = Person()
   am = Address()
   print tm.gen_create_table_sqlite3()
   print am.gen_create_table_sqlite3()
   print tm.filter(first_name=5, last_name__ne=10)
   print (Person.first_name == 5).gen(ENGINE_SQLITE3)
   print ((Person.first_name == 5) & (Person.last_name == 10)).gen(ENGINE_SQLITE3)

   print Person.address_id.gen_join_field_sqlite3(alias="p1", local_alias="a1")
   print Person.select().where((Person.first_name == 5) & (Person.last_name == 10)).gen(ENGINE_SQLITE3)
   print Person.select()\
               .join(Address.id)\
               .where(Person.first_name == 5).gen(ENGINE_SQLITE3)

   ma1 = Alias(Address.id)
   print Person.select()\
               .join(ma1)\
               .where((Person.first_name == 5) & (ma1.addr_type == 1)).gen(ENGINE_SQLITE3)