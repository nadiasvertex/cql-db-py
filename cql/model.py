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

class Expression(object):
   def __init__(self, left, op, right):
      self.left = left
      self.op = op
      self.right = right

   def gen(self):
      s = StringIO()
      if isinstance(self.left, Expression):
         s.write(self.left.gen())
      else:
         s.write(convert_to_sqlite3(self.left))
      s.write(self.op)
      if isinstance(self.right, Expression):
         s.write(self.right.gen())
      else:
         s.write(convert_to_sqlite3(self.right))

      return s.getvalue()

class FieldNameExpression(Expression):
   def __init__(self, name):
      self.name = name
   def gen(self):
      return self.name

class Field(object):
   def __init__(self, **kw):
      self.name = kw.get("name", None)
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
      return Expression(FieldNameExpression(self.name), "=", other)
   def __ne__(self, other):
      return Expression(FieldNameExpression(self.name), "<>", other)
   def __gt__(self, other):
      return Expression(FieldNameExpression(self.name), ">", other)
   def __lt__(self, other):
      return Expression(FieldNameExpression(self.name), "<", other)
   def __ge__(self, other):
      return Expression(FieldNameExpression(self.name), ">=", other)
   def __le__(self, other):
      return Expression(FieldNameExpression(self.name), "<=", other)
   def __or__(self, other):
      




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
   def __init__(self, other_model, **kw):
      Field.__init__(self, **kw)
      self.other_table = other_model

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

      setattr(cls, "_model_fields", fields)
      return fields

   def get_table_name(self):
      return type(self).__name__.lower()

   def gen_create_table_sqlite3(self):
      o = StringIO()
      o.write("CREATE TABLE ")
      o.write(self.get_table_name())
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


if __name__ == "__main__":
   fi = IntegerField(name="test_int_field", default=5, required=True)
   print fi.gen_create_field_sqlite3()
   print fi.gen_create_field_monetdb()

   class Person(Model):
      first_name = IntegerField(required=True)
      last_name = IntegerField(required=True)
      age = IntegerField()

   tm = Person()
   print tm.gen_create_table_sqlite3()
   print tm.filter(first_name=5, last_name__ne=10)
   print (Person.first_name == 5).gen()
