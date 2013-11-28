from cStringIO import StringIO
from expression import *
from field import *
from common import convert_to_sql, ENGINE_MONETDB, ENGINE_SQLITE3
from query import SelectQuery

import unittest


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
   def gen_insert_sqlite3(cls, pod):
      # First get all of the data fields in the pod.
      field_names = pod.fields_.keys()
      # Next get their values.
      values = [getattr(pod, field) for field in field_names]
      # Finally, create a dictionary mapping the fields to the values, where the values have been set to
      # a real value.
      valued_tuples = { e[0] : e[1] for e in zip(field_names, values) if e[1] is not None}
      o = StringIO()
      o.write("INSERT INTO ")
      o.write(cls.get_table_name())
      o.write("(")
      o.write(",".join(valued_tuples.keys()))
      o.write(") VALUES(")
      o.write(",".join([convert_to_sql(value, ENGINE_SQLITE3) for value in valued_tuples.values()]))
      o.write(")")
      return o.getvalue()

   @classmethod
   def gen_update_sqlite3(cls, pod):
      # First get all of the data fields in the pod.
      field_names = pod.fields_.keys()
      # Next get their values.
      values = [getattr(pod, field) for field in field_names]
      # Finally, create a dictionary mapping the fields to the values, where the values have been set to
      # a real value.
      valued_tuples = { e[0] : e[1] for e in zip(field_names, values) if (e[1] is not None) or (e[0] in pod.dirty_)}
      o = StringIO()
      o.write("UPDATE ")
      o.write(cls.get_table_name())
      o.write(" SET ")
      o.write(" ".join([key + "=" + convert_to_sql(value, ENGINE_SQLITE3) for key,value in valued_tuples.iteritems()]))
      o.write(" WHERE id=")
      o.write(convert_to_sql(pod.id, ENGINE_SQLITE3))
      return o.getvalue()

   @classmethod
   def gen_save(cls, engine, pod):
      if engine==ENGINE_SQLITE3:
         if pod.persisted_:
            return cls.gen_update_sqlite3(pod)
         else:
            return cls.gen_insert_sqlite3(pod)

      else:
         if pod.persisted_:
            return cls.gen_update_monetdb(pod)
         else:
            return cls.gen_insert_monetdb(pod)


   @classmethod
   def select(cls, **kw):
      return SelectQuery(cls)

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

   @classmethod
   def save(cls, pod):
      sql = cls.gen_insert_sqlite3(pod)


class Pod(object):
   def __init__(self, session, model, **kwargs):
      self.session_ = session
      self.fields_ = model.get_fields()
      self.model_ = model
      self.dirty_ = {}
      self.persisted_ = kwargs.get("persisted", False)
      for field in self.fields_:
         setattr(self, field, kwargs.get(field))

   def __setattr__(self, key, value):
      if hasattr(self, "persisted_") and\
         self.persisted_ and\
         key in self.fields_ and\
         key not in self.dirty_:
         self.dirty_[key] = getattr(self, key)

      object.__setattr__(self, key, value)

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
      self.model_.save(self)
      self.persisted_ = True


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

   p1 = Person.new(first_name='jessica')
   print Person.gen_insert_sqlite3(p1)

   p1.save()
   p1.first_name = None
   p1.last_name = 'nelson'
   print p1.dirty_
   print Person.gen_save(ENGINE_SQLITE3, p1)
