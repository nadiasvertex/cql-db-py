from cStringIO import StringIO
from expression import *
from field import *
from common import convert_to_sql, ENGINE_MONETDB, ENGINE_SQLITE3
from query import SelectQuery

import unittest

def _next_id(start=0):
   i = start
   while True:
      yield i
      i+=1

class ModelMeta(type):
   models = {}

   def __new__(cls, name, bases, attributes, **kwargs):
      fields = {}
      for attr_name, member in attributes.iteritems():
         if isinstance(member, Field):
            fields[attr_name]=member
            member.set_name(attr_name)

      # Check to make sure we have a primary key field
      pk = name.lower() + "_id"
      if pk not in fields:
         pk_field = IntegerField(required=True)
         pk_field.set_name(pk)
         pk_field.set_model(cls)
         fields[pk] = pk_field
         attributes[pk] = pk_field

      # Cache the data fields dictionary.
      attributes["_model_fields"] = fields

      # Instantiate the metaclass (producing a class.)
      new_class = super(ModelMeta, cls).__new__(cls, name, bases, attributes, **kwargs)

      # Update the model members with a link to their model class.
      for member in fields.values():
         member.set_model(new_class)

      # Remember the model for initialization
      cls.models[name] = new_class

      return new_class

class Model(object):
   __metaclass__ = ModelMeta

   pk_seq_ = _next_id()

   @classmethod
   def get_fields(cls):
      return cls._model_fields

   @classmethod
   def get_table_name(cls):
      return cls.__name__.lower()

   @classmethod
   def get_primary_key_name(cls):
      return cls.get_table_name() + "_id"

   @classmethod
   def get_primary_key_field(cls):
      return getattr(cls, cls.get_primary_key_name())

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
      o.write(convert_to_sql(getattr(pod, pod.model_.get_primary_key_name()), ENGINE_SQLITE3))
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
   def select(cls, cache=None, **kw):
      return SelectQuery(cls, cache=cache)

   @classmethod
   def new(cls, cache=None, **kwargs):
      '''
      Create a new instance of a model to hold actual data.

      :param cache: The cache object to use.
      :param kwargs: Initializers for each field, or other options.
      :return: A plain old data object initialized as requested and configured
               to have the same fields as the model.
      '''
      return Pod(cache, cls, **kwargs)

   @classmethod
   def save(cls, pod):
      pk = cls.get_primary_key_name()
      if getattr(pod, pk) is None:
         setattr(pod, pk, cls.pk_seq_.next())

      sql = cls.gen_insert_sqlite3(pod)
      pod.cache_.execute(sql)

class Pod(object):
   def __init__(self, cache, model, **kwargs):
      self.cache_ = cache
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
      self.model = field.model
      self.alias = "_" + self.model.get_table_name() + str(id(self))
      self.model_fields = self.model.get_fields()

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
         return getattr(self.model, item)

## ==---------- Tests ------------------------------------------------------------------------------------------------==
if __name__ == "__main__":
   class Address(Model):
         addr_type = IntegerField()

   class Person(Model):
      first_name = StringField(required=True)
      last_name = StringField(required=True)
      age = IntegerField()
      address_id = OneToManyField(Address.address_id)

   class TestModel(unittest.TestCase):
      def setUp(self):
         self.tm = Person()
         self.am = Address()

      def testCreatePersonTableSqlite3(self):
         sql = self.tm.gen_create_table(ENGINE_SQLITE3)

      def testCreatePersonTableMonetDb(self):
         sql = self.tm.gen_create_table(ENGINE_MONETDB)

      def testCreateAddressTableSqlite3(self):
         sql = self.am.gen_create_table(ENGINE_SQLITE3)

      def testCreateAddressTableMonetDb(self):
         sql = self.am.gen_create_table(ENGINE_MONETDB)

      def testAlias(self):
         ma1 = Alias(Address.address_id)
         sql = Person.select()\
                  .join(ma1)\
                  .where((Person.first_name == 'jessica') & (ma1.addr_type == 1)).gen(ENGINE_SQLITE3)

      def testInsertModel(self):
         p1 = Person.new(first_name='jessica')
         sql = Person.gen_insert_sqlite3(p1)

      def testUpdateModel(self):
         p1 = Person.new(first_name='jessica')
         p1.save()
         p1.first_name = None
         p1.last_name = 'nelson'
         self.assertNotEquals(0, len(p1.dirty_))
         sql = Person.gen_save(ENGINE_SQLITE3, p1)

   unittest.main()
