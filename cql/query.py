__author__ = 'cnelson'

from cStringIO import StringIO
from common import ENGINE_SQLITE3, ENGINE_MONETDB
import unittest

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
      from expression import BooleanExpression

      if self.expr is not None:
         self.expr = BooleanExpression(self.expr, "AND", expr)
      else:
         self.expr = expr

      return self

   def join(self, join_field):
      from model import Alias
      from field import OneToManyField

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


if __name__ == "__main__":
   from model import Model
   from field import IntegerField, StringField, OneToManyField
   from common import ENGINE_SQLITE3, ENGINE_MONETDB

   class Address(Model):
         addr_type = IntegerField()

   class Person(Model):
      first_name = StringField(required=True)
      last_name = StringField(required=True)
      age = IntegerField()
      address_id = OneToManyField(Address.address_id)

   class TestSelect(unittest.TestCase):

      def setUp(self):
         self.tm = Person()
         self.am = Address()

      def testGenerateJoinField(self):
         sql = Person.address_id.gen_join_field_sqlite3(alias="p1", local_alias="a1")

      def testGenerateSelectWithPredicates(self):
         sql = Person.select()\
                                .where((Person.first_name == 'jessica') &
                                       (Person.last_name == 'nelson'))\
                                .gen(ENGINE_SQLITE3)

      def testGenerateJoin(self):
         sql = Person.select()\
                     .join(Address.address_id)\
                     .where(Person.first_name == 'jessica').gen(ENGINE_SQLITE3)

      def testGenerateSubselect(self):
         q1 = Person.select()\
                     .join(Address.address_id)\
                     .where(Person.first_name == 'jessica')
         sql = Person.select().where(Person.person_id.found_in(q1)).gen(ENGINE_SQLITE3)


   unittest.main()