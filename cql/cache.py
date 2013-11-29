import sqlite3

class Cache(object):
   """
   This object manages the cache. All data is initially written into the cache.
   Over time, in the background, data is migrated into the persistent store.
   """
   def __init__(self, database, username, password):
      self.persistent_store = database.connect()
      self.cache = sqlite3.connect(":memory:")
      self._create_schema()

   def __del__(self):
      self.flush()

   def _create_schema(self):
      from model import ModelMeta
      from common import ENGINE_SQLITE3
      for m in ModelMeta.models.values():
         sql = m.gen_create_table(ENGINE_SQLITE3)
         self.cache.execute(sql)

   def flush(self):
      self.move()

   def move(self, count=-1):
      pass

   def execute(self, sql):
      self.cache.execute(sql)

## ==---------- Tests ------------------------------------------------------------------------------------------------==
if __name__ == "__main__":
   import unittest
   from common import ENGINE_SQLITE3
   from field import IntegerField, StringField, OneToManyField
   from model import Model
   from store import Database, Warehouse

   class Address(Model):
         addr_type = IntegerField()

   class Person(Model):
      first_name = StringField(required=True)
      last_name = StringField(required=True)
      age = IntegerField()
      address_id = OneToManyField(Address.address_id)

   class TestCache(unittest.TestCase):
      def setUp(self):
         self.w = Warehouse("/tmp/test_warehouse", "letmein", port=60001)
         self.db = Database(self.w, "testdb")
         self.cache = Cache(self.db, "test", "pass")

      def tearDown(self):
         self.db.stop()
         self.db.destroy()
         self.w.stop()
         self.w.destroy()


      def testCreateSchema(self):
         # The following SQL should not throw.
         self.cache.execute("SELECT * FROM person;")
         self.cache.execute("SELECT * FROM address;")

      def testSaveDataInCache(self):
         am = Address.new(self.cache)
         am.addr_type = 10
         am.save()


   unittest.main()
