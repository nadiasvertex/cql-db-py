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

   def _get_persistent_store_tables(self):
      cur = self.persistent_store.cursor()
      cur.execute("SELECT name, type FROM sys.tables WHERE system=false;")
      tables = cur.fetchall()
      cur.close()
      return [row[0] for row in tables]

   def _create_schema(self):
      from model import ModelMeta
      from common import ENGINE_SQLITE3, ENGINE_MONETDB

      cur = self.persistent_store.cursor()
      persistent_tables = set(self._get_persistent_store_tables())
      for n, m in ModelMeta.models.iteritems():
         # Always create the table in the cache, since the cache disappears when closed.
         sql_1 = m.gen_create_table(ENGINE_SQLITE3)
         self.cache.execute(sql_1)

         # Only create the schema if it doesn't exist already.
         if not n in persistent_tables:
            sql_2 = m.gen_create_table(ENGINE_MONETDB)
            cur.execute(sql_2)

      cur.close()


   def flush(self):
      self.move()

   def move(self, count=-1):
      from model import ModelMeta
      from common import ENGINE_SQLITE3, ENGINE_MONETDB

      cur = self.persistent_store.cursor()
      for n, m in ModelMeta.models.iteritems():
         for row in self.cache.execute("SELECT COUNT(*) FROM " + m.get_table_name()):
            row_count = row[0]
            if row_count==0:
               continue

            move_count=min(count, row_count)
            field_names = ",".join(m.get_fields().names())
            from_sql = "SELECT " + field_names + " FROM " + m.get_table_name() + " LIMIT " + str(move_count)


   def execute(self, sql):
      return self.cache.execute(sql)

   def commit(self):
      self.cache.commit()

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

         rowcount=0
         for row in self.cache.execute("SELECT address_id, addr_type from address;"):
            rowcount+=1
            self.assertEqual(10, row[1])

         self.assertEqual(1, rowcount)

      def testConcurrentCacheAccess(self):
         cache2 = Cache(self.db, "test", "pass")

         am = Address.new(self.cache)
         am.addr_type = 10
         am.save()
         self.cache.commit()

         rowcount=0
         for row in cache2.execute("SELECT address_id, addr_type from address;"):
            rowcount+=1

         self.assertEqual(0, rowcount)


   unittest.main()
