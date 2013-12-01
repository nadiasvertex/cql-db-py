import sqlite3

class Cache(object):
   """
   This object manages the cache. All data is initially written into the cache.
   Over time, in the background, data is migrated into the persistent store.
   """
   def __init__(self, database, username, password):
      self.store = database.connect()
      self.cache = sqlite3.connect(":memory:")
      self._create_schema()

   def __del__(self):
      self.flush()

   def _get_persistent_store_tables(self):
      cur = self.store.cursor()
      cur.execute("SELECT name, type FROM sys.tables WHERE system=false;")
      tables = cur.fetchall()
      cur.close()
      return [row[0] for row in tables]

   def _create_schema(self):
      from model import ModelMeta
      from common import ENGINE_SQLITE3, ENGINE_MONETDB

      cur = self.store.cursor()
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

   def move(self, count=0):
      from model import ModelMeta
      from common import ENGINE_SQLITE3, ENGINE_MONETDB

      scur = self.store.cursor()
      ccur = self.cache.cursor()
      for n, m in ModelMeta.models.iteritems():
         for row in self.cache.execute("SELECT COUNT(*) FROM " + m.get_table_name()):
            row_count = row[0]
            if row_count==0:
               continue

            # Figure out how many rows to move
            move_count = row_count if count < 1 else min(count, row_count)

            # Prepare to move the data
            field_names_list = m.get_fields().keys()
            field_names = ",".join(field_names_list)
            values = ",".join(["%s" for x in range(0, len(field_names_list))])
            from_sql = "SELECT " + field_names + " FROM " + m.get_table_name() + " LIMIT " + str(move_count)
            to_sql = "INSERT INTO " + m.get_table_name() + "(" + field_names +\
                     ") VALUES (" + values + ")"

            # Load rows up from the cache
            ccur.execute(from_sql)
            data = ccur.fetchall()

            # Write them in the persistent store
            scur.executemany(to_sql, data)

            # Delete the data from the cache
            pk_name = m.get_primary_key_name()
            pk_index = field_names_list.index(pk_name)
            id_values = [str(row[pk_index]) for row in data]
            ccur.execute("DELETE FROM " + m.get_table_name() + " WHERE " +
                        pk_name + " IN (" + ",".join(id_values) + ")")

      # Commit the transaction
      ccur.close()
      scur.close()
      self.store.commit()

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

      def testFlushDataToStore(self):
         am = Address.new(self.cache)
         am.addr_type = 10
         am.save()

         self.cache.flush()

         # Now check to see if we got the data into the store.
         cur = self.cache.store.cursor()
         cur.execute("SELECT address_id, addr_type from address;")

         rowcount=0
         for row in cur.fetchall():
            rowcount+=1
            self.assertEqual(10, row[1])

         self.assertEqual(1, rowcount)

         # Finally, make sure the data is out of the cache
         rowcount=0
         for row in self.cache.execute("SELECT address_id, addr_type from address;"):
            rowcount+=1
         self.assertEqual(0, rowcount)




   unittest.main()
