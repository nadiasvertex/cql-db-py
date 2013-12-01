import sqlite3
from common import ENGINE_MONETDB, ENGINE_SQLITE3
from concurrent import futures
from functools import partial

class SameThreadFuture(object):
   """
   Emulates a future. This is used for sqlite3 objects because they can't
   be run on a separate thread from the one they were created on.
   """
   def __init__(self, func, *args, **kwargs):
      self._runnable = partial(func, *args, **kwargs)
      self._result = None

   def result(self):
      if self._result is None:
         self._result = self._runnable()

      return self._result

class CacheResultsIterator(object):
   def __init__(self, cache_cursor, store_cursor):
      self.pool = futures.ThreadPoolExecutor(max_workers=2)
      self.cache_cursor = cache_cursor
      self.store_cursor = store_cursor

      self.result_pools = []
      self._submit()

      self.result_pool = None
      self.result_index = 0

   def _submit(self):
      # Fetch from the cache first, it is probably faster.
      data = SameThreadFuture(self.cache_cursor.fetchmany)
      # If we have data, append the pool to the result pools.
      if data.result():
         self.result_pools.append(data)
      # Issue a request to the backing store.
      self.result_pools.append(self.pool.submit(self.store_cursor.fetchmany))

   def __iter__(self):
      return self

   def _read_result(self, result_pool):
      """
      Reads a single result from a result pool.

      :param result_pool:  The result pool to read from.
      :return: The data. If data is None then this result pool has
      been exhausted.
      """

      if len(result_pool) == 0:
         return None

      if len(result_pool) <= self.result_index:
         # Re-issue a fetch, but indicate that we need to switch pools
         self._submit()
         self.result_index = 0
         return None

      current_index = self.result_index
      self.result_index += 1

      return result_pool[current_index]


   def next(self):
      '''
      Provide the next result. Note that results are read from the cache and
      the persistent store in an undefined order.

      :return: The next result.
      '''
      if self.result_pool is None:
         if len(self.result_pools)==0:
            self.pool.shutdown()
            raise StopIteration()

         self.result_pool=self.result_pools.pop()

      # Fetch a record from the result pool. This operation may
      # block if the underlying fetch has not completed.
      result = self._read_result(self.result_pool.result())

      # If the result is None then this pool has been exhausted. Recursively
      # defer to the next pool for results.
      if result is None:
         self.result_pool = None
         return self.next()

      # provide the result
      return result

class Cache(object):
   """
   This object manages the cache. All data is initially written into the cache.
   Over time, in the background, data is migrated into the persistent
   store.
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
      '''
      Execute some SQL against the cache.

      :param sql: The SQL to execute.
      :return: A DB-API2 style iterator of the query results against the cache.
      '''
      return self.cache.execute(sql)

   def commit(self):
      '''
      Commit the current transaction in the cache.
      :return: None
      '''
      self.cache.commit()

   def retrieve(self, query):
      """
      Executes a query against the entire system. The operation is required to
      be a SelectQuery.

      :param query: The query to execute.
      :return: An iterator which yields up all the results from the query.
      """

      # Issue the store query execution first in the assumption that it will
      # take longer.
      store_cursor = self.store.cursor()
      store_cursor.execute(query.gen(ENGINE_MONETDB))

      # Issue the cache query execution.
      cq = query.gen(ENGINE_SQLITE3)
      cache_cursor = self.cache.cursor()
      cache_cursor.execute(cq)

      # Provide an iterator object over the results.
      return CacheResultsIterator(cache_cursor, store_cursor)

   def new(self, model, **kwargs):
      '''
      Initializes a new object of the given model. You may specify field
      names as keyword arguments to provide them with values.

      :param model: The model to use when creating a new object.
      :return: A plain old data object initialized as requested, and attached to
      this cache.
      '''
      return model.new(cache=self, **kwargs)

   def starting_with(self, model, **kwargs):
      '''
      The starting point of a selection query.

      :param model: The model to start with.
      :param kwargs: Additional settings.
      :return: A select query.
      '''

      from query import SelectQuery
      return SelectQuery(model, cache=self, **kwargs)

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
         self.w = Warehouse("/tmp/test_warehouse", "letmein", port=60002)
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

      def testStartingWithCacheOnly(self):
         am = Address.new(self.cache)
         am.addr_type = 10
         am.save()

         q = self.cache.starting_with(Address)\
                       .select(Address.address_id)

         rowcount = 0
         for row in q:
            rowcount += 1

         self.assertEqual(1, rowcount)


   unittest.main()
