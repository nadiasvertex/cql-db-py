import sqlite3

class Cache(object):
   """
   This object manages the cache. All data is initially written into the cache.
   Over time, in the background, data is migrated into the persistent store.
   """
   def __init__(self, database, username, password):
      self.persistent_store = database.connect()
      self.cache = sqlite3.connect(":memory:")

   def __del__(self):
      self.flush()

   def flush(self):
      self.move()

   def move(self, count=-1):
      pass
