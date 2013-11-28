__author__ = 'christopher'

class Session(object):
   def __init__(self, database, cache):
      self.db=database
      self.cache = cache
