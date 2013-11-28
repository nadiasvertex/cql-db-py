__author__ = 'cnelson'

import types

pred_map = {
   "eq" : "=",
   "ne" : "<>",
   "lt" : "<",
   "gt" : ">",
   "lte" : "<=",
   "gte" : ">="
}

ENGINE_SQLITE3 = 0
ENGINE_MONETDB = 1

def convert_to_sqlite3(value):
   t = type(value)
   if t in types.StringTypes:
      return "'%s'" % (value.replace("'", "''"))
   if t in (types.IntType, types.LongType, types.FloatType):
      return str(value)
   if value is None:
      return "NULL"
   raise TypeError("Unable to convert '%s' to sqlite3 value." % type(value))

def convert_to_monetdb(value):
   t = type(value)
   if t in types.StringTypes:
      return "'%s'" % (value.replace("'", "''"))
   if t in (types.IntType, types.LongType, types.FloatType):
      return str(value)
   if value is None:
      return "NULL"
   raise TypeError("Unable to convert '%s' to monetdb value." % type(value))

def convert_to_sql(value, engine):
   if engine==ENGINE_SQLITE3:
      return convert_to_sqlite3(value)
   else:
      return convert_to_monetdb(value)


