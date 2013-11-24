"""
Created on Nov 21, 2013

@author: Christopher Nelson
"""

from cStringIO import StringIO

import logging
import os
import select
import subprocess
import sys

# The path to the storage engine. This is where we keep on-disk, persistent
# storage for our archives.
storage_engine_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                   'storage_engine')
warehouse_controller = os.path.join(storage_engine_path, "bin", "monetdbd")
database_controller = os.path.join(storage_engine_path, "bin", "monetdb")

sys.path.append(os.path.join(storage_engine_path, "lib", "python2.7",
                             "site-packages"))
from monetdb import sql

def run_command(cmd):
   if not cmd:
      return
   p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   out = StringIO()
   err = StringIO()
   poll = select.poll()
   poll.register(p.stdout)
   poll.register(p.stderr)

   fd_map = {
      p.stdout.fileno(): (p.stdout, out),
      p.stderr.fileno(): (p.stderr, err),
   }

   while p.poll() is None:
      ready = poll.poll(250)
      for fd, event in ready:
         if event != select.POLLIN:
            continue

         h, stream = fd_map[fd]
         stream.write(h.read())

   out = out.getvalue()
   err = err.getvalue()
   if p.returncode != 0:
      msg = "Unable to run command: '%s'" % (" ".join(cmd))
      logging.error(msg)
      logging.debug(out)
      logging.error(err)
      raise subprocess.CalledProcessError(p.returncode, cmd, out)

   logging.debug("'%s'", " ".join(cmd))
   if out: logging.debug(out)
   if err: logging.error(err)

   return out


class Warehouse():
   """
   A warehouse provides for storage of persistent data. Instead of being just
   a dumb store, the warehouse has intelligence. Higher level functions will
   push some operations down to the warehouse layer for better performance.
   """

   def __init__(self, storage_path, pass_phrase, port=50000,
                start_immediately=True, initialize=True):
      """
      This initializes the storage path if it doesn't already exist. If the
      path does exist, it will simply prepare the storage for use. If the
      'start_immediately' flag is set, the storage will be started immediately.
      
      :param storage_path: A directory where the storage should be placed. If
                           the directory exists it is expected to contain
                           a valid warehouse image. If it does not contain
                           a valid image the warehouse will not be usable. Also
                           see the 'initialize' member below.
                           
      :param pass_phrase:   A security token used to secure access to the
                           control plane. If the warehouse has not been
                           initialized, the security token will be set to
                           this value. If the warehouse has been initialized
                           you must provide the same value here in order to
                           connect to it.
                           
      :param port:         The port that this warehouse should listen on.
                           
      :param start_immediately: If set to True and if initialize is True, the
                           database will be started immediately. Otherwise an 
                           explicit call to 'start' will be required. If the
                           database is not started it will not be possible
                           to use it in any way. The default is True. 
                           
      :param initialize:   If set to True, the database will be initialized.
                           If set to False, you will need to manually perform
                           the initialization steps. The default is True.
      """
      self.storage_path = storage_path
      self.pass_phrase = pass_phrase
      self.port = port
      self.started = False

      if not initialize:
         return

      if not os.path.exists(self.storage_path):
         self.create()
      else:
         self._set_properties()

      if not start_immediately:
         return

      self.start()

   def _control_warehouse(self, operation, arg=None):
      """
      Provides control of the warehouse on a low level. Start, stop, create, 
      etc. If the command fails a CalledProcessError will be thrown.
      
      @param operation: A control operation. Must be one of: 'create', 'start',
                        'stop', 'get', 'set', 'version'.
                        
      @param arg:      The argument for the command. Each command only accepts
                       zero or one arguments.
      """
      if arg:
         cmd = [warehouse_controller,
                operation, arg,
                self.storage_path]
      else:
         cmd = [warehouse_controller,
                operation, self.storage_path]

      return run_command(cmd)

   def destroy(self):
      """
      Destroys an existing data warehouse. The warehouse must already be 
      stopped.
      """
      if self.started:
         self.stop()

      logging.info("Destroying warehouse at '%s'", self.storage_path)
      for dirpath, dirnames, filenames in os.walk(self.storage_path, False):
         for filename in filenames:
            os.unlink(os.path.join(dirpath, filename))
         for dirname in dirnames:
            os.rmdir(os.path.join(dirpath, dirname))

      os.removedirs(self.storage_path)


   def _set_properties(self):
      for prop in ["passphrase=" + self.pass_phrase, "control=yes",
                   "discovery=yes",
                   "port=%d" % self.port]:
         self._control_warehouse("set", prop)

   def create(self):
      """
      Creates a warehouse. If the specified location already exists, it will
      be cleaned out prior to creation. If 'initialize' is set to True in the
      constructor, this function will be called automatically as needed.      
      """
      if self.started:
         logging.error("Tried to 'create' a warehouse that is already running.")
         return

      if os.path.exists(self.storage_path):
         self.destroy()

      logging.info("Creating warehouse at '%s'", self.storage_path)
      self._control_warehouse("create")
      self._set_properties()

   def set(self, prop, value):
      """
      Sets the value of prop.
      
      @param prop: The property to set.
      @param value: The value to set it to.    
      """
      logging.debug("Setting property %s=%s", prop, value)
      self._control_warehouse("set", prop + "=" + value)

   def get(self, prop):
      """
      Gets the value of prop.
      
      @param prop: The property to get, or 'all' to get all properties.
      
      @returns: A dictionary mapping the properties to their values.
      """
      output = self._control_warehouse("get", prop).splitlines()[1:]
      return dict([line.split(" ", 1) for line in output])


   def start(self):
      """
      Starts the data warehouse. It will be available for service once
      started.
      """
      if self.started:
         return

      logging.info("Starting warehouse at '%s'", self.storage_path)

      self._control_warehouse("start")
      self.started = True

   def stop(self):
      """
      Stops the data warehouse. It will no longer be available for service
      once stopped.
      """
      if not self.started:
         return

      logging.info("Stopping warehouse at '%s'", self.storage_path)

      self._control_warehouse("stop")
      self.started = False


class Database():
   def __init__(self, warehouse, db_name, hostname="localhost",
                start_immediately=True, initialize=True):
      self.warehouse = warehouse
      self.db_name = db_name
      self.hostname = hostname

      if not initialize:
         return

      if not self.exists():
         self.create()

      if not start_immediately:
         return

      self.start()

   def _control_database(self, operation, args=tuple(), no_database=False):
      """
      Controls a database in a given warehouse.

      :rtype : string
      :param no_database: If this is set to 'False' no 'database' clause will
                          be added to the command.

      :param operation: The operation to run. Must be one of create, destroy,
                        lock, release, status, start, stop, kill, set, get,
                        inherit, discover, and version
                        
      :param args:      The arguments for the operation. These vary based on
                        the operation being performed.
      """
      cmd = (database_controller,
             "-h", self.hostname,
             "-p", str(self.warehouse.port),
             "-P", self.warehouse.pass_phrase,
             "-q",
             operation) + args +\
             (self.db_name,) if not no_database else tuple()

      return run_command(cmd)

   def exists(self):
      stat = self.status(all_databases=True)
      if not stat:
         return False

      return True

   def status(self, mode="simple", all_databases=False):
      """
      Provides status information about this database, or if all_databases is
      set to True all databases in this warehouse.
      
      @param mode: The status mode. This may be 'simple', 'common', 
                   or 'complete'. 
      @param all_databases: Whether to return status information for all
                  databases, or just this database.
      
      @returns: A list of dictionaries, one for each database with the
                properties as keys.
      """
      if mode == "common":
         args = ("-c",)
      elif mode == "complete":
         args = ("-l",)
      else:
         args = tuple()

      output = self._control_database("status", args,
                                      no_database=all_databases)
      if not output:
         return []

      if mode == "simple":
         values = []
         lines = output.splitlines()
         names = lines[0].split(None, 4)
         for line in lines[1:]:
            values.append(dict(zip(names, line.split(None, 4))))

         return values
      elif mode=="common" or mode=="complete":
         lines = output.splitlines()
         d={}
         if mode=="common":
            d["summary"] = lines[0]
            lines = lines[1:]

         for line in lines:
            k,v = line.split(":",1)
            d[k.strip()]=v.strip()

         values = [d]
         return values


   def create(self):
      self._control_database("create")

   def destroy(self):
      self._control_database("destroy", ("-f", ))

   def stop(self):
      self._control_database("stop")

   def start(self):
      self._control_database("start")

   def connect(self, username="monetdb", password="monetdb"):
      """
      :param username: The username to connect as.
      :param password: The password to authenticate with.
      :return: A connection object ready for use.
      """
      return  sql.Connection(hostname=self.hostname, port=self.warehouse.port,
                username=username, password=password,
                database=self.db_name)

if __name__ == "__main__":
   logging.basicConfig(level=logging.DEBUG)
   w = Warehouse("/tmp/test_warehouse", "letmein", port=60001)
   db = Database(w, "testdb")
   db.status()
   db.status(mode="common")
   db.status(mode="complete")
   con = db.connect()
   cur = con.cursor()
   print cur.execute("CREATE TABLE test_table(id bigint)")
   print cur.execute("INSERT INTO test_table VALUES(1), (2), (3)")
   print cur.execute("SELECT * FROM test_table")
   print cur.fetchall()
   db.stop()
   db.destroy()
   w.stop()
   w.destroy()
