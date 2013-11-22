'''
Created on Nov 21, 2013

@author: Christopher Nelson
'''

import logging
import os
import subprocess

# The path to the storage engine. This is where we keep on-disk, persistent
# storage for our archives.
storage_engine_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'storage_engine')
warehouse_controller = os.path.join(storage_engine_path, "bin", "monetdbd")

class Warehouse():
   """
   A warehouse provides for storage of persistent data. Instead of being just
   a dumb store, the warehouse has intelligence. Higher level functions will
   push some operations down to the warehouse layer for better performance.
   """
   def __init__(self, storage_path, passphrase, port=50000,
                start_immediately=True, initialize=True):
      """
      This initializes the storage path if it doesn't already exist. If the
      path does exist, it will simply prepare the storage for use. If the
      'start_immediately' flag is set, the storage will be started immediately.
      
      @param storage_path: A directory where the storage should be placed. If
                           the directory exists it is expected to contain
                           a valid warehouse image. If it does not contain
                           a valid image the warehouse will not be useable. Also
                           see the 'initialize' member below.
                           
      @param passphrase:   A security token used to secure access to the control
                           plane. If the warehouse has not been initialized,
                           the security token will be set to this value. If the
                           warehouse has been initialized you must provide the
                           same value here in order to connect to it.
                           
      @param port:         The port that this warehouse should listen on.
                           
      @param start_immediately: If set to True and if initialize is True, the 
                           database will be started immediately. Otherwise an 
                           explicit call to 'start' will be required. If the
                           database is not started it will not be possible
                           to use it in any way. The default is True. 
                           
      @param initialize:   If set to True, the database will be initialized.
                           If set to False, you will need to manually perform
                           the initialization steps. The default is True.
      """
      self.storage_path = storage_path
      self.passphrase = passphrase
      self.port = port
      self.started = False
      
      if not initialize:
         return
      
      if not os.path.exists(self.storage_path):
         self.create()
         
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
      
      try:
         output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
      except Exception, e:
         msg = "Unable to run command: '%s' -> %s" % (" ".join(cmd), e.output)
         logging.exception(msg)
         raise e
      
      logging.debug("'%s' -> %s", (" ".join(cmd), output))
      
      return output
      
   def destroy(self):
      """
      Destroys an existing data warehouse. The warehouse must already be 
      stopped.
      """
      if self.started:
         self.stop()
      
      logging.info("Destroying warehouse at '%s'", (self.storage_path,))
      for dirpath, dirnames, filenames in os.walk(self.storage_path, False):
         for filename in filenames:
            os.unlink(os.path.join(dirpath, filename))
         for dirname in dirnames:
            os.rmdir(os.path.join(dirpath, dirname))
         
      os.removedirs(self.storage_path)   
               
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
      
      logging.info("Creating warehouse at '%s'", (self.storage_path,))
      self._control_warehouse("create")
      for property in ["passphrase="+self.passphrase,
                       "control=yes",
                       "discovery=yes",
                       "port=%d" % self.port]:
         self._control_warehouse("set", property)
      
   def set(self, property, value):
      """
      Sets the value of property.
      
      @param property: The property to set.
      @param value: The value to set it to.    
      """
      self._control_warehouse("set", property+"="+value)
      
   def get(self, property):
      """
      Gets the value of property.
      
      @param property: The property to get, or 'all' to get all properties.
      
      @returns: A dictionary mapping the properties to their values.
      """
      output = self._control_warehouse("get", property).splitlines()[1:]
      return dict([line.split(" ", 1) for line in output])
      
      
   def start(self):
      """
      Starts the data warehouse. It will be available for service once
      started.
      """
      if self.started:
         return
      
      logging.info("Starting warehouse at '%s'", (self.storage_path,))
      
      self._control_warehouse("start")
      self.started = True
      
   def stop(self):
      """
      Stops the data warehouse. It will no longer be available for service
      once stopped.
      """
      if not self.started:
         return
      
      logging.info("Stopping warehouse at '%s'", (self.storage_path,))
      
      self._control_warehouse("stop")
      self.started = False  

if __name__ == "__main__":
   logging.basicConfig(level=logging.DEBUG)
   w = Warehouse("/tmp/test_warehouse", "letmein")
   w.stop()
   w.destroy()