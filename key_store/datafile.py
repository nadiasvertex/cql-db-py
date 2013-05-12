"""
Data file theory of operation
=============================

The structure of the data file is quite straightforward. There are two files:
one file is the actual data content, the other file is the write-ahead log. All
operations are first journaled, and then later written to disk. The journal 
itself only contains metadata operations. The actual data is always written to
a free spot in a data page. In the case of updates, the data is written to a
new empty area, and the old area is deallocated. This way data can never be
lost one we commit to a write.

The datafile has a header on the first page which indicates how many virtual
pages exist in the datafile. There is also a directory that has an entry for each
virtual page. The directory provides a mapping from virtual pages to actual
disk pages, and is the mechanism by which the extensible hashing algorithm is
implemented.

The first 32 bytes of the header are composed of an sha256 digest of the 
directory and the directory size.

0                       8
+-----------------------+
| directory_size        |
+-----------------------+
| entry 0               |
+-----------------------+
| entry n..             |
+-----------------------+
"""

import array
import hashlib
import io
import os
import struct

# signature + directory entry count
header_read_fmt = "<32sQ"
# directory entry count + directory
header_write_fmt = "<Q%ss"
# key value + data pointer
key_fmt = "<QQ"
# number of bytes in value
value_header_fmt = "<I"
# the actual value
value_fmt = "<%ss"

class IntegrityError(Exception):
     def __init__(self):
         pass
     def __str__(self):
         return "integrity failure"


class KeyPage(object):
   """
   synopsis: Maintains a key page. 
   
   The key page does not contain values. Rather, it contains 64-bit value
   pointers.
   """
   __slots__ = ["dirty", "keys", "undo_keys", "page_size", "max_entries"]
   def __init__(self, d, page_size):
      entry_size = struct.calcsize(key_fmt)
      
      self.dirty = False
      self.keys = {}
      self.undo_keys = {}
      self.page_size = page_size
      self.max_entries = self.page_size / entry_size
      for i in range(0, self.max_entries):
         data = d.read(entry_size)
         if len(data) < entry_size:
            break
         k,v = struct.unpack(key_fmt, data)
         if k == 0 and v == 0:
            break
         self.keys[k] = v
         
   def get(self, key, default=None):
      """
      :synopsis: If the key exists, return the value, otherwise return default.
      :param key: The key to lookup
      :param default: The value to return if the key does not exist.
      :returns: The value if key exists, otherwise 'default'
           
      The key is the actual user's key, however the value is merely a pointer
      to the value data somewhere in the data file.
      """
      return self.keys.get(key, default)
      
   def set(self, key, value):
      """
      :synopsis: Write a new key, if and only if we have space to write it.
      
      :param key: The key to write.
      :param value: The value pointer to write.
      :returns: False if we were unable to write the new value, True otherwise.
      
      Writes a new key. If we have too many keys, we will fail. The old value is
      saved before we write the new value.
      """
      
      old_value = self.keys.get(key)
      if old_value==None and len(self.keys) >= self.max_entries:
         return False
         
      if not self.undo_keys.has_key(key):
         self.undo_keys[key] = old_value
         
      self.keys[key] = value
      self.dirty = True
      
      return True
      
   def commit(self):
      """
      :synopsis: Commits to the new state.
      """
      self.undo_keys = {}
      
   def rollback(self):
      """
      :synopsis: Restores the keypage to the values it had before the current
                 transaction began.
      """
      for k,v in self.undo_keys.iteritems():
         if v==None:
            del self.keys[k]
         else:
            self.keys[k] = v
            
   def flush(self, d):
      """
      :synopsis: Writes the key page to disk. The file object must be positioned
      where the data should be written. Unused space on the page will be cleared.
      """
      bytes_to_clear = self.page_size
      for k,v in self.keys.iteritems():
         data = struct.pack(key_fmt, k, v)
         d.write(data)
         bytes_to_clear-=len(data)
      
      if bytes_to_clear>0:
         d.write("\x00" * bytes_to_clear)
      self.dirty = False
      
class DataFile(object):
   """
   :synopsis: Manages the data file header and large-scale operations of the data file.
   
   A datafile is a disk-based hash, with certain elements kept in memory for fast access. The elements
   can be paged out of memory one flushed to disk, which means that we don't have to take up a lot of
   RAM in order to store a lot of data.
   """
   __slots__ = [ "page_size", "mask", "cache", "d", "l", "a" ]
   def __init__(self, filename):
      self.page_size = 8192
      self.cache = {}
      self.a = array.array("L")
      
      if not os.path.exists(filename):
         self.d = open(filename, "w+b")
         self.l = open(filename + ".wal", "w+b")   
         self._create()
      else:
         self.d = open(filename, "r+b")
         self.l = open(filename + ".wal", "r+b")
         self._load()
         
      self.mask = (1<<(len(self.a)))-1

   def _create_header(self):
      """
      :synopsis: Generates a signature and header blob from the directory.
      """
      directory = self.a.tostring()
      header = struct.pack(header_write_fmt % len(directory), len(self.a), directory)
      m = hashlib.sha256()
      m.update(header)
      return (m.digest(), header)
       
   def _create(self):
      """
      :synopsis: Initializes the database file with a default directory.
      """
      self.a.append(1)
      self.a.append(2)
      signature, header = self._create_header()
      self.d.seek(0)
      #print "signature=%d, header=%d" % (len(signature), len(header))
      self.d.write(signature)
      self.d.write(header)
      self.d.flush()
      
   def _load(self):
      """
      :synopsis: Loads the database header and directory.
      """
      self.d.seek(0)
      header = self.d.read(struct.calcsize(header_read_fmt))
      #print "signature + entry_count = %d" % len(header)
      signature, num_entries = struct.unpack(header_read_fmt, header)
      self.a.fromfile(self.d, num_entries)
      check, header = self._create_header()
      if check != signature:
         raise IntegrityError()
      
   def set(self, key, value):
      index = key & self.mask
      page = self.a[index]
      
      
      
