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

header_read_fmt = "<32sQ"
header_write_fmt = "<Q%ss"
key_fmt = "<Q"
value_header_fmt = "<I"
value_fmt = "<%ss"

class IntegrityError(Exception):
     def __init__(self):
         pass
     def __str__(self):
         return "integrity failure"


class DataFile(object):
   __slots__ = [ "page_size", "cache", "d", "l", "a" ]
   def __init__(self, filename):
      self.page_size = 8192
      self.cache = {}
      self.a = array.array("L")
      
      if not os.path.exists(filename):
         self.d = open(filename, "w+b")
         self.l = open(filename + ".wal", "w+b")   
         self.create()
      else:
         self.d = open(filename, "r+b")
         self.l = open(filename + ".wal", "r+b")
         self.load()

   def _create_header(self):
      """
      :synopsis: Generates a signature and header blob from the directory.
      """
      directory = self.a.tostring()
      header = struct.pack(header_write_fmt % len(directory), len(self.a), directory)
      m = hashlib.sha256()
      m.update(header)
      return (m.digest(), header)
       
   def create(self):
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
      
   def load(self):
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
      
