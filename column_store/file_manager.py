'''
Created on Jun 8, 2013

@author: Christopher Nelson
'''
import io

class Manager(object):
   __slots__ = ["next_file_id"]
   def __init__(self):
      self.next_file_id = 0

   def allocate_file(self):
      file_id = self.next_file_id
      file_handle = io.FileIO("%d.idx" % file_id, "a+")
      self.next_file_id += 1

      return (file_id, file_handle)
