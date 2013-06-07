__author__ = 'Christopher Nelson'

class Level(object):
   __slots__ = ["file_id", "file_handle", "max_block", "entry", "block", "merge_count", "acc_merge_count"]
   def __init__(self):
      self.file_id = 0
      self.file_handle = None
      self.max_block = 0
      self.entry = 0
      self.block = 0
      self.merge_count = 0
      self.acc_merge_count = 0

class LevelState(object):
   __slots__ = ["tree", "level_type", "file_id", "file_handle",
                "current_page", "current_page_head", "current_entries",
                "current_offset", "max_offset", "block_size",
                "level", "page", "entry", "next_fence_ptr", "next_state",
                "buffer"]
   def __init__(self):
      pass

class MergeState(object):
   __slots__ = ["tree", "merge_level", "merge_entry", "step_entry", "state1", "state2", "state_out",
                "entry1", "entry2", "level_id", "finish"]
   def __init__(self):
      pass

class ResultSet(object):
   __slots__ = ["num", "results", "valid", "next_result_set"]
   def __init__(self):
      pass

class FdTree(object):
   __slots__ = [ "n", "k", "current_level_count", "adaptive_level_count",
                 "levels", "tmp_levels", "head_tree", "tmp_tree",
                 "current_level_state", "current_merge_state", "deamortized_mode",
                 "current_pid", "current_offset", "result_set"]
   def __init__(self):
      self.n = 0
      self.k = 0
      self.current_level_count = 0
      self.adaptive_level_count = 0
      self.levels = []
      self.tmp_levels = []
      self.head_tree = None
      self.tmp_tree = None
      self.current_level_state = None
      self.current_merge_state = None
      self.deamortized_mode = False
      self.current_pid = 0
      self.current_offset = 0
      self.result_set = None

   def insert(self, entry):
      """
      :synopsis: Insert a entry into the index. The entry is first inserted
      into the head tree, and is then migrated into lower levels if necessary.

      The return value is the level id of the lowest merged level caused
      by this insertion.If no merge operation occurs, return 0 (level id
      of the head tree).

      :param entry: The entry to insert.
      :returns: The level id of the lowest merged level caused by this
                insertion.
      """

      level_merge = 0

      # ensure the entry is NOT a fence
      entry.set_non_fence()

      # first insert the entry into the head tree.
      self.head_tree.insert(entry)

      # If the head tree exceeds its capacity, we merge it into the
      # next level, and migrate all index entries into the lower levels.
      # We use #entry to check whether L0 is full, instead of #pages.
      # (If using #pages, the external fences from L1 may directly
      # cause L0 full, given a larger k value).
      if self.head_tree.over_capacity():

         # at this point, all entries at the head tree will be merged
         # into lower levels.We can reset the dirty bits of all head
         # tree pages in the buffer pool to zero to prevent the pages
         # from being written by buffer manager later.
         #resetAllDirtyBit();


         # merge the 1st and 2nd levels. This may cause recursive merge
         # in lower levels.
         level_merge = self.merge(1)

      return level_merge
