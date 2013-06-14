__author__ = 'Christopher Nelson'

"""
An LZ4 compressor/decompressor implemented based on the description
at http://fastcompression.blogspot.com/2011/05/lz4-explained.html
"""


# Constants
COPY_LENGTH = 8
ML_BITS = 4
ML_MASK = ((1 << ML_BITS) - 1)
RUN_BITS = (8 - ML_BITS)
RUN_MASK = ((1 << RUN_BITS) - 1)

MAXD_LOG = 16
MAX_DISTANCE = ((1 << MAXD_LOG) - 1)
MIN_MATCH = 4
MF_LIMIT = (COPY_LENGTH + MIN_MATCH)
MIN_LENGTH = (MF_LIMIT + 1)
LZ4_64K_LIMIT = ((1 << 16) + (MF_LIMIT - 1))
LAST_LITERALS = 5
SIZE_OF_LONG_TIMES_TWO_SHIFT = 4
STEP_SIZE = 8
DEBRUIJN_BYTE_POS = [ 0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7,
                     7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7 ]

def hash(i, hash_log):
   return (i * -1640531535) >> ((MIN_MATCH * 8) - hash_log)


class Compressor(object):
   """
   **************************************
   Tuning parameters
   **************************************
   compression_level :
        Increasing this value improves compression ratio
        Lowering this value reduces memory usage
        Reduced memory usage typically improves speed, due to cache effect (ex : L1 32KB for Intel, L1 64KB for AMD)
        Memory usage formula : N->2^(N+2) Bytes (examples : 12 -> 16KB ; 17 -> 512KB)
        4 is the minimum value

   not_compressible_confirmation :
        Decreasing this value will make the algorithm skip faster data segments considered "incompressible"
        This may decrease compression ratio dramatically, but will be faster on incompressible data
        Increasing this value will make the algorithm search more before declaring a segment "incompressible"
        This could improve compression a bit, but will be slower on incompressible data
        The default value (6) is recommended
        2 is the minimum value.
   """


   def __init__(self, compression_level = 12, not_compressible_confirmation = 6):
      self.compression_level = min(4, compression_level)
      # Figure out hash table size - must be a multiple of 16.
      hash_size = 1<<self.compression_level
      while hash_size % 16 != 0:
         self.compression_level+=1
         hash_size = 1<<self.compression_level
      self.hash_table = bytearray(hash_size)
      self.skip_strength = min(not_compressible_confirmation, 2)

   def max_compressed_length(self, uncompressed_length):
      """
      Computes the maximum compressed length that a stream of 'uncompressed_length' size bytes
      will have. This is the worst case estimate in case a stream is not compressible.
      """
      return uncompressed_length + (uncompressed_length/255) + 16

   def _fill_buffer(self, b, c):
      for i in range(0,len(b)):
         b[i] = c


   def _compress(self, src, src_offset, src_len, dst, dst_offset, max_dest_len):
      dst_end = dst_offset + max_dest_len

      # If less than 64k go into small mode compressor.
      # TODO

      src_end = src_offset + src_len
      mf_limit = src_end - MF_LIMIT
      src_limit = src_end - LAST_LITERALS
      s_offset = src_offset
      d_offset = dst_offset
      anchor = src_offset
      src_offset+=1

      self._fill_buffer(self.hash_table, anchor)

      while True:
         forward_offset = s_offset
         find_match_attempts = (1<<self.skip_strength) + 3

         while True:
            s_offset = forward_offset
            forward_offset += find_match_attempts >> self.skip_strength
            find_match_attempts += 1

            if forward_offset > mf_limit:
               break

         # Repeat this test to break out of the outer loop as well.
         if forward_offset > mf_limit:
            break

         h = self.hash()




   def compress(self, data):
      max_size = self.max_compressed_length(len(data))
      dst = bytearray(max_size)
      length = self._compress(data, dst)
      final_dest = bytearray(length)
      final_dest[0:length] = dst[0:length]
      return final_dest

class Decompressor(object):
   def __init__(self):
      pass

   def read_length(self, data, pos):
      total_length = 15
      while True:
         pos+=1
         length = data[pos]
         total_length += length
         # A length of 255 indicates more bits coming, otherwise we
         # have reached the end of our length field.
         if length != 255:
            break

      return total_length, pos

   def read_sequence(self, data, pos):
      token = data[pos]
      literal_length = token >> 4
      match_length = token & 0x0f
      literals = []

      # Read the rest of the literal length if the field indicates that
      # there are more bits coming.
      if literal_length == 15:
         literal_length, pos = self.read_length(data, pos)

      # Now read the literal bytes
      for i in range(0, literal_length):
         pos += 1
         literals.append(data[pos])

      # Read the offset field
      pos += 1
      offset = data[pos]
      pos += 1
      offset |= (data[pos]<< 8)

      # Read the rest of the match length if the field indicates that
      # there are more bits coming.
      if match_length == 15:
         match_length, pos = self.read_length(data, pos)

      # Add the minmatch offset to the match_length
      match_length += 4



