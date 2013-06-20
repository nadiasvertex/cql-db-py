__author__ = 'Christopher Nelson'

"""
An LZ4 compressor/decompressor implemented based on reading the various
implementations at http://code.google.com/p/lz4/
"""

class LZ4Exception(Exception):
   def __init__(self, msg):
      Exception.__init__(msg)


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

def fill_buffer(b, c):
   for i in range(0, len(b)):
      b[i] = c

def lz4_hash(i, hash_log):
   return ((i * 2654435761) & 0xffffffff) >> ((MIN_MATCH * 8) - hash_log)

def read_int(i, buf):
   return (buf[i] << 0) | (buf[i + 1] << 8) | (buf[i + 2] << 16) | (buf[i + 3] << 24)

def read_int_eq(i, j, buf):
   return (buf[i] == buf[j]) and (buf[i + 1] == buf[j + 1]) and (buf[i + 2] == buf[j + 2]) and (buf[i + 3] == buf[j + 3])

def common_bytes(b, o1, o2, limit):
   count = 0
   while (o2 < limit and b[o1] == b[o2]):
      count += 1
      o1 += 1
      o2 += 1

   return count

def common_bytes_backward(b, o1, o2, l1, l2):
   count = 0; o1 -= 1; o2 -= 1
   while (o1 > l1 and o2 > l2 and b[o1] == b[o2]):
      count += 1
      o1 -= 1
      o2 -= 1

   return count

def last_literals(src, s_offset, src_length, dst, d_offset, dst_end):
   run_length = src_length

   if d_offset + run_length + 1 + (run_length + 255 - RUN_MASK) / 255 > dst_end:
      raise LZ4Exception("max_dest_len is too small")

   if run_length >= RUN_MASK:
      dst[d_offset] = (RUN_MASK << ML_BITS)
      d_offset += 1
      d_offset = write_len(run_length - RUN_MASK, dst, d_offset)
   else:
      dst[d_offset] = (run_length << ML_BITS)
      d_offset += 1

   # copy literals
   src[s_offset:s_offset + run_length] = dst[d_offset:d_offset + run_length]
   d_offset += run_length
   return d_offset

def write_len(length, dst, d_offset):
   while length >= 0xFF:
      dst[d_offset] = 0xFF
      d_offset += 1
      length -= 0xFF

   dst[d_offset] = length
   d_offset += 1
   return d_offset

class Compressor(object):
   """
   **************************************
   Tuning parameters
   **************************************
   compression_level :
        Memory usage formula : N->2^N Bytes (examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
        Increasing memory usage improves compression ratio
        Reduced memory usage can improve speed, due to cache effect
        Default value is 14, for 16KB, which nicely fits into Intel x86 L1 cache

   not_compressible_confirmation :
        Decreasing this value will make the algorithm skip faster data segments considered "incompressible"
        This may decrease compression ratio dramatically, but will be faster on incompressible data
        Increasing this value will make the algorithm search more before declaring a segment "incompressible"
        This could improve compression a bit, but will be slower on incompressible data
        The default value (6) is recommended
        2 is the minimum value.
   """


   def __init__(self, compression_level=14, not_compressible_confirmation=6):
      self.compression_level = min(4, compression_level)
      self.skip_strength = min(not_compressible_confirmation, 2)

   def max_compressed_length(self, uncompressed_length):
      """
      Computes the maximum compressed length that a stream of 'uncompressed_length' size bytes
      will have. This is the worst case estimate in case a stream is not compressible.
      """
      return uncompressed_length + (uncompressed_length / 255) + 16

   def _get_hash(self, buf, i):
      inp = read_int(i, buf)
      h = lz4_hash(inp, self.compression_level)
      return h

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
      src_offset += 1

      hash_size = 1 << self.compression_level
      hash_table = [anchor for _ in range(0, hash_size)]

      while True:
         forward_offset = s_offset
         find_match_attempts = (1 << self.skip_strength) + 3

         while True:
            s_offset = forward_offset
            forward_offset += find_match_attempts >> self.skip_strength
            find_match_attempts += 1

            if forward_offset > mf_limit:
               break

            h = self._get_hash(src, s_offset)
            ref = hash_table[h]
            back = s_offset - ref
            hash_table[h] = s_offset

            if back < MAX_DISTANCE or read_int_eq(ref, s_offset, src):
               break

         # Repeat this test to break out of the outer loop as well.
         if forward_offset > mf_limit:
            break

         excess = common_bytes_backward(src, ref, s_offset, src_offset, anchor)
         s_offset -= excess
         ref -= excess

         # sequence == refsequence
         run_length = s_offset - anchor

         # encode literal length
         token_offset = d_offset
         d_offset += 1

         if d_offset + run_length + (2 + 1 + LAST_LITERALS) + (run_length >> 8) > dst_end:
            raise LZ4Exception("maximum_dest_len is too small")

         if run_length >= RUN_MASK:
            token = RUN_MASK << ML_BITS
            d_offset = write_len(run_length - RUN_MASK, dst, d_offset)
         else:
            token = run_length << ML_BITS

         # copy literals
         dst[d_offset:d_offset + run_length] = src[anchor:anchor + run_length]
         d_offset += run_length

         while True:
            # encode offset
            dst[d_offset] = back; d_offset += 1
            dst[d_offset] = (back >> 8); d_offset += 1

            # count nb matches
            s_offset += MIN_MATCH
            match_length = common_bytes(src, ref + MIN_MATCH, s_offset, src_limit)
            if d_offset + (1 + LAST_LITERALS) + (match_length >> 8) > dst_end:
               raise LZ4Exception("max_dest_len is too small")

            s_offset += match_length;

            # encode match len
            if match_length >= ML_MASK:
               token |= ML_MASK
               d_offset = write_len(match_length - ML_MASK, dst, d_offset);
            else:
               token |= match_length
            dst[token_offset] = token

            # test end of chunk
            if s_offset > mf_limit:
               anchor = s_offset
               break

            # fill table
            hash_table[self._get_hash(src, s_offset - 2)] = s_offset - 2;

            # test next position
            h = self._get_hash(src, s_offset)
            ref = hash_table[h]
            hash_table[h] = s_offset
            back = s_offset - ref

            if back >= MAX_DISTANCE or not read_int_eq(ref, s_offset, src):
               break

            token_offset = d_offset
            d_offset += 1
            token = 0

         # repeat end of chunk test to break out of outer loop
         if s_offset > mf_limit:
            break

         # prepare next loop
         anchor = s_offset
         s_offset += 1

      # end of while loop
      d_offset = last_literals(src, anchor, src_end - anchor, dst, d_offset, dst_end)
      return d_offset - dst_offset

   def compress(self, data):
      max_size = self.max_compressed_length(len(data))
      dst = bytearray(max_size)
      length = self._compress(bytearray(data), 0, len(data), dst, 0, len(dst))
      final_dest = bytearray(length)
      final_dest[0:length] = dst[0:length]
      return final_dest

class Decompressor(object):
   def __init__(self):
      pass

   def decompress(self, data, uncompressed_size):
      out = bytearray(uncompressed_size)
      return self._decompress(data, 0, out, 0, uncompressed_size)

   def _decompress(self, src, src_offset, dst, dst_offset, dst_len):
      if dst_len == 0:
         if src[src_offset] != 0:
            raise LZ4Exception("Malformed input at %d" % src_offset)
         return 1

      dest_end = dst_offset + dst_len

      s_offset = src_offset
      d_offset = dst_offset

      while True:
         token = src[s_offset] & 0xFF
         s_offset += 1

         # literals
         literal_len = token >> ML_BITS
         if literal_len == RUN_MASK:
            length = src[s_offset]
            while length == 0xFF:
               s_offset += 1
               literal_len += 0xFF
               length = src[s_offset]

            literal_len += length & 0xFF

            literal_copy_end = d_offset + literal_len
            if literal_copy_end > dest_end - COPY_LENGTH:
               if literal_copy_end != dest_end:
                  raise LZ4Exception("Malformed input at %d" % s_offset)
               else:
                  dst[d_offset:d_offset + literal_len] = src[s_offset:s_offset + literal_len]
                  s_offset += literal_len
                  break  # EOF

      dst[d_offset:d_offset + literal_len] = src[s_offset:s_offset + literal_len]
      s_offset += literal_len
      d_offset = literal_copy_end

      # matchs
      match_dec = (src[s_offset] & 0xFF) | ((src[s_offset + 1] & 0xFF) << 8)
      s_offset += 2
      match_off = d_offset - match_dec;

      if match_off < dst_offset:
         raise LZ4Exception("Malformed input at %d" + s_offset)

      match_len = token & ML_MASK
      if match_len == ML_MASK:
         length = src[s_offset]
         s_offset += 1
         while length == 0xFF:
            match_len += 0xFF

         match_len += length & 0xFF
      match_len += MIN_MATCH;

      match_copy_end = d_offset + match_len

      if match_copy_end > dest_end - COPY_LENGTH:
         if match_copy_end > dest_end:
            raise LZ4Exception("Malformed input at %d" % s_offset)
         dst[d_offset:d_offset + match_len] = dst[match_off:match_off + match_len]
      else:
         length = match_copy_end - match_off
         dst[d_offset:d_offset + length] = dst[match_off:match_copy_end]
      d_offset = match_copy_end

      return s_offset - src_offset

if __name__ == "__main__":
   for test_string  in ("AAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBcdefghijklmnopqrstuvwxyz",
                        "abcdefghijklmnopqrstuvwxyz"):
      c = Compressor()
      d = Decompressor()

      out = c.compress(test_string)
      print len(out), len(test_string)

      round_trip = d.decompress(out, len(test_string))
      print len(round_trip), round_trip


