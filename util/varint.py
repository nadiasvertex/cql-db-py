import struct

def encode(v):
  values = []
  v = (v<<1) ^ (v>>63)
  while True:
    b = v & 0x7f
    v >>= 7
    if v!=0:
      b|=0x80
      values.append(b)
    else:
      values.append(b)
      break
      
  return struct.pack("%dB" % len(values), *values) 
    
def decode(s, offset=0):
  v = 0; shift = 0
  while True:
    b = ord(s[offset])
    v |= (b& 0x7f)<<shift
    if b & 0x80:
      offset+=1
      shift+=7
      continue      
    break   
  v = (v>>1) ^ (-(v&1))
  return v
    
def encode_stream(v, f):
  values = []
  v = (v<<1) ^ (v>>63)
  while True:
    b = v & 0x7f
    v >>= 7
    if v!=0:
      b|=0x80
      f.write(chr(b))
    else:
      f.write(chr(b))
      break
          
def decode_stream(f):
  v = 0; shift = 0
  while True:
    b = ord(f.read(1)[0])
    v |= (b& 0x7f)<<shift
    if b & 0x80:
      shift+=7
      continue      
    break   
  v = (v>>1) ^ (-(v&1))
  return v
  
  
