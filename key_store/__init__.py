"""
The key_store package implements an atomic key/value storage facility. The 
values are compressed where that makes sense. Updates and inserts are performed 
atomically and conform to ACID semantics.

They key store itself is not thread safe. You must arrange for thread safety
yourself. They key store data file cannot be used by concurrent processes. The
assumption is that only one thread is accessing the file at any one time.
"""
__version__ = 1.0

