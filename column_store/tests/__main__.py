# RUNME as 'python -m key_store.tests.__main__'
import unittest
import column_store.tests

def main():
   "Run all of the tests when run as a module with -m."
   suite = column_store.tests.get_suite()
   runner = unittest.TextTestRunner()
   runner.run(suite)

if __name__ == '__main__':
   main()
