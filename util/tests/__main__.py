# RUNME as 'python -m key_store.tests.__main__'
import unittest
import util.tests

def main():
   "Run all of the tests when run as a module with -m."
   suite = util.tests.get_suite()
   runner = unittest.TextTestRunner()
   runner.run(suite)

if __name__ == '__main__':
   main()
