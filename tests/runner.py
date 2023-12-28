# tests/runner.py
import unittest

# import your test modules
import adls
import local

# initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# add tests to the test suite
# suite.addTests(loader.loadTestsFromModule(adls))
# suite.addTests(loader.loadTestsFromModule(local))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)