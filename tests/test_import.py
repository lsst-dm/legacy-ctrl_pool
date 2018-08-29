import unittest
import lsst.ctrl.pool.pool


class ImportTest(unittest.TestCase):
    def testImport(self):
        self.assertTrue(hasattr(lsst.ctrl.pool.pool, "Pool"))


if __name__ == "__main__":
    unittest.main()
