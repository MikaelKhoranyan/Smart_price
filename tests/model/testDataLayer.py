import unittest

from sqlalchemy import inspect
from model.data_layer import get_sql_connection

class testDataLayer(unittest.TestCase):
    def setUp(self):
        self.engine = get_sql_connection()
    
    def testDbConn(self):
        insp = inspect(self.engine)
        self.assertGreater(len(insp.get_table_names()), 0)

if __name__ == '__main__':
    unittest.main()