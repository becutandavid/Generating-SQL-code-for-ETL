import numpy as np 
import pandas as pd

class Dimension:
    metadata = pd.read_csv("tables_columns_metadata.csv")
    dimensions = pd.read_csv("dimenzii.csv")
    def __init__(self, name, pkey = 'skey'):
        self.name = name
        self.pkey = pkey
        self.attributes = []

        





    def ddl(self):
        sql = [f"CREATE TABLE {self.name}(\n {self.pkey} serial primary key,\n"]

        for attribute in self.attributes:
            sql.append(attribute.ddl())

        return ",\n".join(sql)
    








