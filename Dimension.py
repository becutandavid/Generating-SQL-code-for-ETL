import numpy as np 
import pandas as pd

from Attribute import Attribute

class Dimension:
    metadata = pd.read_csv("tables_columns_metadata.csv")
    dimensions = pd.read_csv("dimenzii.csv")
    def __init__(self, name, pkey = 'skey'):
        self.name = name
        self.pkey = pkey
        self.attributes = []
        dimensions = self.dimensions

        for row in dimensions[dimensions["dim_name"]==self.name].to_numpy():
            attr = Attribute(row[1],row[2],row[3])
            self.attributes.append(attr)       

    def ddl(self):
        sql = [f"CREATE TABLE {self.name}(\n{self.pkey} serial primary key"]

        for attribute in self.attributes:
            sql.append(attribute.ddl())

        return ",\n".join(sql)+"\n)"


if __name__=="__main__":
    dim = Dimension("dim_admins")
    print(dim.ddl())
