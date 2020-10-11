import numpy as np
import pandas as pd
from Attribute import Attribute
from Dimension import Dimension, DimensionSCD1, DimensionSCD2



class Warehouse:

    def __init__(self, name, metadata, specifications):
        """Warehouse constructor

        Args:
            name (string): name of the warehouse
            metadata (csv): load csv file containing the DB metadata (table and column names, datatypes, references etc.)
            specifications (csv): specification of the warehouse. A CSV file containing dimension table names, and columns contained in the dimension
        """
        self.name = name
        self.dimensions = []
        self.fact_tables = []

        self.metadata = metadata
        self.specifications = specifications

        dimension_Names = list(set(specifications.loc[:, 'dim_name'].to_numpy()))
        for dim in dimension_Names:
            self.add_dimension_scd2(dim)

    def add_dimension(self, name):
        """Adds a dimension to the warehouse. The dimensions from the specifications file are added in the warehouse constructor, this is just the method used for that.

        Args:
            name (string): name of the dimension
        """
        dimension = Dimension(name, self.metadata, self.specifications)
        self.dimensions.append(dimension)
        return

    def add_dimension_scd1(self, name):
        dimension = DimensionSCD1(name, self.metadata, self.specifications)
        self.dimensions.append(dimension)

    def add_dimension_scd2(self, name):
        dimension = DimensionSCD2(name, self.metadata, self.specifications)
        self.dimensions.append(dimension)

    def get_dimension(self, name):
        """finds a dimension in the warehouse by name

        Args:
            name (string): name of dimension in the warehouse

        Returns:
            Dimension: Dimension object
        """
        for dim in self.dimensions:
            if dim.name == name:
                return dim
        
        return None


if __name__=="__main__":
    warehouse = Warehouse("test warehouse", pd.read_csv("metadata_with_foreign_keys.csv"), pd.read_csv("dimenzii.csv"))

    # for dim in warehouse.dimensions:
    #     print(warehouse.get_dimension(dim.name).ddl())
    #     print('\nFK:')
    #     for i in dim.get_foreign_keys():
    #         print(i)
    #     print('\n')

    # print(warehouse.get_dimension("dim_admins").ddl() + "\n")
    # print(warehouse.get_dimension("dim_students").ddl() + "\n")

    # print(warehouse.get_dimension("dim_admins").dml())
    # print(warehouse.get_dimension("dim_students").dml())
    # warehouse.get_dimension("dim_admins").ddl()
    # print('='*20)
    # print(warehouse.get_dimension("dim_admins").dml())

    # warehouse.get_dimension("dim_admins").sp_performETL()
    # print(warehouse.get_dimension("dim_students").ddl())
    # print('='*20)
    print(warehouse.get_dimension("dim_students").sp_performETL())