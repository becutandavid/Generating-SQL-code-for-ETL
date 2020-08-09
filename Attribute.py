import numpy as np
import pandas as pd
import math

class Attribute:
    metadata = pd.read_csv("tables_columns_metadata.csv")
    def __init__(self, schema, table, attribute):
        self.schema = schema
        self.table = table
        self.attribute = attribute

        metadata = self.metadata

        self.data_type = metadata.loc[np.logical_and(metadata['table_schema'] == schema, np.logical_and(metadata['table_name'] == table, metadata['column_name'] == attribute)),'udt_name'].to_numpy()[0]
        self.nullable = True if metadata.loc[np.logical_and(metadata['table_schema'] == schema, np.logical_and(metadata['table_name'] == table, metadata['column_name'] == attribute)),'is_nullable'].to_numpy()[0] == 'YES' else False
        self.length = Attribute.nan_convert(metadata.loc[np.logical_and(metadata['table_schema'] == schema, np.logical_and(metadata['table_name'] == table, metadata['column_name'] == attribute)),'character_maximum_length'].to_numpy()[0])

    @staticmethod
    def nan_convert(x):
        if (math.isnan(x)):
            return 0
        else:
            return int(x)

    def is_nullable(self):
        if self.nullable:
            return ""
        else:
            return " NOT NULL"
    
    def ddl(self):
        if self.data_type in ["char", "varchar"]:
            return f"{self.attribute} {self.data_type}({self.length}){self.is_nullable()}"
        else:
            return f"{self.attribute} {self.data_type}{self.is_nullable()}"
    




