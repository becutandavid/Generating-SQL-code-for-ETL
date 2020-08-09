import numpy as np
import pandas as pd


class Attribute:
    metadata = pd.read_csv("tables_columns_metadata.csv")
    def __init__(self, schema, table, attribute):
        self.schema = schema
        self.table = table
        self.attribute = attribute

        self.data_type = metadata.loc[np.logical_and(metadata['table_schema'] == 'platforma', np.logical_and(metadata['table_name'] == 'admins', metadata['column_name'] == 'id')),'udt_name'][0]
        self.nullable = True if metadata.loc[np.logical_and(metadata['table_schema'] == 'platforma', np.logical_and(metadata['table_name'] == 'admins', metadata['column_name'] == 'id')),'nullable'][0] == 'YES' else False
        self.length = int(metadata.loc[np.logical_and(metadata['table_schema'] == 'platforma', np.logical_and(metadata['table_name'] == 'admins', metadata['column_name'] == 'id')),'character_maximum_length'][0])

    def is_nullable(self):
        if self.nullable:
            return ""
        else:
            return " NOT NULL"
    
    def ddl(self):
        if self.data_type in ["char", "varchar"]:
            return f"{self.attribute} {self.data_type}({self.length}){self.is_nullable()}"
    




