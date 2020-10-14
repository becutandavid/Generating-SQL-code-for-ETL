import numpy as np
import pandas as pd
import math

class Attribute:
    def __init__(self, schema, table, attribute, metadata=None):
        self.schema = schema
        self.table = table
        self.attribute = attribute
        self.metadata = metadata

        if metadata is not None:
            self.data_type = metadata.loc[np.logical_and(metadata['table_schema'].str.lower() == schema.lower(), np.logical_and(metadata['table_name'].str.lower() == table.lower(), metadata['column_name'].str.lower() == attribute.lower())),'udt_name'].to_numpy()[0]
            self.nullable = True if metadata.loc[np.logical_and(metadata['table_schema'].str.lower() == schema.lower(), np.logical_and(metadata['table_name'].str.lower() == table.lower(), metadata['column_name'].str.lower() == attribute.lower())),'is_nullable'].to_numpy()[0] == 'YES' else False
            self.length = Attribute.nan_convert(metadata.loc[np.logical_and(metadata['table_schema'].str.lower() == schema.lower(), np.logical_and(metadata['table_name'].str.lower() == table.lower(), metadata['column_name'].str.lower() == attribute.lower())),'character_maximum_length'].to_numpy()[0])

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
        """Like a toString() method. Outputs a string that's used for specifying the attribute in the DDL. 
        Example: 
        Name VARCHAR(20) NOT NULL

        Returns:
            string: [description]
        """
        if self.data_type in ["nchar", "nvarchar"]:
            return f"  {self.attribute} {self.data_type}({self.length}){self.is_nullable()}"
        else:
            return f"  {self.attribute} {self.data_type}{self.is_nullable()}"
    

    def getTableSchema(self):
        return f"{self.schema}.{self.table}"

    def __str__(self):
        return f"{self.schema}.{self.table}.{self.attribute}"

    def set_data_type(self, type, nullable=True, length=None):
        self.data_type = type
        self.nullable = nullable
        self.length = length



class ForeignKey(Attribute):
    def __init__(self, schema, table, attribute, f_schema, f_table, f_attribute, metadata):
        super().__init__(schema, table, attribute, metadata)
        self.f_schema = f_schema
        self.f_table = f_table
        self.f_attribute = f_attribute

class SCDAttribute(Attribute):
    def __init__(self, schema, table, attribute, metadata=None, default='NOW()'):
        super().__init__(schema, table, attribute, None)
        self.default = default

    def ddl(self):
        return super().ddl() + f' default {self.default}'
