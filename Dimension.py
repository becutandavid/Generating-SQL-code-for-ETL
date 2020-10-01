import numpy as np 
import pandas as pd

from Attribute import Attribute, ForeignKey

class Dimension:
    def __init__(self, name, metadata, dimensions):
        self.name = name
        self.attributes = []
        self.dimensions = dimensions
        self.metadata = metadata

        for row in dimensions[dimensions["dim_name"]==self.name].to_numpy():
            self.add_attribute(row[1],row[2],row[3])

    def add_attribute(self, schema, table, attribute):
        """Adds an attribute to the dimension. It passes the same metadata used for the dimension.
        The method checks if the attribute is a foreign key, and if it is it adds a ForeignKey object instead of a regular Attribute object.

        Args:
            schema (string): schema name
            table (string ): table name
            attribute (string): attribute/column name
        """
        metadata = self.metadata
        
        # check if attribute is foreign key
        if (pd.isna(metadata.loc[(metadata['table_schema'] == schema) & (metadata['table_name'] == table) & (metadata['column_name'] == attribute), 'foreign_table_schema'].to_numpy()[0])):
            attr = Attribute(schema, table, attribute, metadata)
        else:
            # get foreign schema, table and attribute names and create a ForeignKey attribute.
            f_schema = metadata.loc[(metadata['table_schema'] == schema) & (metadata['table_name'] == table) & (metadata['column_name'] == attribute), 'foreign_table_schema'].to_numpy()[0]
            f_table = metadata.loc[(metadata['table_schema'] == schema) & (metadata['table_name'] == table) & (metadata['column_name'] == attribute), 'foreign_table_name'].to_numpy()[0]
            f_attribute = metadata.loc[(metadata['table_schema'] == schema) & (metadata['table_name'] == table) & (metadata['column_name'] == attribute), 'foreign_column_name'].to_numpy()[0]

            attr = ForeignKey(schema, table, attribute, f_schema, f_table, f_attribute, metadata)

        self.attributes.append(attr)

        return

    def ddl(self):
        """Like a toString() method. When called it prints out the DDL for creating the Dimension in PostgreSQL

        Returns:
            string: PostgreSQL for creating the dimension
        """
        sql = [f"CREATE TABLE {self.name}(\n  skey serial primary key"]

        for attribute in self.attributes:
            sql.append(attribute.ddl())

        return ",\n".join(sql)+"\n);"
    
    def get_foreign_keys(self):
        """returns a list of all attributes that are foreign keys

        Returns:
            list: list of ForeignKey objects
        """
        fkeys = []
        for attribute in self.attributes:
            if isinstance(attribute, ForeignKey):
                fkeys.append(attribute)
        
        return fkeys

    def dml(self):
        """Like a toString() method. When called it prints out the DML for the Dimension in PostgreSQL

        Returns:
            string: INSERT statements
        """
        # sql = string to return
        sql = [f""]

        # INSERT INTO sql
        sql_insert = [f"INSERT INTO {self.name}("]

        for attribute in self.attributes:
            sql_insert.append(attribute.attribute + ", ")

        sql_insert = "".join(sql_insert)
        sql_insert = sql_insert[:-2]        


        # FROM sql
        table= [f""]
        for i in self.get_foreign_keys():
            table.append(str(i))
        table = "".join(table).split('.')
        table = table[:-1]
        table = ".".join(table)

        initials = table.split('.')
        table_short = initials[0][0] + initials[1][0]

        sql_from = [f"FROM {table} {table_short}"]
        sql_from = "".join(sql_from)


        # SELECT sql
        sql_select = [f"SELECT "]
        for attribute in self.attributes:
            sql_select.append(table_short + "." + attribute.attribute + ", ")

        sql_select = "".join(sql_select)
        sql_select = sql_select[:-2]


        # LEFT OUTER JOIN sql
        initials = self.name.split('_')
        dim_short = initials[0][0] + initials[1][0]
         
        fk = [f""]
        for i in self.get_foreign_keys():
            fk.append(str(i))
        fk = "".join(fk).split('.')
        fk = fk[-1]

        join_sql = [f"LEFT OUTER JOIN {self.name} {dim_short} ON {dim_short}.{fk} = {table_short}.{fk}"]
        join_sql = "".join(join_sql)


        # WHERE sql
        sql_where = [f"WHERE {dim_short}.skey IS NULL"]

        sql_where = "".join(sql_where)


        sql.append(sql_insert + ")\n")
        sql.append(sql_select + "\n")
        sql.append(sql_from + "\n")
        sql.append(join_sql + "\n")
        sql.append(sql_where + "\n")

        return "".join(sql)