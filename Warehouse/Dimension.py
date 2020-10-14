from typing import List

import pandas as pd

from Warehouse.Attribute import Attribute, ForeignKey, SCDAttribute

class Dimension:
    def __init__(self, name, metadata, dimensions, language="POSTGRES"):
        self.name = name
        self.attributes = []
        self.dimensions = dimensions
        self.metadata = metadata
        self.attr_id = 0
        self.language = language
        self.global_counter = 0

        for row in dimensions[dimensions["dim_name"]==self.name].to_numpy():
            self.add_attribute(row[1],row[2],row[3])
            if row[3].lower().endswith('id'):
                self.attr_id = len(self.attributes)-1


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
        if (pd.isna(metadata.loc[(metadata['table_schema'].str.lower() == schema.lower()) & (metadata['table_name'].str.lower() == table.lower()) & (metadata['column_name'].str.lower() == attribute.lower()), 'foreign_table_schema'].to_numpy()[0])):
            attr = Attribute(schema, table, attribute, metadata)
        else:
            # get foreign schema, table and attribute names and create a ForeignKey attribute.
            f_schema = metadata.loc[(metadata['table_schema'].str.lower() == schema.lower()) & (metadata['table_name'].str.lower() == table.lower()) & (metadata['column_name'].str.lower() == attribute.lower()), 'foreign_table_schema'].to_numpy()[0]
            f_table = metadata.loc[(metadata['table_schema'].str.lower() == schema.lower()) & (metadata['table_name'].str.lower() == table.lower()) & (metadata['column_name'].str.lower() == attribute.lower()), 'foreign_table_name'].to_numpy()[0]
            f_attribute = metadata.loc[(metadata['table_schema'].str.lower() == schema.lower()) & (metadata['table_name'].str.lower() == table.lower()) & (metadata['column_name'].str.lower() == attribute.lower()), 'foreign_column_name'].to_numpy()[0]

            if f_schema == schema and f_table == table and f_attribute == attribute:
                attr = Attribute(schema,table,attribute,metadata)
            else:
                attr = ForeignKey(schema, table, attribute, f_schema, f_table, f_attribute, metadata)

        self.attributes.append(attr)

        return

    def _get_attributes(self, instanceName):
        return map(lambda x: instanceName+'.'+x, self.attributes)

    def ddl(self):
        """Like a toString() method. When called it prints out the DDL for creating the Dimension in PostgreSQL

        Returns:
            string: PostgreSQL for creating the dimension
        """
        if self.language == 'POSTGRES':
            sql = [f"CREATE TABLE {self.name}(\n  skey serial primary key"]
        elif self.language == 'MSSQL':
            sql = [f"CREATE TABLE {self.name}(\n  skey int identity(1,1) primary key"]

        for attribute in self.attributes:
            sql.append(attribute.ddl())

        return ",\n".join(sql)+"\n);\n"

    def dml(self):
        select_string = []
        for attr in self.attributes:
            select_string.append(f'{attr.schema}.{attr.table}.{attr.attribute}')
        select_string = f'SELECT {", ".join(select_string)}'


        from_string = [f'FROM']
        tables = set()
        for fkey in self.get_foreign_keys():
            from_string.append(f'{fkey.schema}.{fkey.table} INNER JOIN {fkey.f_schema}.{fkey.f_table} on ({fkey.schema}.{fkey.table}.{fkey.attribute} = {fkey.f_schema}.{fkey.f_table}.{fkey.f_attribute})')
            tables.add(f'{fkey.schema}.{fkey.table}')
            tables.add(f'{fkey.f_schema}.{fkey.f_table}')

        for attr in self.attributes:
            if f'{attr.schema}.{attr.table}' not in tables:
                tables.add(f'{attr.schema}.{attr.table}')
                from_string.append(f'{attr.schema}.{attr.table}')

        from_string.append(f'LEFT OUTER JOIN {self.name} on '
                           f'({self.attributes[self.attr_id].schema}.{self.attributes[self.attr_id].table}.{self.attributes[self.attr_id].attribute} = {self.name}.{self.attributes[self.attr_id].attribute})')

        from_string = ' '.join(from_string)

        where_string = f'WHERE {self.name}.skey is NULL;'

        return f'INSERT INTO {self.name}({",".join([attr.attribute for attr in self.attributes if not isinstance(attr, SCDAttribute)])})\n' + select_string + '\n' + from_string + '\n' + where_string + '\n'


    def get_table_names(self):
        out = []
        for attr in self.attributes:
            out.append(attr.table)
        return out

    
    def get_foreign_keys(self) -> List[Attribute]:
        """returns a list of all attributes that are foreign keys

        Returns:
            list: list of ForeignKey objects
        """
        fkeys = []
        for attribute in self.attributes:
            if isinstance(attribute, ForeignKey):
                fkeys.append(attribute)
        
        return fkeys

    def get_etl_name(self):
        return f"sp_performETL_{self.name.split('_')[1].capitalize()}"

    def get_table_alias(self, table_name):
        if table_name.startswith('dim_'):
            table_alias = 'd'
            table_name = table_name[4:]
        else:
            table_alias = ''

        names = table_name.split('.')
        for n in names:
            table_alias += n[0]

        table_alias += str(self.global_counter)
        self.global_counter += 1

        return table_alias


class DimensionSCD1(Dimension):
    def __init__(self, name, metadata, dimensions, language):
        super().__init__(name, metadata, dimensions, language)

    def sp_performETL(self):
        if self.language == 'POSTGRES':
            out = [f'CREATE OR REPLACE PROCEDURE sp_performETL_{self.name.split("_")[1].capitalize()}()\nLANGUAGE PLPGSQL\nAS $$\nBEGIN\n']
        elif self.language == 'MSSQL':
            out = [f'CREATE PROCEDURE sp_performETL_{self.name.split("_")[1].capitalize()}\nAS\nBEGIN\n']
        # insert izraz
        out.append(self.dml())

        # UPDATE
        set_string = []
        for attr in self.attributes:
            set_string.append(f'{self.name}.{attr.attribute}={attr.schema}.{attr.table}.{attr.attribute}')
        set_string = f'SET {", ".join(set_string)}'


        from_string = [f'FROM']
        tables = set()
        for fkey in self.get_foreign_keys():
            from_string.append(f'{fkey.schema}.{fkey.table} INNER JOIN {fkey.f_schema}.{fkey.f_table} on ({fkey.schema}.{fkey.table}.{fkey.attribute} = {fkey.f_schema}.{fkey.f_table}.{fkey.f_attribute})')
            tables.add(f'{fkey.schema}.{fkey.table}')
            tables.add(f'{fkey.f_schema}.{fkey.f_table}')

        for attr in self.attributes:
            if f'{attr.schema}.{attr.table}' not in tables:
                tables.add(f'{attr.schema}.{attr.table}')
                from_string.append(f'{attr.schema}.{attr.table}')


        from_string = ' '.join(from_string)

        where_string = []

        for idx, attr in enumerate(self.attributes):
            if idx != self.attr_id:
                where_string.append(f'{attr.schema}.{attr.table}.{attr.attribute} != {self.name}.{attr.attribute}')

        where_string = f'WHERE {self.attributes[self.attr_id].schema}.{self.attributes[self.attr_id].table}.' \
                       f'{self.attributes[self.attr_id].attribute} = {self.name}.{self.attributes[self.attr_id].attribute}' \
                       f' and' + ' (' + ' or '.join(where_string) + ');'


        update_string = f'UPDATE {self.name}\n' + set_string + '\n' + from_string + '\n' + where_string + '\n'
        out.append(update_string)

        if self.language == 'POSTGRES':
            out.append('END $$;')
        elif self.language == "MSSQL":
            out.append('END;\n\n')



        return "\n".join(out)


class DimensionSCD2(Dimension):

    def __init__(self, name, metadata, dimensions, language):
        super().__init__(name, metadata, dimensions, language)
        self._add_scd_columns()

    def _add_scd_columns(self):
        if self.language == 'POSTGRES':
            start = SCDAttribute(None, self.name, 'start_date')
        else:
            start = SCDAttribute(None, self.name, 'start_date', default='getdate()')
        end = SCDAttribute(None, self.name, 'end_date', default="'9999-12-31'")
        start.set_data_type('timestamp', False)
        end.set_data_type('timestamp', True)

        self.attributes.append(start)
        self.attributes.append(end)

    def dml(self):
        select_string = []
        for attr in self.attributes:
            if not isinstance(attr, SCDAttribute):
                select_string.append(f'{attr.schema}.{attr.table}.{attr.attribute}')
        select_string = f'SELECT {", ".join(select_string)}'


        from_string = [f'FROM']
        tables = set()
        for fkey in self.get_foreign_keys():
            from_string.append(f'{fkey.schema}.{fkey.table} INNER JOIN {fkey.f_schema}.{fkey.f_table} on ({fkey.schema}.{fkey.table}.{fkey.attribute} = {fkey.f_schema}.{fkey.f_table}.{fkey.f_attribute})')
            tables.add(f'{fkey.schema}.{fkey.table}')
            tables.add(f'{fkey.f_schema}.{fkey.f_table}')

        for attr in self.attributes:
            if f'{attr.schema}.{attr.table}' not in tables and not isinstance(attr, SCDAttribute):
                tables.add(f'{attr.schema}.{attr.table}')
                from_string.append(f'{attr.schema}.{attr.table}')

        from_string.append(f'LEFT OUTER JOIN {self.name} on '
                           f'({self.attributes[self.attr_id].schema}.{self.attributes[self.attr_id].table}.{self.attributes[self.attr_id].attribute} = {self.name}.{self.attributes[self.attr_id].attribute})')

        from_string = ' '.join(from_string)

        where_string = f'WHERE {self.name}.skey is NULL;'

        return f'INSERT INTO {self.name}({",".join([attr.attribute for attr in self.attributes if not isinstance(attr, SCDAttribute)])})\n' + select_string + '\n' + from_string + '\n' + where_string + '\n'

    def sp_performETL(self):
        if self.language == 'POSTGRES':
            out = [f'CREATE OR REPLACE PROCEDURE sp_performETL_{self.name.split("_")[1].capitalize()}()\nLANGUAGE PLPGSQL\nAS $$\nBEGIN\n']
        elif self.language == 'MSSQL':
            out = [f'CREATE PROCEDURE sp_performETL_{self.name.split("_")[1].capitalize()}\nAS\nBEGIN\n']

        # insert izraz
        out.append(self.dml())
        out.append('-- find modified')
        # find modified

        if self.language == 'POSTGRES':
            select_modified = [f'CREATE TEMP TABLE scd2 AS\nSELECT {self.name}.skey']
        else:
            select_modified = [f'SELECT {self.name}.skey']

        for attr in self.attributes:
            if not isinstance(attr, SCDAttribute):
                select_modified.append(f'{attr.schema}.{attr.table}.{attr.attribute}')

        select_modified = ", ".join(select_modified) + '\n'

        if self.language == 'MSSQL':
            select_modified += 'INTO scd2\n'

        # from izraz
        from_modified = [f'FROM']
        tables = set()
        for fkey in self.get_foreign_keys():
            from_modified.append(f'{fkey.schema}.{fkey.table} INNER JOIN {fkey.f_schema}.{fkey.f_table} on ({fkey.schema}.{fkey.table}.{fkey.attribute} = {fkey.f_schema}.{fkey.f_table}.{fkey.f_attribute})')
            tables.add(f'{fkey.schema}.{fkey.table}')
            tables.add(f'{fkey.f_schema}.{fkey.f_table}')

        for attr in self.attributes:
            if f'{attr.schema}.{attr.table}' not in tables and not isinstance(attr, SCDAttribute):
                tables.add(f'{attr.schema}.{attr.table}')
                from_modified.append(f'{attr.schema}.{attr.table}')

        from_modified.append(f'INNER JOIN {self.name} on '
                           f'({self.attributes[self.attr_id].schema}.{self.attributes[self.attr_id].table}.{self.attributes[self.attr_id].attribute} = {self.name}.{self.attributes[self.attr_id].attribute})')

        from_modified = ' '.join(from_modified)

        from_and = []
        for idx, attr in enumerate(self.attributes):
            if idx != self.attr_id and not isinstance(attr, SCDAttribute):
                from_and.append(f'{attr.schema}.{attr.table}.{attr.attribute} != {self.name}.{attr.attribute}')

        from_modified += ' and (' + ' or '.join(from_and) + ')\n'

        # where
        where_modified = f"WHERE {self.name}.end_date = '9999-12-31';\n"

        out.append(select_modified + from_modified + where_modified)

        out.append('-- update table')
        # update table (scd2)
        update_modified = f'UPDATE {self.name}\n'\
                           f'SET end_date=NOW()\n'\
                           f'FROM scd2\n'\
                           f'WHERE scd2.skey = {self.name}.skey;\n'
        out.append(update_modified)

        out.append('-- add updated rows')
        # update rows
        add_updated_rows = []
        update_rows_attrs = []
        for attr in self.attributes:
            if not isinstance(attr, SCDAttribute):
                update_rows_attrs.append(f'{attr.attribute}')

        add_updated_rows.append(f'INSERT INTO {self.name}({", ".join(update_rows_attrs)})')
        add_updated_rows.append(f'SELECT {", ".join(update_rows_attrs)}\n'
                                f'FROM scd2;\n')

        out.append("\n".join(add_updated_rows))

        if self.language == 'POSTGRES':
            out.append('DROP TABLE scd2;\nEND $$;\n\n')
        elif self.language == "MSSQL":
            out.append('END;\n\n')

        return "\n".join(out)



