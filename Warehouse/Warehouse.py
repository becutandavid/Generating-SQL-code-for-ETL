import csv
import pandas as pd
from Warehouse.Dimension import Dimension, DimensionSCD1, DimensionSCD2
import psycopg2
from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


class Warehouse:

    def __init__(self, name, metadata=None, specifications=None):
        """Warehouse constructor

        Args:
            name (string): name of the warehouse
            metadata (csv): load csv file containing the DB metadata (table and column names, datatypes, references etc.)
            specifications (csv): specification of the warehouse. A CSV file containing dimension table names, and columns contained in the dimension
        """
        self.name = name
        self.dimensions = []
        self.fact_tables = []

        if metadata is None:
            self.metadata = self._get_metadata()

        else:
            self.metadata = pd.read_csv(metadata)
        self.specifications = pd.read_csv(specifications)

        dimension_Names = self.dimension_names()
        for dim in dimension_Names:
            self.add_dimension_scd2(dim)

    def _connect_to_db(self):
        try:
            self.conn = psycopg2.connect(**config())
            return self.conn.cursor()

        except (Exception) as error:
            print(error)

    def _close_db_connection(self, cur):
        try:
            cur.close()
            return True
        except Exception as error:
            print(error)

    def _get_metadata(self):
        cur = self._connect_to_db()
        with open('sql_scripts/sql_query_metadata.sql', 'r') as file:
            try:
                query = file.read()
                cur.execute(query)
                metadata = cur.fetchall()
                if metadata:
                    with open('metadata/metadata.csv', 'w') as metadata_file:
                        myFile = csv.writer(metadata_file)
                        myFile.writerow(['table_schema', 'table_name', 'column_name', 'udt_name', 'is_nullable',
                                         'character_maximum_length', 'foreign_table_schema', 'foreign_table_name',
                                         'foreign_column_name'])
                        myFile.writerows(metadata)
                    metadata_file.close()
                else:
                    raise NotImplementedError
            except Exception as error:
                self._close_db_connection(cur)
                print(error)

        metadata = pd.read_csv('metadata/metadata.csv')
        file.close()
        self._close_db_connection(cur)

        return metadata

    def dimension_names(self):
        return list(set(self.specifications.loc[:, 'dim_name'].to_numpy()))

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

    def write_etl_to_file(self, file_path):
        with open(file_path, 'w') as file:
            for dim in self.dimension_names():
                file.write(f'\n -- DDL for {dim}\n')
                file.write(self.get_dimension(dim).ddl())
                file.write(f'\n -- Procedure for {dim}\n')
                file.write(self.get_dimension(dim).sp_performETL())

            for dim in self.dimensions:
                file.write(f'CALL {dim.get_etl_name()};\n')
        file.close()

    def etl(self):
        cur = self._connect_to_db()

        submission = []
        for dim in self.dimension_names():
            submission.append(self.get_dimension(dim).ddl())
            submission.append(self.get_dimension(dim).sp_performETL())

        for dim in self.dimension_names():
            submission.append(f'CALL {self.get_dimension(dim).get_etl_name()};')

        submission = '\n'.join(submission)
        cur.execute(submission)

        return cur.fetchall()





