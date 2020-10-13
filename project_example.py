from Warehouse.Warehouse import Warehouse

if __name__ == "__main__":
    warehouse = Warehouse("test warehouse", metadata='metadata/metadata_mssql.csv', specifications="dimenzii.csv", language='MSSQL')
    warehouse.write_etl_to_file('outputETL.sql')
