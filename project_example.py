from Warehouse.Warehouse import Warehouse

if __name__ == "__main__":
    warehouse = Warehouse("test warehouse", metadata='metadata_with_foreign_keys.csv', specifications="dimenzii.csv")
    warehouse.write_etl_to_file('outputETL.sql')
