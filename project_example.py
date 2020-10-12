from Warehouse.Warehouse import Warehouse

if __name__ == "__main__":
    warehouse = Warehouse("test warehouse", metadata=None, specifications="dimenzii.csv")
    warehouse.write_etl_to_file('outputETL.sql')
