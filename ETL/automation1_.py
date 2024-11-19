import psycopg2
import pandas as pd

def get_last_rowid():
    conn = psycopg2.connect(
        host= "localhost",
        database="sales_data_warehouse",
        user="postgres",
        password="new_avocado81",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(rowid) FROM sales_data")
    result = cursor.fetchone()
    conn.close()
    # Return the highest rowid or 0 if no records exist
    return result[0] if result[0] is not None else 0

def get_latest_records(last_row_id):
    # Load data from CSV
    df = pd.read_csv('/Volumes/Alex/Data_Engineering/CAPSTONE/ETL/sales-csv3mo8i5SHvta76u7DzUfhiw.csv')
   # Filter records with rowid greater than the last_row_id
    new_records = df[df['rowid'] > last_row_id]
    # Return records as a list of dictionaries
    return new_records.to_dict(orient='records')

def insert_records(records):
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="sales_data_warehouse",
        user="postgres",
        password="new_avocado81",
        port="5432"
    )

    cursor = conn.cursor()
        # Loop through each record and insert into the sales_data table

    for record in records:
        cursor.execute(
            """
            INSERT INTO sales_data (rowid, product_id, customer_id, price, quantity, timestamp, total_sales) 
            # Replace with actual data fields
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record['rowid'],
                record['product_id'],
                record['customer_id'],
                record['price'],
                record['quantity'],
                record['timestamp'],
                record['total_sales']
            )
        )
    # Commit changes and close the connection
    conn.commit()
    conn.close()

# ETL Execution
last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))

if new_records:
    insert_records(new_records)
    print("New rows inserted into production datawarehouse = ", len(new_records))
else:
    print("No new records to insert.")
