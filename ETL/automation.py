import psycopg2
import pandas as pd

def get_last_rowid():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Iloveavocado"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(rowid) FROM sales_data")
    result = cursor.fetchone()
    conn.close()
    return result[0] if result[0] is not None else 0

def get_latest_records(last_row_id):
    df = pd.read_csv('/Users/mariomartinez/Desktop/ETL/sales.csv')
    new_records = df[df['rowid'] > last_row_id]
    return new_records.to_dict(orient='records')

def insert_records(records):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Iloveavocado"
    )
    cursor = conn.cursor()
    
    for record in records:
        cursor.execute(
            """
            INSERT INTO sales_data (rowid, product_id, customer_id, price, quantity, timestamp, total_sales)
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
