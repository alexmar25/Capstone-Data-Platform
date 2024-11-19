import psycopg2
import pandas as pd

def get_last_rowid():
    conn = psycopg2.connect(
        host="localhost",
        database="sales_data",  # Update if your actual database name differs
        user="postgres",
        password="new_avocado81",  # Use your actual password
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(rowid) FROM sales_data")
    result = cursor.fetchone()
    conn.close()
    return result[0] if result[0] is not None else 0

def get_latest_records(last_row_id):
    df = pd.read_csv('/path/to/your/sales-csv3mo8i5SHvta76u7DzUfhiw.csv')  # Update file path as needed
    new_records = df[df['rowid'] > last_row_id]
    return new_records.to_dict(orient='records')

def insert_records(records):
    conn = psycopg2.connect(
        host="localhost",
        database="sales_data",  # Ensure this matches your database name
        user="postgres",
        password="new_avocado81",  # Your password
        port="5432"
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

# Main ETL Execution
if __name__ == "__main__":
    last_row_id = get_last_rowid()
    print("Last row id on production data warehouse =", last_row_id)
    
    new_records = get_latest_records(last_row_id)
    print("New rows on staging data warehouse =", len(new_records))
    
    if new_records:
        insert_records(new_records)
        print("New rows inserted into production data warehouse =", len(new_records))
    else:
        print("No new records to insert.")
