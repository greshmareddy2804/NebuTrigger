import os
import requests
import csv
import psycopg2

def fetch_csv_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Check for any HTTP request errors
    return response.text

def insert_data_into_postgres(data, connection_string):
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        # Replace 'your_table_name' with the actual name of the table in your database
        table_name = 'geopzo'

        # Assuming the CSV has a header row. If not, set 'has_header' to False and manually provide column names.
        has_header = True
        csv_reader = csv.reader(data.splitlines())
        header = next(csv_reader) if has_header else None

        for row in csv_reader:
            # Your CSV columns should match the order and data types of the columns in your table.
            # Adjust this part based on your table structure and data types.
            query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({', '.join(['%s']*len(row))})"
            cursor.execute(query, row)

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def main() -> None:
    # Replace 'your_url_here' with the URL that points to the CSV data
    url ='https://celestrak.org/NORAD/elements/gp.php?SPECIAL=gpz&FORMAT=csv'

    # Replace 'your_connection_string_here' with your actual Azure Database for PostgreSQL connection string
    connection_string = '''dbname='nebucyber' user='nebula@nebula-demo' host='nebula-demo.postgres.database.azure.com' password='Greshu@928' port='5432' sslmode='true''''

    try:
        csv_data = fetch_csv_data(url)
        insert_data_into_postgres(csv_data, connection_string)
    except Exception as e:
        print(f"Error: {e}")

# Make sure to include the required packages (requests and psycopg2) in the function's directory.
# This function assumes you have already set up the necessary environment variables for connection_string.
