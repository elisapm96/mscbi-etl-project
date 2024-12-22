import psycopg2
import csv
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database parameters and file path
csv_file = '/Users/elisaperez/Documents/visual-studio/msc-bi/etl-project/dataset-relational-database.csv'
db_params = {
    'dbname': 'etl_project',
    'user': 'elisaperez',
    'password': 'elisa',
    'host': 'localhost'
}
table_name = 'elisapm96_schema.sales_data'

def test_connection(db_params):
    """Function to test database connection"""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("SELECT 'Connection test successful!'")
        message = cursor.fetchone()
        logging.info(message)
        conn.close()
    except Exception as e:
        logging.error("Connection test failed: %s", e)

def delete_all_rows(db_params, table_name):
    """Delete all rows from a specified table."""
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Execute the DELETE statement
        delete_query = f'DELETE FROM {table_name}'
        cursor.execute(delete_query)
        conn.commit()
        
        # Log success
        logging.info("All rows deleted successfully from table: %s", table_name)
    except Exception as e:
        # Log failure
        logging.error("Failed to delete rows from table: %s | Reason: %s", table_name, e)
    finally:
        if conn:
            conn.close()

import psycopg2
import csv
import logging

def get_table_columns(cursor, table_name):
    """Retrieve column names and data types of the target database table."""
    try:
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
        """, (table_name.split('.')[-1], table_name.split('.')[0]))
        return {row[0]: row[1] for row in cursor.fetchall()}
    except Exception as e:
        logging.error("Failed to retrieve table columns: %s", e)
        raise

def clean_csv_columns(csv_columns):
    """Clean column names from the CSV file (remove spaces, handle BOM)."""
    csv_columns = [col.strip() for col in csv_columns]
    if csv_columns[0].startswith('\ufeff'):  # Handle BOM character
        csv_columns[0] = csv_columns[0].replace('\ufeff', '')
    return csv_columns

def clean_row_keys(row):
    """Clean row keys to match cleaned column names."""
    return {key.replace('\ufeff', '').strip(): value for key, value in row.items()}

def prepare_insert_query(table_name, matching_columns):
    """Prepare the SQL INSERT query dynamically based on matching columns."""
    placeholders = ', '.join(['%s'] * len(matching_columns))
    return f"INSERT INTO {table_name} ({', '.join(matching_columns)}) VALUES ({placeholders})"

def process_row(cleaned_row, matching_columns):
    """Process a single row and prepare values for insertion."""
    values = []
    for col in matching_columns:
        value = cleaned_row.get(col, "").strip()
        if not value:  # Handle empty or missing values
            value = None
        values.append(value)
    return values

def insert_data_from_csv(db_params, table_name, csv_file):
    """Main function to insert data from CSV into the database table."""
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Retrieve database table structure
        table_columns = get_table_columns(cursor, table_name)
        logging.info("Database columns: %s", table_columns)

        # Read CSV and clean column names
        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            csv_columns = clean_csv_columns(reader.fieldnames)
            logging.info("CSV columns: %s", csv_columns)

            # Match CSV columns to database columns
            matching_columns = [col for col in csv_columns if col in table_columns]
            logging.info("Matching columns: %s", matching_columns)

            if not matching_columns:
                logging.error("No matching columns between the CSV file and the database table.")
                return

            # Prepare the INSERT query
            insert_query = prepare_insert_query(table_name, matching_columns)

            # Process and insert rows
            for row in reader:
                cleaned_row = clean_row_keys(row)  # Clean row keys
                logging.info("Cleaned row keys: %s", cleaned_row.keys())

                # Skip rows missing required columns
                if not all(col in cleaned_row for col in matching_columns):
                    logging.error("Row missing required columns: %s | Row: %s", matching_columns, cleaned_row)
                    continue

                try:
                    values = process_row(cleaned_row, matching_columns)
                    cursor.execute(insert_query, values)
                except Exception as e:
                    logging.error("Failed to insert row: %s | Reason: %s", cleaned_row, str(e))

        conn.commit()
        logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error("Database operation failed: %s", e)
    finally:
        if conn:
            conn.close()


def fetch_sample_data(db_params, table_name, sample_size=10):
    """Fetch and print a sample of the data from the table """
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        query = f'SELECT * FROM "{table_name}" LIMIT {sample_size}'
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.close()
    except Exception as e:
        logging.error("Failed to fetch data: %s", e)

if __name__ == "__main__":
    #test_connection(db_params)
    delete_all_rows(db_params, table_name)
    insert_data_from_csv(db_params, table_name, csv_file)
    fetch_sample_data(db_params, table_name)
