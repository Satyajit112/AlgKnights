import trino
from trino.dbapi import connect
from trino.exceptions import TrinoUserError

def create_connection():
    """
    Establishes a connection to the Trino server with dynamic schema selection.
    """
    host = input("Enter Trino server host (default: localhost): ") or 'localhost'
    port = input("Enter Trino server port (default: 8080): ") or 8080
    user = input("Enter your Trino username: ")
    catalog = input("Enter catalog (default: memory): ") or 'memory'
    schema = input("Enter schema (default: default): ") or 'default'

    try:
        conn = connect(
            host=host,
            port=int(port),
            user=user,
            catalog=catalog,
            schema=schema,
        )
        print(f"Connected to Trino at {host}:{port} as user '{user}' using catalog '{catalog}' and schema '{schema}'.")
        return conn
    except Exception as e:
        print(f"Failed to connect to Trino: {e}")
        return None

def create_table(cur):
    """
    Prompts the user for a table name and columns, then creates the table.
    """
    table_name = input("Enter table name: ")
    columns = input("Enter columns (name type, comma-separated): ")

    query = f"CREATE TABLE {table_name} ({columns})"

    try:
        cur.execute(query)
        print(f"Table '{table_name}' created successfully.")
    except TrinoUserError as e:
        print(f"Error creating table: {e}")

def insert_data(cur):
    """
    Prompts the user for table name, columns, and values, then inserts the data into the specified table.
    """
    table_name = input("Enter table name: ")
    columns = input("Enter columns to insert into (comma-separated): ")
    values = input("Enter values to insert (comma-separated): ")

    # Format values for SQL insertion
    formatted_values = []
    for value in values.split(','):
        value = value.strip()
        # Check if value is numeric
        try:
            float(value)
            formatted_values.append(value)
        except ValueError:
            # Non-numeric value, add quotes
            formatted_values.append(f"'{value}'")
    
    formatted_values_str = ', '.join(formatted_values)

    query = f"INSERT INTO {table_name} ({columns}) VALUES ({formatted_values_str})"

    try:
        cur.execute(query)
        print(f"Data inserted into '{table_name}' successfully.")
    except TrinoUserError as e:
        print(f"Error inserting data: {e}")

def update_data(cur):
    """
    Prompts the user for table name, set clause, and condition, then updates the specified rows in the table.
    """
    table_name = input("Enter table name: ")
    set_clause = input("Enter SET clause (e.g., column1 = value1, column2 = value2): ")
    condition = input("Enter condition (e.g., id = 1): ")

    query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"

    try:
        cur.execute(query)
        print(f"Data in '{table_name}' updated successfully.")
    except TrinoUserError as e:
        print(f"Error updating data: {e}")

def delete_data(cur):
    """
    Prompts the user for table name and condition, then deletes the specified rows from the table.
    """
    table_name = input("Enter table name: ")
    condition = input("Enter condition for deletion (e.g., id = 1): ")

    query = f"DELETE FROM {table_name} WHERE {condition}"

    try:
        cur.execute(query)
        print(f"Data deleted from '{table_name}' successfully.")
    except TrinoUserError as e:
        print(f"Error deleting data: {e}")

def show_tables(cur):
    """
    Displays all tables in the current schema.
    """
    query = "SHOW TABLES"

    try:
        cur.execute(query)
        tables = cur.fetchall()
        if tables:
            print("Tables in the current schema:")
            for table in tables:
                print(table[0])
        else:
            print("No tables found in the current schema.")
    except TrinoUserError as e:
        print(f"Error retrieving tables: {e}")

def show_create_table(cur):
    """
    Displays the CREATE TABLE statement for the specified table.
    """
    table_name = input("Enter table name: ")
    query = f"SHOW CREATE TABLE {table_name}"

    try:
        cur.execute(query)
        create_table_stmt = cur.fetchone()[0]
        print(f"CREATE TABLE statement for '{table_name}':\n{create_table_stmt}")
    except TrinoUserError as e:
        print(f"Error retrieving CREATE TABLE statement: {e}")

def show_table_contents(cur):
    """
    Displays all records from the specified table.
    """
    table_name = input("Enter table name to display contents: ")
    query = f"SELECT * FROM {table_name}"

    try:
        cur.execute(query)
        rows = cur.fetchall()
        if rows:
            # Fetch column names
            column_names = [desc[0] for desc in cur.description]
            # Print column headers
            print("\t".join(column_names))
            # Print rows
            for row in rows:
                print("\t".join(str(cell) for cell in row))
        else:
            print(f"No data found in table '{table_name}'.")
    except TrinoUserError as e:
        print(f"Error retrieving data from '{table_name}': {e}")

def execute_query(cur):
    """
    Prompts the user for a SQL query, executes it, and displays the results.
    """
    query = input("Enter the SQL query to execute: ")

    try:
        cur.execute(query)
        if cur.description:
            rows = cur.fetchall()
            if rows:
                # Fetch column names
                column_names = [desc[0] for desc in cur.description]
                # Print column headers
                print("\t".join(column_names))
                # Print rows
                for row in rows:
                    print("\t".join(str(cell) for cell in row))
            else:
                print("Query executed successfully, but no data was returned.")
        else:
            print("Query executed successfully.")
    except TrinoUserError as e:
        print(f"Error executing query: {e}")

def main():
    conn = create_connection()
    if conn is None:
        return

    cur = conn.cursor()

    while True:
        print("\n1. Create Table")
        print("2. Insert Data")
        print("3. Update Data")
        print("4. Delete Data")
        print("5. Show Tables")
        print("6. Show Create Table")
        print("7. Show Table Contents")
        print("8. Execute SQL Query")
        print("9. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            create_table(cur)
        elif choice == '2':
            insert_data(cur)
        elif choice == '3':
            update_data(cur)
        elif choice == '4':
            delete_data(cur)
        elif choice == '5':
            show_tables(cur)
        elif choice == '6':
            show_create_table(cur)
        elif choice == '7':
            show_table_contents(cur)
        elif choice == '8':
            execute_query(cur)
        elif choice == '9':
            print("Exiting program.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()