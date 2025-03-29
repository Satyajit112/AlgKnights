from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, ParseException

def create_connection():
    """
    Establishes a connection to Spark with dynamic configuration options.
    """
    app_name = input("Enter Spark application name (default: SparkCLI): ") or 'SparkCLI'
    master = input("Enter Spark master URL (default: local[*]): ") or 'local[*]'
    
    # Optional configurations
    configs = {}
    add_config = input("Do you want to add additional Spark configurations? (y/n): ").lower()
    
    while add_config == 'y':
        key = input("Enter configuration key: ")
        value = input("Enter configuration value: ")
        configs[key] = value
        add_config = input("Add another configuration? (y/n): ").lower()
    
    try:
        # Create a SparkSession builder
        builder = SparkSession.builder.appName(app_name).master(master)
        
        # Add any additional configurations
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        # Create the SparkSession
        spark = builder.getOrCreate()
        
        print(f"Connected to Spark with application name '{app_name}' and master '{master}'.")
        return spark
    except Exception as e:
        print(f"Failed to connect to Spark: {e}")
        return None

def create_table(spark):
    """
    Creates a table in Spark SQL from user input or from a data source.
    """
    table_name = input("Enter table name: ")
    
    table_source = input("Create table from (1) user defined schema or (2) data source? (1/2): ")
    
    if table_source == '1':
        # Create table with user-defined schema
        columns = input("Enter columns (name type, comma-separated): ")
        
        # Format the schema for Spark SQL
        schema_parts = []
        for col_def in columns.split(','):
            col_def = col_def.strip()
            if col_def:
                name_type = col_def.split()
                if len(name_type) >= 2:
                    col_name = name_type[0]
                    col_type = ' '.join(name_type[1:])
                    schema_parts.append(f"`{col_name}` {col_type}")
        
        schema_str = ', '.join(schema_parts)
        query = f"CREATE TABLE {table_name} ({schema_str})"
        
        try:
            spark.sql(query)
            print(f"Table '{table_name}' created successfully.")
        except (AnalysisException, ParseException) as e:
            print(f"Error creating table: {e}")
    
    elif table_source == '2':
        # Create table from data source
        source_type = input("Enter data source type (csv, json, parquet, etc.): ")
        path = input("Enter path to data source: ")
        header = input("Does the data have a header? (true/false, default: true): ") or 'true'
        infer_schema = input("Infer schema? (true/false, default: true): ") or 'true'
        
        try:
            # Read the data from the source
            if source_type == 'csv':
                df = spark.read.format('csv').option("header", header).option("inferSchema", infer_schema).load(path)
            elif source_type == 'json':
                df = spark.read.json(path)
            elif source_type == 'parquet':
                df = spark.read.parquet(path)
            else:
                df = spark.read.format(source_type).load(path)
            
            # Create a temporary view or table
            save_mode = input("Create table as (1) temporary view or (2) persistent table? (1/2): ")
            
            if save_mode == '1':
                df.createOrReplaceTempView(table_name)
                print(f"Temporary view '{table_name}' created successfully.")
            else:
                save_format = input("Save table in format (parquet, orc, etc., default: parquet): ") or 'parquet'
                df.write.format(save_format).saveAsTable(table_name)
                print(f"Persistent table '{table_name}' created successfully.")
                
        except Exception as e:
            print(f"Error creating table from data source: {e}")
    else:
        print("Invalid option selected.")

def insert_data(spark):
    """
    Inserts data into a specified table.
    """
    table_name = input("Enter table name: ")
    
    # Check if the table exists
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}").show()
    except (AnalysisException, ParseException):
        print(f"Table '{table_name}' does not exist.")
        return
    
    insert_mode = input("Insert (1) single row or (2) multiple rows? (1/2): ")
    
    if insert_mode == '1':
        # Single row insert
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
        
    elif insert_mode == '2':
        # Multiple rows insert (using DataFrame)
        print("For multiple rows, we'll create a temporary DataFrame.")
        
        # Get the data from the user
        data_rows = []
        col_names = input("Enter column names (comma-separated): ").split(',')
        col_names = [col.strip() for col in col_names]
        
        num_rows = int(input("How many rows to insert? "))
        
        for i in range(num_rows):
            print(f"Enter data for row {i+1}:")
            row_values = []
            
            for col in col_names:
                value = input(f"  Value for '{col}': ")
                row_values.append(value)
                
            data_rows.append(row_values)
        
        # Create a DataFrame from the collected data
        df = spark.createDataFrame(data_rows, col_names)
        
        # Insert the data
        try:
            df.write.insertInto(table_name)
            print(f"Data inserted into '{table_name}' successfully.")
            return
        except Exception as e:
            print(f"Error inserting data: {e}")
            return
    else:
        print("Invalid option selected.")
        return
    
    try:
        spark.sql(query)
        print(f"Data inserted into '{table_name}' successfully.")
    except (AnalysisException, ParseException) as e:
        print(f"Error inserting data: {e}")

def update_data(spark):
    """
    Updates data in a specified table.
    """
    table_name = input("Enter table name: ")
    set_clause = input("Enter SET clause (e.g., column1 = value1, column2 = value2): ")
    condition = input("Enter condition (e.g., id = 1): ")
    
    query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
    
    try:
        spark.sql(query)
        print(f"Data in '{table_name}' updated successfully.")
    except (AnalysisException, ParseException) as e:
        print(f"Error updating data: {e}")

def delete_data(spark):
    """
    Deletes data from a specified table.
    """
    table_name = input("Enter table name: ")
    condition = input("Enter condition for deletion (e.g., id = 1): ")
    
    query = f"DELETE FROM {table_name} WHERE {condition}"
    
    try:
        spark.sql(query)
        print(f"Data deleted from '{table_name}' successfully.")
    except (AnalysisException, ParseException) as e:
        print(f"Error deleting data: {e}")

def show_tables(spark):
    """
    Displays all tables in the current database.
    """
    try:
        print("Tables in the current database:")
        spark.sql("SHOW TABLES").show(truncate=False)
    except Exception as e:
        print(f"Error retrieving tables: {e}")

def show_databases(spark):
    """
    Displays all available databases.
    """
    try:
        print("Available databases:")
        spark.sql("SHOW DATABASES").show(truncate=False)
    except Exception as e:
        print(f"Error retrieving databases: {e}")

def use_database(spark):
    """
    Switches to a different database.
    """
    database = input("Enter database name to use: ")
    
    try:
        spark.sql(f"USE {database}")
        print(f"Switched to database '{database}'.")
    except (AnalysisException, ParseException) as e:
        print(f"Error switching to database: {e}")

def show_table_contents(spark):
    """
    Displays the contents of a specified table.
    """
    table_name = input("Enter table name to display contents: ")
    limit = input("Enter maximum number of rows to display (default: 20): ") or 20
    
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    
    try:
        result = spark.sql(query)
        print(f"Contents of table '{table_name}':")
        result.show(int(limit), truncate=False)
    except (AnalysisException, ParseException) as e:
        print(f"Error retrieving table contents: {e}")

def describe_table(spark):
    """
    Displays the schema of a specified table.
    """
    table_name = input("Enter table name to describe: ")
    
    try:
        print(f"Schema for table '{table_name}':")
        spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)
    except (AnalysisException, ParseException) as e:
        print(f"Error describing table: {e}")

def execute_query(spark):
    """
    Executes a custom SQL query.
    """
    query = input("Enter the SQL query to execute: ")
    
    try:
        result = spark.sql(query)
        
        # Check if the query returns a result
        if result:
            max_rows = input("Enter maximum number of rows to display (default: 20): ") or 20
            print("Query result:")
            result.show(int(max_rows), truncate=False)
        else:
            print("Query executed successfully.")
    except (AnalysisException, ParseException) as e:
        print(f"Error executing query: {e}")

def main():
    spark = create_connection()
    if spark is None:
        return
    
    while True:
        print("\n===== Spark SQL Interactive CLI =====")
        print("1. Create Table")
        print("2. Insert Data")
        print("3. Update Data")
        print("4. Delete Data")
        print("5. Show Tables")
        print("6. Show Databases")
        print("7. Use Database")
        print("8. Describe Table")
        print("9. Show Table Contents")
        print("10. Execute SQL Query")
        print("11. Exit")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            create_table(spark)
        elif choice == '2':
            insert_data(spark)
        elif choice == '3':
            update_data(spark)
        elif choice == '4':
            delete_data(spark)
        elif choice == '5':
            show_tables(spark)
        elif choice == '6':
            show_databases(spark)
        elif choice == '7':
            use_database(spark)
        elif choice == '8':
            describe_table(spark)
        elif choice == '9':
            show_table_contents(spark)
        elif choice == '10':
            execute_query(spark)
        elif choice == '11':
            print("Stopping Spark session...")
            spark.stop()
            print("Exiting program.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()