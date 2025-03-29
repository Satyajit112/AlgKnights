from flask import Flask, request, jsonify
import trino
from trino.dbapi import connect
from trino.exceptions import TrinoUserError
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Global connection object
conn = None
cur = None

@app.route('/api/connect', methods=['POST'])
def create_connection():
    """API endpoint to establish a connection to Trino server"""
    global conn, cur
    
    data = request.json
    host = data.get('host', 'localhost')
    port = int(data.get('port', 8080))
    user = data.get('user', '')
    catalog = data.get('catalog', 'memory')
    schema = data.get('schema', 'default')
    
    try:
        conn = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
        )
        cur = conn.cursor()
        return jsonify({
            'status': 'success',
            'message': f"Connected to Trino at {host}:{port} as user '{user}' using catalog '{catalog}' and schema '{schema}'."
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Failed to connect to Trino: {str(e)}"
        }), 500

@app.route('/api/create-table', methods=['POST'])
def create_table():
    """API endpoint to create a table"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    table_name = data.get('table_name', '')
    columns = data.get('columns', '')
    
    query = f"CREATE TABLE {table_name} ({columns})"
    
    try:
        cur.execute(query)
        return jsonify({
            'status': 'success',
            'message': f"Table '{table_name}' created successfully."
        })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error creating table: {str(e)}"
        }), 400

@app.route('/api/insert-data', methods=['POST'])
def insert_data():
    """API endpoint to insert data into a table"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    table_name = data.get('table_name', '')
    columns = data.get('columns', '')
    values = data.get('values', '')
    
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
        return jsonify({
            'status': 'success',
            'message': f"Data inserted into '{table_name}' successfully."
        })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error inserting data: {str(e)}"
        }), 400

@app.route('/api/update-data', methods=['POST'])
def update_data():
    """API endpoint to update data in a table"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    table_name = data.get('table_name', '')
    set_clause = data.get('set_clause', '')
    condition = data.get('condition', '')
    
    query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
    
    try:
        cur.execute(query)
        return jsonify({
            'status': 'success',
            'message': f"Data in '{table_name}' updated successfully."
        })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error updating data: {str(e)}"
        }), 400

@app.route('/api/delete-data', methods=['POST'])
def delete_data():
    """API endpoint to delete data from a table"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    table_name = data.get('table_name', '')
    condition = data.get('condition', '')
    
    query = f"DELETE FROM {table_name} WHERE {condition}"
    
    try:
        cur.execute(query)
        return jsonify({
            'status': 'success',
            'message': f"Data deleted from '{table_name}' successfully."
        })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error deleting data: {str(e)}"
        }), 400

@app.route('/api/show-tables', methods=['GET'])
def show_tables():
    """API endpoint to show all tables in the current schema"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    query = "SHOW TABLES"
    
    try:
        cur.execute(query)
        tables = cur.fetchall()
        table_list = [table[0] for table in tables]
        
        return jsonify({
            'status': 'success',
            'tables': table_list
        })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving tables: {str(e)}"
        }), 400

@app.route('/api/show-create-table', methods=['POST'])
def show_create_table():
    """API endpoint to show the CREATE TABLE statement for a specified table"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    table_name = data.get('table_name', '')
    query = f"SHOW CREATE TABLE {table_name}"
    
    try:
        cur.execute(query)
        create_table_stmt = cur.fetchone()[0]
        
        return jsonify({
            'status': 'success',
            'create_statement': create_table_stmt
        })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving CREATE TABLE statement: {str(e)}"
        }), 400

@app.route('/api/show-table-contents', methods=['POST'])
def show_table_contents():
    """API endpoint to show all records from a specified table"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    table_name = data.get('table_name', '')
    query = f"SELECT * FROM {table_name}"
    
    try:
        cur.execute(query)
        rows = cur.fetchall()
        
        if cur.description:
            column_names = [desc[0] for desc in cur.description]
            formatted_rows = []
            
            for row in rows:
                formatted_row = {}
                for i, cell in enumerate(row):
                    formatted_row[column_names[i]] = cell
                formatted_rows.append(formatted_row)
            
            return jsonify({
                'status': 'success',
                'columns': column_names,
                'rows': formatted_rows
            })
        else:
            return jsonify({
                'status': 'success',
                'message': f"No data found in table '{table_name}'."
            })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving data from '{table_name}': {str(e)}"
        }), 400

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    """API endpoint to execute a SQL query"""
    global cur
    
    if not cur:
        return jsonify({
            'status': 'error',
            'message': 'No active Trino connection. Please connect first.'
        }), 400
    
    data = request.json
    query = data.get('query', '')
    
    try:
        cur.execute(query)
        
        if cur.description:
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            formatted_rows = []
            
            for row in rows:
                formatted_row = {}
                for i, cell in enumerate(row):
                    formatted_row[column_names[i]] = cell
                formatted_rows.append(formatted_row)
            
            return jsonify({
                'status': 'success',
                'columns': column_names,
                'rows': formatted_rows
            })
        else:
            return jsonify({
                'status': 'success',
                'message': "Query executed successfully."
            })
    except TrinoUserError as e:
        return jsonify({
            'status': 'error',
            'message': f"Error executing query: {str(e)}"
        }), 400

if __name__ == "__main__":
    app.run(debug=True)