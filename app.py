from flask import Flask, request, jsonify, render_template
from trino.dbapi import connect
from trino.exceptions import TrinoUserError

app = Flask(__name__)

def create_connection():
    """
    Establishes a connection to the Trino server.
    """
    try:
        conn = connect(
            host='localhost',
            port=8080,
            user='satyajit112',
            catalog='memory',
            schema='default',
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to Trino: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/create_table', methods=['POST'])
def create_table():
    data = request.json
    table_name = data.get('table_name')
    columns = data.get('columns')
    query = f"CREATE TABLE {table_name} ({columns})"
    conn = create_connection()
    if conn:
        cur = conn.cursor()
        try:
            cur.execute(query)
            return jsonify({"message": f"Table '{table_name}' created successfully."})
        except TrinoUserError as e:
            return jsonify({"error": str(e)})
    return jsonify({"error": "Connection to Trino failed."})

@app.route('/insert_data', methods=['POST'])
def insert_data():
    data = request.json
    table_name = data.get('table_name')
    columns = data.get('columns')
    values = data.get('values')
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    conn = create_connection()
    if conn:
        cur = conn.cursor()
        try:
            cur.execute(query)
            return jsonify({"message": f"Data inserted into '{table_name}' successfully."})
        except TrinoUserError as e:
            return jsonify({"error": str(e)})
    return jsonify({"error": "Connection to Trino failed."})

@app.route('/execute_query', methods=['POST'])
def execute_query():
    data = request.json
    query = data.get('query')
    conn = create_connection()
    if conn:
        cur = conn.cursor()
        try:
            cur.execute(query)
            rows = cur.fetchall()
            return jsonify({"result": rows})
        except TrinoUserError as e:
            return jsonify({"error": str(e)})
    return jsonify({"error": "Connection to Trino failed."})

if __name__ == '__main__':
    app.run(debug=True)
