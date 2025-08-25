import os
import ibm_db
from flask import Flask, request, jsonify
from dotenv import load_dotenv

app = Flask(__name__)

# Load environment variables
load_dotenv()

# DB2 connection variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Connection string
CONN_STR = f"DATABASE={DB_NAME};HOSTNAME={DB_HOST};PORT={DB_PORT};PROTOCOL=TCPIP;UID={DB_USERNAME};PWD={DB_PASSWORD};"

def fallback_response(service_name, error_msg=None):
    resp = {"error": f"{service_name} service unavailable", "fallback": True}
    if error_msg:
        resp["details"] = error_msg
    return resp

def get_db_connection():
    """Get DB2 connection"""
    try:
        conn = ibm_db.connect(CONN_STR, "", "")
        return conn
    except Exception as e:
        app.logger.error(f"DB connection failed: {e}")
        return None

@app.route("/live", methods=["GET"])
def live():
    return jsonify(status="ok"), 200

@app.route("/db/test", methods=["GET"])
def test_connection():
    """Test DB2 connection"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify(fallback_response("DB2", "Connection failed")), 500
        
        # Test query
        sql = "SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1"
        stmt = ibm_db.exec_immediate(conn, sql)
        result = ibm_db.fetch_tuple(stmt)
        
        ibm_db.close(conn)
        return jsonify({
            "status": "connected", 
            "database": DB_NAME,
            "current_date": str(result[0])
        }), 200
        
    except Exception as e:
        app.logger.error(f"DB test failed: {e}")
        return jsonify(fallback_response("DB2", str(e))), 500

@app.route("/db/query", methods=["POST"])
def execute_query():
    """Execute SQL query"""
    try:
        data = request.get_json()
        sql = data.get("sql", "")
        
        if not sql:
            return jsonify({"error": "No SQL query provided"}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify(fallback_response("DB2", "Connection failed")), 500
        
        stmt = ibm_db.exec_immediate(conn, sql)
        
        # Fetch results
        results = []
        while ibm_db.fetch_row(stmt):
            row = {}
            for i in range(ibm_db.num_fields(stmt)):
                field_name = ibm_db.field_name(stmt, i)
                field_value = ibm_db.result(stmt, i)
                row[field_name] = field_value
            results.append(row)
        
        ibm_db.close(conn)
        return jsonify({
            "sql": sql,
            "row_count": len(results),
            "results": results
        }), 200
        
    except Exception as e:
        app.logger.error(f"Query execution failed: {e}")
        return jsonify(fallback_response("DB2", str(e))), 500

@app.route("/db/tables", methods=["GET"])
def list_tables():
    """List all tables in the database"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify(fallback_response("DB2", "Connection failed")), 500
        
        sql = """
        SELECT TABSCHEMA, TABNAME, TYPE 
        FROM SYSCAT.TABLES 
        WHERE TABSCHEMA NOT IN ('SYSIBM', 'SYSCAT', 'SYSSTAT', 'SYSFUN', 'SYSPROC')
        ORDER BY TABSCHEMA, TABNAME
        """
        
        stmt = ibm_db.exec_immediate(conn, sql)
        tables = []
        
        while ibm_db.fetch_row(stmt):
            tables.append({
                "schema": ibm_db.result(stmt, 0),
                "table": ibm_db.result(stmt, 1),
                "type": ibm_db.result(stmt, 2)
            })
        
        ibm_db.close(conn)
        return jsonify({
            "table_count": len(tables),
            "tables": tables
        }), 200
        
    except Exception as e:
        app.logger.error(f"List tables failed: {e}")
        return jsonify(fallback_response("DB2", str(e))), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)