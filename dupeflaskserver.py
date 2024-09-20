from flask import Flask, render_template, request, g
import sqlite3

app = Flask(__name__)

DATABASE = 'file_data.db'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

# Route for displaying all duplicate files
@app.route('/', methods=['GET'])
def show_duplicates():
    # Find all files with duplicate hashes
    files = query_db('''
    SELECT * FROM files
    WHERE hash IN 
    (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1)
    ORDER BY hash, path
    ''')
    return render_template('files.html', files=files, title="Duplicate Files", search_route="duplicates")

# Route for searching any file by name or hash
@app.route('/search', methods=['GET'])
def search_files():
    search_query = request.args.get('search', '').strip()
    if search_query:
        # Search by hash or filename
        files = query_db('''
        SELECT * FROM files
        WHERE hash LIKE ? OR path LIKE ?
        ''', (f'%{search_query}%', f'%{search_query}%'))
    else:
        files = query_db('SELECT * FROM files')
    
    return render_template('files.html', files=files, title="Search Files", search_route="search", search_query=search_query)

# Route to download CSV of all duplicate files.
# The CSV will return the hash, path, size, last_modified, and last_checked fields.
@app.route('/download', methods=['GET'])
def download_csv():
    import csv
    from io import StringIO
    from flask import Response
    
    # Find all files with duplicate hashes
    files = query_db('''
    SELECT hash, path, size, last_modified, last_checked FROM files
    WHERE hash IN 
    (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1)
    ''')
    
    # Create a CSV file in memory
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['hash', 'path', 'size', 'last_modified', 'last_checked'])
    for file in files:
        writer.writerow(file)
    
    # Return the CSV file as a download
    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers['Content-Disposition'] = 'attachment; filename=duplicates.csv'
    return response

if __name__ == '__main__':
    app.run(debug=True)