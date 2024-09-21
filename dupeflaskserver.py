from flask import Flask, render_template, request, g, Response
import sqlite3
import csv
from io import StringIO
from pathlib import Path
from fnmatch import fnmatch

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

def apply_filters(files, exclude_hidden, exclude_small, exclude_patterns):
    filtered_files = []
    for file in files:
        path = file[2]
        size = file[3]

        # Check for hidden directories (directories starting with a dot)
        if exclude_hidden and any(part.startswith('.') for part in Path(path).parts):
            continue

        # Check for files smaller than 10 KB
        if exclude_small and size < 10 * 1024:
            continue

        # Check for patterns to exclude
        if exclude_patterns:
            exclude_patterns_list = [pattern.strip() for pattern in exclude_patterns.split(',') if pattern.strip()]
            if any(fnmatch(path, pattern) for pattern in exclude_patterns_list):
                continue

        filtered_files.append(file)
    
    return filtered_files

def sizeof_fmt(num, suffix="B"):
    """Convert a size in bytes to a human-readable string with appropriate unit."""
    for unit in ["", "K", "M", "G", "T"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} P{suffix}"

def calculate_statistics(files):
    total_bytes = sum(file[3] for file in files)
    total_files = len(files)

    # Calculate top 10 file types by size
    extension_size = {}
    for file in files:
        path = file[2]
        size = file[3]
        suffix = Path(path).suffix.lower()
        extension_size[suffix] = extension_size.get(suffix, 0) + size
    
    top_file_types = sorted(extension_size.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "total_bytes": sizeof_fmt(total_bytes),
        "total_files": total_files,
        "top_file_types": [(ext, sizeof_fmt(size)) for ext, size in top_file_types]
    }

# Register sizeof_fmt function as a template filter
app.jinja_env.filters['sizeof_fmt'] = sizeof_fmt

@app.route('/', methods=['GET'])
def show_duplicates():
    exclude_hidden = request.args.get('exclude_hidden', 'false') == 'true'
    exclude_small = request.args.get('exclude_small', 'false') == 'true'
    exclude_patterns = request.args.get('exclude_patterns', '')

    files = query_db('''
    SELECT * FROM files
    WHERE hash IN 
    (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1)
    ORDER BY hash, path
    ''')

    # Apply filters
    files = apply_filters(files, exclude_hidden, exclude_small, exclude_patterns)
    stats = calculate_statistics(files)

    return render_template('files.html', files=files, stats=stats, title="Duplicate Files", search_route="duplicates",
                           exclude_hidden=exclude_hidden, exclude_small=exclude_small, exclude_patterns=exclude_patterns)

@app.route('/search', methods=['GET'])
def search_files():
    search_query = request.args.get('search', '').strip()
    exclude_hidden = request.args.get('exclude_hidden', 'false') == 'true'
    exclude_small = request.args.get('exclude_small', 'false') == 'true'
    exclude_patterns = request.args.get('exclude_patterns', '')

    if search_query:
        files = query_db('''
        SELECT * FROM files
        WHERE hash LIKE ? OR path LIKE ?
        ''', (f'%{search_query}%', f'%{search_query}%'))
    else:
        files = query_db('SELECT * FROM files')

    # Apply filters
    files = apply_filters(files, exclude_hidden, exclude_small, exclude_patterns)
    stats = calculate_statistics(files)

    return render_template('files.html', files=files, stats=stats, title="Search Files", search_route="search",
                           search_query=search_query, exclude_hidden=exclude_hidden, exclude_small=exclude_small, exclude_patterns=exclude_patterns)

@app.route('/download', methods=['GET'])
def download_csv():
    files = query_db('''
    SELECT hash, path, size, last_modified, last_checked FROM files
    WHERE hash IN 
    (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1)
    ''')

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['hash', 'path', 'size', 'last_modified', 'last_checked'])
    for file in files:
        writer.writerow(file)

    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers['Content-Disposition'] = 'attachment; filename=duplicates.csv'
    return response

if __name__ == '__main__':
    app.run(debug=True)