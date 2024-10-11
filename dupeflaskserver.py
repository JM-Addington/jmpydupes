from flask import Flask, render_template, request, g, Response
import sqlite3
import csv
from io import StringIO
from pathlib import Path
from fnmatch import fnmatch

app = Flask(__name__)

DATABASE = 'file_data.db'
MAX_FILES_RETURNED = 1000

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

SIZE_THRESHOLDS_ORDERED = ["1mb", "10mb", "100mb", "1tb"]

SIZE_THRESHOLDS = {
    '1mb': 1 * 1024 * 1024,
    '10mb': 10 * 1024 * 1024,
    '100mb': 100 * 1024 * 1024,
    '1tb': 1 * 1024 * 1024 * 1024 * 1024
}

def apply_size_filter(files, size_filter):
    if not size_filter:
        return files
    
    threshold = SIZE_THRESHOLDS.get(size_filter)
    if not threshold:
        return files
    
    return [file for file in files if file[3] >= threshold]

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

def get_sort_column_and_order(sort_by, direction):
    # Map the sort_by string to actual database field names and determine the order
    sort_columns = {
        'size': 'size',
        'hash': 'hash',
        'path': 'path'
    }
    sort_column = sort_columns.get(sort_by, 'hash')  # Default to hash if invalid input
    order = 'ASC' if direction == 'asc' else 'DESC'
    return sort_column, order

def generate_sort_urls(route, current_sort, current_direction):
    toggle_direction = 'desc' if current_direction == 'asc' else 'asc'
    return {
        'hash': f"{route}?sort_by=hash&direction={toggle_direction}",
        'path': f"{route}?sort_by=path&direction={toggle_direction}",
        'size': f"{route}?sort_by=size&direction={toggle_direction}"
    }

@app.route('/', methods=['GET'])
def show_duplicates():
    exclude_hidden = request.args.get('exclude_hidden', 'true') == 'true'
    exclude_small = request.args.get('exclude_small', 'true') == 'true'
    exclude_patterns = request.args.get('exclude_patterns', '')
    size_filter = request.args.get('size_filter', '')
    sort_by = request.args.get('sort_by', 'hash')
    direction = request.args.get('direction', 'asc')

    sort_column, order = get_sort_column_and_order(sort_by, direction)

    query = f'''
    SELECT * FROM files
    WHERE hash IN 
    (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1)
    ORDER BY {sort_column} {order}, hash, path
    LIMIT {MAX_FILES_RETURNED}
    '''

    files = query_db(query)
    files = apply_filters(files, exclude_hidden, exclude_small, exclude_patterns)
    files = apply_size_filter(files, size_filter)
    stats = calculate_statistics(files)
    
    sort_urls = generate_sort_urls("/", sort_by, direction)

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('file_list.html', files=files)

    return render_template('files.html', files=files, stats=stats, title="Duplicate Files", search_route="/",
                           exclude_hidden=exclude_hidden, exclude_small=exclude_small, exclude_patterns=exclude_patterns,
                           sort_by=sort_by, direction=direction, sort_urls=sort_urls, selected_size_filter=size_filter)
    

@app.route('/search', methods=['GET'])
def search_files():   
    search_query = request.args.get('search', '').strip()
    exclude_hidden = request.args.get('exclude_hidden', 'true') == 'true'
    exclude_small = request.args.get('exclude_small', 'true') == 'true'
    exclude_patterns = request.args.get('exclude_patterns', '')
    sort_by = request.args.get('sort_by', 'hash')
    direction = request.args.get('direction', 'asc')

    sort_column, order = get_sort_column_and_order(sort_by, direction)

    # Modified query to include only duplicate files
    initial_query = '''
    SELECT * FROM files
    WHERE hash IN 
        (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1)
    AND (hash LIKE ? OR path LIKE ?)
    ORDER BY {sort_column} {order}, hash, path
    '''.format(sort_column=sort_column, order=order)
    
    files = query_db(initial_query, (f'%{search_query}%', f'%{search_query}%'))
    
    # Apply initial non-size filters
    files = apply_filters(files, exclude_hidden, exclude_small, exclude_patterns)
    
    # Automatically apply size filters if necessary
    applied_size_filter = ''
    for size_filter in ["1mb", "10mb", "100mb", "1tb"]:
        if len(files) <= MAX_FILES_RETURNED:
            break
        applied_size_filter = size_filter
        files = apply_size_filter(files, size_filter)

    # Ensure result is not more than MAX_FILES_RETURNED
    if len(files) > MAX_FILES_RETURNED:
        files = files[:MAX_FILES_RETURNED]
    
    stats = calculate_statistics(files)
    
    sort_urls = generate_sort_urls("/search", sort_by, direction)

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('templates/file_list.html', files=files)

    return render_template('files.html', files=files, stats=stats, title="Search Files", search_route="/search",
                           search_query=search_query, exclude_hidden=exclude_hidden, exclude_small=exclude_small,
                           exclude_patterns=exclude_patterns, sort_by=sort_by, direction=direction,
                           sort_urls=sort_urls, selected_size_filter=applied_size_filter)
    
@app.route('/open_in_finder', methods=['POST'])
def open_in_finder():
    if sys.platform != "darwin":  # Check if the system is macOS
        return jsonify({'message': "Open in Finder is only supported on macOS."}), 400

    data = request.get_json()
    file_path = data.get('path')

    if not file_path:
        return jsonify({'message': "Invalid file path."}), 400

    # Use the 'open' command on macOS to open Finder at the file's location
    try:
        os.system(f"open -R '{file_path}'")
        return jsonify({'message': "Opened in Finder successfully."})
    except Exception as e:
        return jsonify({'message': f"Error opening Finder: {str(e)}"}), 500

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