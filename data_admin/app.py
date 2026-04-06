import os
import secrets
import psycopg2
from flask import Flask, jsonify, request, session, redirect, url_for

from functools import wraps

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', secrets.token_hex(32))

ADMIN_USER = os.getenv('ADMIN_USER', 'admin')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'admin')

DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'heart360tk_database'),
    'user': os.getenv('POSTGRES_USER', 'heart360tk'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
}

SCHEMA = 'heart360tk_schema'
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY', 'h360tk-admin-clear-key')

TABLES = [
    'blood_pressures',
    'blood_sugars',
    'scheduled_visits',
    'call_results',
    'encounters',
    'patients',
    'org_units',
]


@app.after_request
def add_cors_headers(response):
    origin = request.headers.get('Origin', '*')
    response.headers['Access-Control-Allow-Origin'] = origin
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-Admin-Key'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response


@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    return '', 204


def login_required(f):
    """Redirect to login page if not authenticated (for browser routes)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def require_auth(f):
    """Require session auth or API key for API endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('authenticated'):
            return f(*args, **kwargs)
        key = request.headers.get('X-Admin-Key', '')
        if key == ADMIN_API_KEY:
            return f(*args, **kwargs)
        return jsonify({'error': 'Authentication required.'}), 401
    return decorated


def require_api_key(f):
    """Require session auth or API key for destructive operations."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('authenticated'):
            return f(*args, **kwargs)
        key = request.headers.get('X-Admin-Key', '')
        if key == ADMIN_API_KEY:
            return f(*args, **kwargs)
        return jsonify({'error': 'Invalid or missing admin key.'}), 403
    return decorated


def get_connection():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    return conn


def get_table_counts():
    counts = {}
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SET search_path TO {SCHEMA};")
        for table in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            counts[table] = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception as e:
        return None, str(e)
    return counts, None


# ── Auth routes ──────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if username == ADMIN_USER and password == ADMIN_PASSWORD:
            session['authenticated'] = True
            session['username'] = username
            return redirect(url_for('index'))
        return LOGIN_HTML.replace('<!--ERROR-->', '<div class="error">Invalid username or password</div>')
    if session.get('authenticated'):
        return redirect(url_for('index'))
    return LOGIN_HTML


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ── API routes ───────────────────────────────────────────────────────────────

@app.route('/api/table-counts')
@require_auth
def table_counts():
    counts, err = get_table_counts()
    if err:
        return jsonify({'error': err}), 500
    return jsonify(counts)


@app.route('/api/clear-data', methods=['POST'])
@require_api_key
def clear_data():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SET search_path TO {SCHEMA};")
        cur.execute(
            "TRUNCATE TABLE blood_pressures, blood_sugars, scheduled_visits, "
            "call_results, encounters, patients, org_units CASCADE;"
        )
        for table in TABLES:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', "
                f"(SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='{SCHEMA}' AND table_name='{table}' "
                f"AND column_default LIKE 'nextval%%' LIMIT 1)), 1, false) "
                f"WHERE pg_get_serial_sequence('{table}', "
                f"(SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='{SCHEMA}' AND table_name='{table}' "
                f"AND column_default LIKE 'nextval%%' LIMIT 1)) IS NOT NULL;"
            )
        cur.close()
        conn.close()
        return jsonify({'status': 'ok', 'message': 'All patient data has been cleared.'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Page routes ──────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    return ADMIN_HTML


# ── Templates ────────────────────────────────────────────────────────────────

LOGIN_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HEARTS360 &mdash; Login</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: #f0f2f5; color: #1a1a2e; min-height: 100vh;
    display: flex; align-items: center; justify-content: center;
  }
  .card {
    background: #fff; border-radius: 16px; box-shadow: 0 4px 24px rgba(0,0,0,.08);
    max-width: 400px; width: 100%; padding: 40px 36px; text-align: center;
  }
  .logo { font-size: 28px; font-weight: 700; color: #0b5ed7; margin-bottom: 4px; }
  .subtitle { font-size: 14px; color: #6c757d; margin-bottom: 28px; }
  .form-group { margin-bottom: 18px; text-align: left; }
  label { display: block; font-size: 13px; font-weight: 600; color: #555; margin-bottom: 6px; }
  input[type="text"], input[type="password"] {
    width: 100%; padding: 12px 14px; border: 1px solid #dee2e6; border-radius: 10px;
    font-size: 15px; outline: none; transition: border-color .2s;
  }
  input[type="text"]:focus, input[type="password"]:focus { border-color: #0b5ed7; }
  .btn {
    display: inline-block; width: 100%; padding: 14px; border: none; border-radius: 10px;
    font-size: 16px; font-weight: 600; cursor: pointer; transition: all .2s;
    background: #0b5ed7; color: #fff; margin-top: 6px;
  }
  .btn:hover { background: #0a4fb8; }
  .error {
    background: #f8d7da; color: #842029; padding: 10px 14px; border-radius: 8px;
    font-size: 13px; margin-bottom: 18px;
  }
</style>
</head>
<body>
<div class="card">
  <div class="logo">HEARTS360</div>
  <div class="subtitle">Data Administration &mdash; Sign In</div>
  <!--ERROR-->
  <form method="POST" action="/login">
    <div class="form-group">
      <label for="username">Username</label>
      <input type="text" id="username" name="username" required autocomplete="username" autofocus>
    </div>
    <div class="form-group">
      <label for="password">Password</label>
      <input type="password" id="password" name="password" required autocomplete="current-password">
    </div>
    <button type="submit" class="btn">Sign In</button>
  </form>
</div>
</body>
</html>'''


ADMIN_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HEARTS360 &mdash; Data Admin</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: #f0f2f5; color: #1a1a2e; min-height: 100vh;
    display: flex; align-items: center; justify-content: center;
  }
  .card {
    background: #fff; border-radius: 16px; box-shadow: 0 4px 24px rgba(0,0,0,.08);
    max-width: 520px; width: 100%; padding: 40px 36px; text-align: center;
  }
  .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; }
  .header-left { text-align: left; }
  .logo { font-size: 26px; font-weight: 700; color: #0b5ed7; }
  .subtitle { font-size: 13px; color: #6c757d; }
  .logout-btn {
    padding: 8px 18px; border: 1px solid #dee2e6; border-radius: 8px; background: #fff;
    font-size: 13px; font-weight: 500; color: #555; cursor: pointer; text-decoration: none;
    transition: all .2s;
  }
  .logout-btn:hover { background: #f8f9fa; border-color: #adb5bd; }
  table {
    width: 100%; border-collapse: collapse; margin-bottom: 28px;
    font-size: 14px; text-align: left;
  }
  th { background: #f8f9fa; padding: 10px 14px; font-weight: 600; border-bottom: 2px solid #dee2e6; }
  td { padding: 9px 14px; border-bottom: 1px solid #eee; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; font-weight: 500; }
  .total-row td { font-weight: 700; border-top: 2px solid #dee2e6; background: #f8f9fa; }
  .btn {
    display: inline-block; padding: 14px 36px; border: none; border-radius: 10px;
    font-size: 16px; font-weight: 600; cursor: pointer; transition: all .2s;
  }
  .btn-danger { background: #dc3545; color: #fff; }
  .btn-danger:hover { background: #b02a37; }
  .btn-danger:disabled { background: #e9a0a7; cursor: not-allowed; }
  .btn-secondary { background: #6c757d; color: #fff; margin-left: 10px; }
  .btn-secondary:hover { background: #565e64; }
  #status {
    margin-top: 20px; padding: 12px 16px; border-radius: 8px;
    font-size: 14px; display: none;
  }
  .status-ok { background: #d1e7dd; color: #0f5132; display: block !important; }
  .status-err { background: #f8d7da; color: #842029; display: block !important; }
  .status-loading { background: #fff3cd; color: #664d03; display: block !important; }
  .overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,.45);
    display: none; align-items: center; justify-content: center; z-index: 100;
  }
  .overlay.active { display: flex; }
  .confirm-box {
    background: #fff; border-radius: 14px; padding: 32px 28px;
    max-width: 400px; width: 90%; text-align: center;
    box-shadow: 0 8px 32px rgba(0,0,0,.18);
  }
  .confirm-box h3 { color: #dc3545; margin-bottom: 12px; font-size: 20px; }
  .confirm-box p { font-size: 14px; color: #555; margin-bottom: 24px; line-height: 1.5; }
  .confirm-actions { display: flex; gap: 12px; justify-content: center; }
  .warn-icon { font-size: 40px; margin-bottom: 8px; }
</style>
</head>
<body>

<div class="card">
  <div class="header">
    <div class="header-left">
      <div class="logo">HEARTS360</div>
      <div class="subtitle">Data Administration</div>
    </div>
    <a href="/logout" class="logout-btn">Sign Out</a>
  </div>

  <table id="counts-table">
    <thead><tr><th>Table</th><th style="text-align:right">Records</th></tr></thead>
    <tbody id="counts-body">
      <tr><td colspan="2" style="text-align:center;color:#999">Loading&hellip;</td></tr>
    </tbody>
  </table>

  <button class="btn btn-danger" id="clear-btn" onclick="showConfirm()">
    Clear All Patient Data
  </button>

  <div id="status"></div>
</div>

<div class="overlay" id="overlay">
  <div class="confirm-box">
    <div class="warn-icon">&#9888;&#65039;</div>
    <h3>Are you sure?</h3>
    <p>This will permanently delete <strong>all patient data</strong> from the database.
       This action cannot be undone.</p>
    <div class="confirm-actions">
      <button class="btn btn-danger" onclick="clearData()">Yes, Delete Everything</button>
      <button class="btn btn-secondary" onclick="hideConfirm()">Cancel</button>
    </div>
  </div>
</div>

<script>
const DISPLAY_NAMES = {
  org_units: 'Org Units',
  patients: 'Patients',
  encounters: 'Encounters',
  blood_pressures: 'Blood Pressures',
  blood_sugars: 'Blood Sugars',
  scheduled_visits: 'Scheduled Visits',
  call_results: 'Call Results'
};
const TABLE_ORDER = ['org_units','patients','encounters','blood_pressures','blood_sugars','scheduled_visits','call_results'];

function loadCounts() {
  fetch('/api/table-counts', { credentials: 'same-origin' })
    .then(r => { if (r.status === 401) { window.location = '/login'; } return r.json(); })
    .then(data => {
      if (data.error) { setStatus(data.error, 'err'); return; }
      const tbody = document.getElementById('counts-body');
      let total = 0;
      let rows = '';
      TABLE_ORDER.forEach(t => {
        const c = data[t] || 0;
        total += c;
        rows += '<tr><td>'+(DISPLAY_NAMES[t]||t)+'</td><td class="num">'+c.toLocaleString()+'</td></tr>';
      });
      rows += '<tr class="total-row"><td>Total</td><td class="num">'+total.toLocaleString()+'</td></tr>';
      tbody.innerHTML = rows;
    })
    .catch(e => setStatus('Failed to load counts: ' + e, 'err'));
}

function showConfirm() { document.getElementById('overlay').classList.add('active'); }
function hideConfirm() { document.getElementById('overlay').classList.remove('active'); }

function clearData() {
  hideConfirm();
  const btn = document.getElementById('clear-btn');
  btn.disabled = true;
  btn.textContent = 'Clearing...';
  setStatus('Clearing all data...', 'loading');

  fetch('/api/clear-data', { method: 'POST', credentials: 'same-origin' })
    .then(r => { if (r.status === 401) { window.location = '/login'; } return r.json(); })
    .then(data => {
      if (data.error) {
        setStatus('Error: ' + data.error, 'err');
      } else {
        setStatus(data.message, 'ok');
        loadCounts();
      }
      btn.disabled = false;
      btn.textContent = 'Clear All Patient Data';
    })
    .catch(e => {
      setStatus('Request failed: ' + e, 'err');
      btn.disabled = false;
      btn.textContent = 'Clear All Patient Data';
    });
}

function setStatus(msg, type) {
  const el = document.getElementById('status');
  el.textContent = msg;
  el.className = 'status-' + type;
}

loadCounts();
</script>
</body>
</html>'''


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
