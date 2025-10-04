import os, io, datetime, json
from contextlib import redirect_stdout, redirect_stderr
import logging
from app import main as run_duplicate_combiner
import requests
from flask import Flask, request, redirect, render_template_string, abort, url_for
import threading

# === env / config ===
HEROKU_API_KEY  = os.environ.get("HEROKU_API_KEY", "")
HEROKU_APP_NAME = os.environ.get("HEROKU_APP_NAME", "")
ADMIN_TOKEN     = os.environ.get("ADMIN_TOKEN", "")  # bookmark as ?token=<this>

# if your runner isn't app.main, change this import/alias:


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev")

# === in-memory log & last run summary ===
_ACTIVITY_LOG: list[str] = []
_MAX_LOG_CHARS = 200_000
_last_run_summary = None
_run_in_progress = False

def _append_log(text: str):
    if not text:
        return
    _ACTIVITY_LOG.append(text)
    total = sum(len(x) for x in _ACTIVITY_LOG)
    while total > _MAX_LOG_CHARS and _ACTIVITY_LOG:
        total -= len(_ACTIVITY_LOG.pop(0))

def _log_header():
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    return f"==== Run @ {ts} ====\n"

# === Heroku config helpers ===
if HEROKU_API_KEY and HEROKU_APP_NAME:
    _HK_URL = f"https://api.heroku.com/apps/{HEROKU_APP_NAME}/config-vars"
    _HK_HEADERS = {
        "Accept": "application/vnd.heroku+json; version=3",
        "Authorization": f"Bearer {HEROKU_API_KEY}",
        "Content-Type": "application/json",
    }
else:
    _HK_URL = None
    _HK_HEADERS = {}


def _append_summary_lines(summary):
    # try to print explicit item names if the worker returns them
    lines = []
    if isinstance(summary, dict):
        if "combined_items" in summary and isinstance(summary["combined_items"], list):
            lines.append("Combined Items:")
            for combo in summary["combined_items"]:
                parent = combo.get("parent", "")
                kids = combo.get("children", [])
                kids_str = ", ".join(kids) if isinstance(kids, list) else str(kids)
                lines.append(f"  Parent: {parent}")
                lines.append(f"    Children: {kids_str}")
        elif "combined_pairs" in summary and isinstance(summary["combined_pairs"], list):
            lines.append("Combined Pairs:")
            for pair in summary["combined_pairs"]:
                if isinstance(pair, (list, tuple)) and len(pair) == 2:
                    a, b = pair
                    lines.append(f"  {a}  <= combined with =>  {b}")

        if "moved_items" in summary and isinstance(summary["moved_items"], list):
            lines.append("Moved Originals:")
            for name in summary["moved_items"]:
                lines.append(f"  {name}")
        if "archived_items" in summary and isinstance(summary["archived_items"], list):
            lines.append("Archived Originals:")
            for name in summary["archived_items"]:
                lines.append(f"  {name}")

    if lines:
        _append_log("\n".join(lines) + "\n")


def _heroku_config_get():
    # Prefer live Heroku values so you see changes immediately
    if not _HK_URL:
        return dict(os.environ)
    r = requests.get(_HK_URL, headers=_HK_HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def _heroku_config_patch(updates: dict):
    if not updates:
        return
    if not _HK_URL:
        # no-op if not on Heroku; avoids crashes when running locally
        return
    r = requests.patch(_HK_URL, headers=_HK_HEADERS, json=updates, timeout=20)
    r.raise_for_status()
    return r.json()

# keys we expose/edit
_SETTING_KEYS = [
    "MODE",
    "GROUPING",
    "SIMILARITY_THRESHOLD",
    "MAX_ITEMS_EMBED",
    "MIN_COUNT",
    "MAX_GROUPS",
    "MAX_CHILDREN",
    "AFTER_ACTION",
    "MOVE_GROUP_ID",
    "BOARD_ID",
    "GROUP_IDS",
]

def load_cfg():
    conf = _heroku_config_get()
    # Prefer MODE; fall back to GROUPING (app.py reads GROUPING)
    mode = conf.get("MODE") or conf.get("GROUPING")
    out = {k: conf.get(k) for k in _SETTING_KEYS}
    if mode is not None:
        out["MODE"] = mode
    return out

def save_cfg(form):
    patch = {}
    for k in _SETTING_KEYS:
        if k in form:
            v = (form.get(k) or "").strip()
            if v == "":
                continue
            # Keep MODE/GROUPING in sync; normalize legacy "semantic" to "ai"
            if k == "MODE":
                v_norm = "ai" if v.lower() == "semantic" else v
                patch["MODE"] = v_norm
                patch["GROUPING"] = v_norm
            else:
                patch[k] = v
    _heroku_config_patch(patch)

# === HTML template ===
HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Duplicate Combinator</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    /* reset-ish */
    * { box-sizing: border-box; }
    body { margin: 0; padding: 24px; background:#ffffff; color:#111; font: 14px/1.4 system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
    h1 { margin: 0 0 6px; font-size: 22px; }
    .sub { margin: 0 0 18px; color:#555; }

    /* layout */
    .container { max-width: 980px; margin: 0 auto; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 16px 24px; }
    @media (min-width: 960px) { .grid { grid-template-columns: 1fr 1fr; } }

    .field { background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:12px; }
    .field label { display:block; font-weight:600; margin:0 0 6px; }
    .note { color:#666; font-size:12px; margin-top:6px; }
    input[type=text], input[type=number], select, textarea {
      width:100%; padding:9px 10px; border:1px solid #d1d5db; border-radius:6px; background:#fff; color:#111;
    }

    .actions { display:flex; gap:12px; flex-wrap:wrap; margin: 12px 0 6px; }
    button { padding:10px 14px; border:1px solid #d1d5db; border-radius:8px; background:#111; color:#fff; cursor:pointer; }
    button.secondary { background:#fff; color:#111; }
    .flash { background:#ecfdf5; border:1px solid #a7f3d0; padding:10px; border-radius:8px; margin: 0 0 16px; }
    .danger { background:#fef2f2; border-color:#fecaca; }

    .panel { background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:12px; }
    .two { display:grid; grid-template-columns: 1fr; gap:16px; margin-top:18px; }
    @media (min-width: 960px) { .two { grid-template-columns: 1fr 1fr; } }

    pre { background:#0b1020; color:#d7eaff; padding:12px; border-radius:8px; overflow:auto; margin:0; }
    textarea.log { min-height:220px; white-space:pre; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }

    .toolbar { display:flex; gap:8px; align-items:center; margin:0 0 8px; }
    .toolbar h3 { margin:0; font-size:16px; }
  </style>
</head>
<body>
  {% if run_in_progress %}
    <div class="flash">A run is currently in progress. This page will show logs as they arrive.</div>
  {% endif %}
  <div class="container">
    {% if msg %}<div class="flash">{{ msg }}</div>{% endif %}
    {% if err %}<div class="flash danger">{{ err }}</div>{% endif %}

    <h1>Duplicate Combinator</h1>
    <p class="sub">Configure how duplicates are detected and what to do with originals after combining.</p>

    <!-- SETTINGS -->
    <form method="post" action="/settings?token={{ token }}">
      <div class="grid">

        <div class="field">
          <label for="MODE">Mode</label>
          <select id="MODE" name="MODE">
            {% for opt in ["hybrid","exact","ai"] %}
              <option value="{{ opt }}" {% if cfg.MODE==opt %}selected{% endif %}>{{ opt }}</option>
            {% endfor %}
          </select>
          <div class="note">Choose how to detect duplicates: <code>hybrid</code> (exact + ai), <code>exact</code> (strict text match), or <code>ai</code> (embeddings).</div>
        </div>

        <div class="field">
          <label for="SIMILARITY_THRESHOLD">Similarity Threshold (0–1, default 0.86)</label>
          <input id="SIMILARITY_THRESHOLD" name="SIMILARITY_THRESHOLD" type="number" step="0.01" min="0" max="1"
                 placeholder="0.86" value="{{ cfg.SIMILARITY_THRESHOLD or '' }}">
          <div class="note">Higher = stricter matching.</div>
        </div>

        <div class="field">
          <label for="MAX_ITEMS_EMBED">Max Items to Embed (integer, default 500)</label>
          <input id="MAX_ITEMS_EMBED" name="MAX_ITEMS_EMBED" type="number" min="1" placeholder="500"
                 value="{{ cfg.MAX_ITEMS_EMBED or '' }}">
          <div class="note">Caps how many items are embedded/scored per run.</div>
        </div>

        <div class="field">
          <label for="MIN_COUNT">Minimum Cluster Size (integer, default 2)</label>
          <input id="MIN_COUNT" name="MIN_COUNT" type="number" min="1" placeholder="2" value="{{ cfg.MIN_COUNT or '' }}">
          <div class="note">Only clusters with at least this many items will be combined.</div>
        </div>

        <div class="field">
          <label for="MAX_GROUPS">Max Groups to Scan (integer, default 50)</label>
          <input id="MAX_GROUPS" name="MAX_GROUPS" type="number" min="1" placeholder="50" value="{{ cfg.MAX_GROUPS or '' }}">
          <div class="note">Upper bound on groups examined per run.</div>
        </div>

        <div class="field">
          <label for="MAX_CHILDREN">Max Subitems per Parent (integer, default 30)</label>
          <input id="MAX_CHILDREN" name="MAX_CHILDREN" type="number" min="1" placeholder="30" value="{{ cfg.MAX_CHILDREN or '' }}">
          <div class="note">Prevents huge parents.</div>
        </div>

        <div class="field">
          <label for="AFTER_ACTION">After Action</label>
          <select id="AFTER_ACTION" name="AFTER_ACTION">
            {% for opt in ["none","move","archive"] %}
              <option value="{{ opt }}" {% if cfg.AFTER_ACTION==opt %}selected{% endif %}>{{ opt }}</option>
            {% endfor %}
          </select>
          <div class="note">Leave, move (requires Move Group), or archive originals.</div>
        </div>

        <div class="field">
          <label for="MOVE_GROUP_ID">Move Group (ID, e.g. <code>group_mkwapj5</code>)</label>
          <input id="MOVE_GROUP_ID" name="MOVE_GROUP_ID" type="text" value="{{ cfg.MOVE_GROUP_ID or '' }}">
          <div class="note">Required if After Action is <code>move</code>.</div>
        </div>

        <div class="field">
          <label for="BOARD_ID">Board ID (e.g. <code>3981892064</code>)</label>
          <input id="BOARD_ID" name="BOARD_ID" type="text" value="{{ cfg.BOARD_ID or '' }}">
        </div>

        <div class="field">
          <label for="GROUP_IDS">Group IDs (comma-separated)</label>
          <input id="GROUP_IDS" name="GROUP_IDS" type="text" value="{{ cfg.GROUP_IDS or '' }}" placeholder="new_group,topics">
          <div class="note">Example: <code>new_group,topics</code></div>
        </div>

      </div>

      <div class="actions">
        <button type="submit">Save &amp; Apply</button>
      </div>
    </form>

    <!-- RUN NOW -->
    <form method="post" action="/run-now?token={{ token }}">
      <div class="actions">
        <button type="submit" class="secondary" title="Execute one dedup run immediately">Run Now</button>
      </div>
    </form>

    <p class="note">Saving updates Heroku Config Vars. Heroku will restart the dyno so the worker picks up new values.</p>

    <div class="two">
      <div class="panel">
        <h3 style="margin:0 0 8px;">Last Run Summary</h3>
        {% if last_run %}
          <pre>{{ last_run | tojson(indent=2) }}</pre>
        {% else %}
          <p class="note">No runs yet.</p>
        {% endif %}
      </div>

      <div class="panel">
        <div class="toolbar">
          <h3>Activity Log</h3>
          <button type="button" class="secondary" onclick="copyLog()">Copy Log</button>
          <form method="post" action="/clear-log?token={{ token }}">
            <button type="submit" class="secondary" title="Clear the in-memory log">Clear</button>
          </form>
        </div>
        <textarea id="activityLog" class="log" readonly placeholder="Run logs (print output + combined item names) will appear here...">{{ activity_log or "" }}</textarea>
        <script>
          function copyLog(){
            const el = document.getElementById('activityLog');
            if (navigator.clipboard && window.isSecureContext) {
              navigator.clipboard.writeText(el.value);
            } else {
              el.select(); el.setSelectionRange(0, 999999);
              document.execCommand('copy');
            }
          }
        </script>
        <p class="note">Plain text & copy-pastable.</p>
      </div>
    </div>
  </div>
</body>
</html>
"""

# === routes ===

@app.route("/")
def index():
    # gentle nudge if they forget the token
    if not ADMIN_TOKEN:
        return "ADMIN_TOKEN not set in env. Set it and visit /settings?token=<ADMIN_TOKEN>", 500
    return redirect(f"/settings?token={ADMIN_TOKEN}")

@app.route("/settings", methods=["GET", "POST"])
def settings():
    token = request.args.get("token", "")
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        abort(401)

    msg = None
    err = None

    if request.method == "POST":
        try:
            save_cfg(request.form)
            msg = "Settings saved."
        except Exception as e:
            err = f"Failed to save settings: {e}"

    cfg = load_cfg()
    activity_log = "".join(_ACTIVITY_LOG)
    return render_template_string(
        HTML,
        cfg=cfg, msg=msg, err=err,
        token=token,
        last_run=_last_run_summary,
        activity_log=activity_log,
        run_in_progress=_run_in_progress
    )

def _do_run_now():
    global _last_run_summary, _run_in_progress
    buf = io.StringIO()
    _append_log(_log_header())
    _append_log(f"[run-now] Using settings: {json.dumps(load_cfg())}\n")
    try:
        with redirect_stdout(buf):
            summary = run_duplicate_combiner()  # your app.main()
    except Exception as e:
        _append_log(buf.getvalue())
        _append_log(f"[ERROR] {e}\n==== End Run ====\n\n")
        _last_run_summary = {"error": str(e)}
    else:
        _append_log(buf.getvalue())
        _append_summary_lines(summary)
        _append_log("==== End Run ====\n\n")
        _last_run_summary = summary or {"ok": True}
    finally:
        _run_in_progress = False


@app.route("/run-now", methods=["POST"])
def run_now():
    token = request.args.get("token", "")
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        abort(401)

    global _run_in_progress
    if _run_in_progress:
        _append_log(_log_header())
        _append_log("[INFO] Run requested while another run is in progress; ignoring duplicate trigger.\n")
        _append_log("==== End Run ====\n\n")
        return redirect(url_for("settings", token=token))

    _run_in_progress = True
    threading.Thread(target=_do_run_now, daemon=True).start()
    # return immediately so we don't hit Heroku's 30s router timeout
    return redirect(url_for("settings", token=token))

@app.route("/clear-log", methods=["POST"])
def clear_log():
    token = request.args.get("token", "")
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        abort(401)
    _ACTIVITY_LOG.clear()
    return redirect(url_for("settings", token=token))

# gunicorn entrypoint: web:app
