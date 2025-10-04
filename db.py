# ONE TIME INITIALIZATION:
#  python -c "import db; db.init_schema(); print('OK')"
# # or: heroku run python -c "import db; db.init_schema(); print('OK')"


# db.py
import os, json
import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS settings (
      key         text PRIMARY KEY,
      value       text NOT NULL,
      updated_at  timestamptz NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS aliases (
      kind        text NOT NULL CHECK (kind IN ('board','group')),
      raw_id      text NOT NULL,
      display     text NOT NULL,
      extra_json  jsonb DEFAULT '{}'::jsonb,
      updated_at  timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (kind, raw_id)
    );
    """
    with get_conn() as c:
        with c.cursor() as cur:
            cur.execute(ddl)

def load_settings(keys=None):
    with get_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if keys:
                cur.execute("SELECT key, value FROM settings WHERE key = ANY(%s)", (keys,))
            else:
                cur.execute("SELECT key, value FROM settings")
            return {row["key"]: row["value"] for row in cur.fetchall()}

def upsert_settings(kv: dict):
    if not kv: return
    with get_conn() as c:
        with c.cursor() as cur:
            for k, v in kv.items():
                cur.execute("""
                  INSERT INTO settings(key, value) VALUES (%s, %s)
                  ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now()
                """, (k, str(v)))

def get_alias(kind: str, raw_id: str):
    with get_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT display, extra_json FROM aliases WHERE kind=%s AND raw_id=%s", (kind, raw_id))
            row = cur.fetchone()
            return (row["display"], row["extra_json"]) if row else (None, {})

def list_aliases(kind: str):
    with get_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT raw_id, display FROM aliases WHERE kind=%s ORDER BY display", (kind,))
            return [{"raw_id": r["raw_id"], "display": r["display"]} for r in cur.fetchall()]

def upsert_alias(kind: str, raw_id: str, display: str, extra_json=None):
    with get_conn() as c:
        with c.cursor() as cur:
            cur.execute("""
              INSERT INTO aliases(kind, raw_id, display, extra_json)
              VALUES (%s, %s, %s, %s::jsonb)
              ON CONFLICT (kind, raw_id) DO UPDATE
                SET display=EXCLUDED.display,
                    extra_json=EXCLUDED.extra_json,
                    updated_at=now()
            """, (kind, raw_id, display, json.dumps(extra_json or {})))
