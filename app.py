import os, json, re, time, sys
from datetime import datetime, timezone
from db import load_settings, get_alias
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any

load_dotenv()

API_URL = "https://api.monday.com/v2"

TEST_BOARD = 3981892064
MARSHAL_RENEE_BOARD = 6696560749

MONDAY_API_KEY   = os.environ["MONDAY_API_KEY"]
BOARD_ID         = os.environ["BOARD_ID"]           # e.g. 3981892064
GROUP_IDS        = os.getenv("GROUP_IDS","")        # optional: comma list

# NEW: grouping strategy
GROUPING         = os.getenv("GROUPING", "hybrid").lower()  # exact|ai|hybrid
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
SIM_THRESHOLD    = float(os.getenv("SIMILARITY_THRESHOLD", "0.86"))
MAX_ITEMS_EMBED  = int(os.getenv("MAX_ITEMS_EMBED", "300"))  # cap items we embed per run

MIN_COUNT        = int(os.getenv("MIN_COUNT","2"))
MAX_GROUPS       = int(os.getenv("MAX_GROUPS","8"))
MAX_CHILDREN     = int(os.getenv("MAX_CHILDREN","25"))
AFTER_ACTION     = os.getenv("AFTER_ACTION","none")          # none|move|archive   <-- fixed
MOVE_GROUP_ID    = os.getenv("MOVE_GROUP_ID","")             # needed if AFTER_ACTION=move

# Load non-secret overrides from DB
_db_overrides = load_settings()
def _get(k, default):
    v = _db_overrides.get(k)
    return v if (v is not None and v != "") else default

# Replace these assignments:
BOARD_ID        = _get("BOARD_ID", BOARD_ID)
GROUP_IDS       = _get("GROUP_IDS", GROUP_IDS)
GROUPING        = _get("GROUPING", GROUPING)
SIM_THRESHOLD   = float(_get("SIMILARITY_THRESHOLD", str(SIM_THRESHOLD)))
MAX_ITEMS_EMBED = int(_get("MAX_ITEMS_EMBED", str(MAX_ITEMS_EMBED)))
MIN_COUNT       = int(_get("MIN_COUNT", str(MIN_COUNT)))
MAX_GROUPS      = int(_get("MAX_GROUPS", str(MAX_GROUPS)))
MAX_CHILDREN    = int(_get("MAX_CHILDREN", str(MAX_CHILDREN)))
AFTER_ACTION    = _get("AFTER_ACTION", AFTER_ACTION)
MOVE_GROUP_ID   = _get("MOVE_GROUP_ID", MOVE_GROUP_ID)


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def gql(query, variables):
    headers = {"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post(API_URL, headers=headers, json={"query": query, "variables": variables}, timeout=30)
    except Exception as e:
        raise Exception(f"HTTP request error: {e}")
    # Monday sometimes returns non-200 for malformed payloads; surface the body
    content = r.text
    try:
        data = r.json()
    except Exception:
        data = None
    if not r.ok:
        raise Exception(f"HTTP {r.status_code} from Monday. Body: {content[:500]}")
    if data is None:
        raise Exception("No JSON in response from Monday.")
    if "errors" in data:
        raise Exception(f"GraphQL errors: {data['errors']}")
    return data.get("data")

# --- Helper: GQL with retry on item-lock/429 errors ---
def gql_with_retry(query, variables, max_retries: int = 5):
    """
    Wrapper around gql() that retries when Monday returns transient locking/429 errors.
    Retries on messages like 'TooManyConcurrentRequestsException' or 'Failed to lock item id for graphql mutation'.
    """
    delay = 0.5
    for attempt in range(max_retries):
        try:
            return gql(query, variables)
        except Exception as e:
            msg = str(e)
            transient = (
                "TooManyConcurrentRequestsException" in msg
                or "Failed to lock item id for graphql mutation" in msg
                or "status_code': 429" in msg
                or "HTTP 429" in msg
            )
            if transient and attempt < max_retries - 1:
                # exponential backoff with light jitter
                time.sleep(delay + 0.1 * attempt)
                delay = min(delay * 2, 5.0)
                continue
            raise


# ---------- Fetch all ----------
def fetch_all(board_id, group_filter):
    q = """
    query($ids:[ID!], $limit:Int, $cursor:String){
      boards(ids:$ids){
        items_page(limit:$limit, cursor:$cursor){
          cursor
          items{ id name created_at group{id title} board{id} }
        }
      }
    }"""
    items, cursor = [], None
    while True:
        d = gql(q, {"ids":[str(board_id)], "limit":100, "cursor":cursor})
        page = (d.get("boards") or [{}])[0].get("items_page") or {}
        batch = page.get("items") or []
        if not batch: break
        if group_filter:
            batch = [it for it in batch if (it.get("group") or {}).get("id") in group_filter]
        items.extend(batch)
        cursor = page.get("cursor")
        if not cursor: break
        time.sleep(0.1)
    return items


_PREFIX_RE = re.compile(r"^\s*((re|fw|fwd|aw|sv|rv|ref|res)\s*[:\]]\s*)+", re.IGNORECASE)
_BRACKET_TAG_RE = re.compile(r"^\s*\[[^\]]+\]\s*")
_TRAILING_PARENS_RE = re.compile(r"\s*[\(\[\{][^\)\]\}]*[\)\]\}]\s*$")

def normalize_subject(s):
    if not s:
        return ""
    s = s.strip()
    # Strip leading tags like [RFP], [Bid], etc.
    while True:
        n = _BRACKET_TAG_RE.sub("", s)
        if n == s:
            break
        s = n.strip()
    # Strip reply/forward prefixes
    while True:
        n = _PREFIX_RE.sub("", s)
        if n == s:
            break
        s = n.strip()
    # Drop *trailing* qualifiers in (), [], {} (often “(Invitation to Bid)”, vendor names, etc.)
    # Do it repeatedly in case there are nested/stacked bits.
    while True:
        n = _TRAILING_PARENS_RE.sub("", s)
        if n == s:
            break
        s = n.strip()
    # Collapse whitespace, trim stray dashes/colons at the end, lowercase
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[-:|]+$", "", s).strip()
    return s.lower()


def group_items(items):
    def ts(s):
        try:
            if s and s.endswith("Z"):
                return int(datetime.strptime(s,"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
            return int(datetime.fromisoformat(s).timestamp())
        except: return 0

    shaped = []
    for it in items:
        shaped.append({
            "id": str(it["id"]),
            "name": it.get("name",""),
            "subject_norm": normalize_subject(it.get("name","")),
            "created_at_ts": ts(it.get("created_at")),
            "group_id": (it.get("group") or {}).get("id"),
        })

    by_subj = {}
    for it in shaped:
        by_subj.setdefault(it["subject_norm"], []).append(it)

    grouped = []
    for subj, arr in by_subj.items():
        arr.sort(key=lambda x: (x["created_at_ts"], x["id"]))
        grouped.append({
            "subject_norm": subj,
            "item_ids_oldest_first": [x["id"] for x in arr],
            "count": len(arr),
        })
    grouped.sort(key=lambda g: (-g["count"], g["subject_norm"]))
    return grouped, {x["id"]: x["name"] for x in shaped}


# ---------- AI semantic grouping (embeddings + greedy clustering) ----------
def _shape_for_ai(items):
    """Return shaped items with created_at_ts + lowercased names (oldest first)."""
    def ts(s):
        try:
            if s and s.endswith("Z"):
                return int(datetime.strptime(s,"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
            return int(datetime.fromisoformat(s).timestamp())
        except:
            return 0
    shaped = []
    for it in items:
        shaped.append({
            "id": str(it["id"]),
            "name": (it.get("name") or "").strip(),
            "created_at_ts": ts(it.get("created_at")),
        })
    shaped.sort(key=lambda x: (x["created_at_ts"], x["id"]))  # oldest first
    return shaped


def _embed_texts(texts):
    """Call OpenAI embeddings in batches of 100."""
    EMBED_URL = "https://api.openai.com/v1/embeddings"
    MODEL = "text-embedding-3-small"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    vecs = []
    B = 100
    for i in range(0, len(texts), B):
        chunk = texts[i:i+B]
        r = requests.post(EMBED_URL, headers=headers, json={"model": MODEL, "input": chunk}, timeout=30)
        r.raise_for_status()
        data = r.json()
        vecs.extend([d["embedding"] for d in data["data"]])
        time.sleep(0.2)  # be gentle on the API
    return vecs


def _cos(a,b):
    num = sum(x*y for x,y in zip(a,b))
    da = (sum(x*x for x in a)) ** 0.5 or 1e-9
    db = (sum(x*x for x in b)) ** 0.5 or 1e-9
    return num/(da*db)


def ai_cluster_items(raw_items, limit_items=MAX_ITEMS_EMBED, sim_threshold=SIM_THRESHOLD):
    """
    Returns list of groups: [{"label": <parent_name>, "item_ids_oldest_first": [...], "count": N}, ...]
    Only returns clusters with count >= 2.
    """
    if not OPENAI_API_KEY:
        return []

    shaped_all = _shape_for_ai(raw_items)
    shaped = shaped_all[-limit_items:]  # newest subset (was oldest)
    if len(shaped) < 2:
        return []

    ids = [it["id"] for it in shaped]
    names = [it["name"] for it in shaped]
    vecs = _embed_texts(names)

    # greedy clustering versus cluster rep
    clusters = []  # list[list[indexes]]
    for i, v in enumerate(vecs):
        placed = False
        for cl in clusters:
            rep_idx = cl[0]
            if _cos(v, vecs[rep_idx]) >= sim_threshold:
                cl.append(i); placed = True; break
        if not placed:
            clusters.append([i])

    # convert to groups (oldest first by created_at_ts -> already sorted in shaped)
    groups = []
    for cl in clusters:
        if len(cl) < 2:
            continue
        cl_ids = [ids[j] for j in cl]
        parent_name = (names[cl[0]] or "")[:80]
        groups.append({
            "label": parent_name,
            "item_ids_oldest_first": cl_ids,
            "count": len(cl_ids),
        })

    groups.sort(key=lambda g: (-g["count"], g["label"]))
    return groups


def get_existing_subitem_names(parent_id):
    q = """query($ids:[ID!]){ items(ids:$ids){ id name subitems{ id name } } }"""
    parent = (gql(q, {"ids":[parent_id]}).get("items") or [{}])[0]
    return { (si.get("name") or "").strip()
             for si in (parent.get("subitems") or []) if (si.get("name") or "").strip() }


def _sanitize_name(name: str, fallback: str) -> str:
    nm = (name or "").strip()
    if not nm:
        nm = fallback
    # Monday item name practical limit is a few hundred chars – keep it conservative
    if len(nm) > 255:
        nm = nm[:255]
    return nm


def create_subitems(parent_id, names):
    """
    Create many subitems under a parent, in safe chunks.
    - Sanitizes names
    - Batches into smaller mutations (<=20 per request)
    - Falls back to single-item creation if a batch fails (to isolate bad inputs)
    """
    # Ensure parent_id is a string for ID!
    parent_id = str(parent_id)

    # sanitize names and drop empties
    clean_names = []
    for i, nm in enumerate(names):
        nm2 = _sanitize_name(nm, f"Item {i+1}")
        if nm2:
            clean_names.append(nm2)
    if not clean_names:
        return


    def build_batch_mutation(p_id: str, batch_names):
        var_defs = ["$parent: ID!"]
        fields, vars = [], {"parent": p_id}
        for i, nm in enumerate(batch_names):
            v = f"n{i}"
            var_defs.append(f"${v}: String!")
            fields.append(f"m{i}: create_subitem(parent_item_id:$parent, item_name:${v}){{ id name }}")
            vars[v] = nm
        q = f"mutation ({', '.join(var_defs)}){{\n  " + "\n  ".join(fields) + "\n}}"
        return q, vars

    BATCH = 20  # keep variable count and payload size reasonable
    for off in range(0, len(clean_names), BATCH):
        chunk = clean_names[off:off+BATCH]
        q, vars = build_batch_mutation(parent_id, chunk)
        try:
            gql_with_retry(q, vars)
            time.sleep(0.1)
        except Exception as e:
            # Fall back to single creates to isolate problematic names
            single_q = "mutation($parent: ID!, $name: String!) { create_subitem(parent_item_id:$parent, item_name:$name){ id name } }"
            for nm in chunk:
                try:
                    gql_with_retry(single_q, {"parent": parent_id, "name": nm})
                    time.sleep(0.05)
                except Exception as e2:
                    # Surface which name failed for fast debugging
                    print(json.dumps({"create_subitem_failed": {"parent": parent_id, "name": nm, "error": str(e2)}}))


def move_items(ids, group_id):
    if not ids:
        return
    BATCH = 5  # small batches reduce item locking
    for off in range(0, len(ids), BATCH):
        chunk = ids[off:off+BATCH]
        var_defs, fields, vars = [], [], {}
        for i, iid in enumerate(chunk):
            var_defs.append(f"$id{i}: ID!")
            var_defs.append(f"$g{i}: String!")
            fields.append(f"m{i}: move_item_to_group(item_id:$id{i}, group_id:$g{i}) {{ id }}")
            vars[f"id{i}"] = str(iid)
            vars[f"g{i}"] = group_id
        query = "mutation(" + ", ".join(var_defs) + ") {\n  " + "\n  ".join(fields) + "\n}"
        gql_with_retry(query, vars)
        time.sleep(0.1)


def archive_items(ids):
    if not ids:
        return
    BATCH = 10  # archiving is cheaper; still keep batches modest
    for off in range(0, len(ids), BATCH):
        chunk = ids[off:off+BATCH]
        var_defs, fields, vars = [], [], {}
        for i, iid in enumerate(chunk):
            var_defs.append(f"$id{i}: ID!")
            fields.append(f"a{i}: archive_item(item_id:$id{i}) {{ id }}")
            vars[f"id{i}"] = str(iid)
        query = "mutation(" + ", ".join(var_defs) + ") {\n  " + "\n  ".join(fields) + "\n}"
        gql_with_retry(query, vars)
        time.sleep(0.1)


def main():
    # Structured summary object that web.py can display & parse
    run_summary: Dict[str, Any] = {
        "board_id": str(BOARD_ID),
        "group_ids": GROUP_IDS if isinstance(GROUP_IDS, str) else ",".join(GROUP_IDS),
        "mode": os.environ.get("GROUPING", "hybrid"),
        "threshold": float(SIM_THRESHOLD),
        "processed_groups": 0,
        "created_subitems": 0,
        "moved_originals": 0,
        "archived_originals": 0,
        # Detailed fields that the settings page will surface:
        "combined_items": [],   # list of {"parent": str, "children": [str,...]}
        "combined_pairs": [],   # kept for compatibility; not used here
        "moved_items": [],      # list of names
        "archived_items": [],   # list of names
    }

    # Build group filter from comma-separated GROUP_IDS
    group_filter = {g.strip() for g in str(GROUP_IDS or "").split(",") if g.strip()}

    # Fetch items and compute exact-grouping view + id->name map (used everywhere)
    items = fetch_all(BOARD_ID, group_filter)
    grouped_exact, id_to_name = group_items(items)

    # Decide which groups to process
    groups = []
    if GROUPING == "exact":
        candidates = [g for g in grouped_exact if g["count"] >= MIN_COUNT]
        groups = candidates[:MAX_GROUPS]

    elif GROUPING == "ai":
        ai_groups = ai_cluster_items(items, limit_items=MAX_ITEMS_EMBED, sim_threshold=SIM_THRESHOLD)
        groups = [{"subject_norm": g.get("label", "ai_cluster"),
                   "item_ids_oldest_first": g["item_ids_oldest_first"],
                   "count": g["count"]}
                  for g in ai_groups][:MAX_GROUPS]

    else:  # hybrid
        exact_first = [g for g in grouped_exact if g["count"] >= MIN_COUNT]
        used_ids = set()
        for g in exact_first:
            used_ids.update(g["item_ids_oldest_first"])
        leftovers = [it for it in items if str(it["id"]) not in used_ids]

        ai_groups = ai_cluster_items(leftovers, limit_items=MAX_ITEMS_EMBED, sim_threshold=SIM_THRESHOLD)
        hybrid = exact_first + [
            {"subject_norm": g.get("label", "ai_cluster"),
             "item_ids_oldest_first": g["item_ids_oldest_first"],
             "count": g["count"]}
            for g in ai_groups
        ]
        groups = hybrid[:MAX_GROUPS]

    total_created, moved, archived = 0, 0, 0

    # Process each candidate group
    for g in groups:
        ids = g["item_ids_oldest_first"]
        if len(ids) < 2:
            continue

        parent, children = ids[0], ids[1:][:MAX_CHILDREN]
        parent_name = id_to_name.get(parent, str(parent))

        # What subitem names already exist on the parent?
        existing = get_existing_subitem_names(parent)

        # Determine which children still need a subitem created (by name)
        pending = []
        for cid in children:
            nm = (id_to_name.get(cid, f"Item {cid}") or "").strip()
            if nm and nm not in existing:
                pending.append((cid, nm))

        # Create any missing subitems (batched with fallbacks)
        if pending:
            names_to_create = [nm for _cid, nm in pending]
            create_subitems(parent, names_to_create)
            total_created += len(names_to_create)
            # reflect new names in the "existing" set so representation calc below is accurate
            existing |= set(names_to_create)

        # Children that are now represented as subitems (newly created + already present)
        represented_ids = [cid for cid, _nm in pending] + \
                          [cid for cid in children if (id_to_name.get(cid, "").strip() in existing)]

        # Log exactly what we combined (parent + children names)
        if represented_ids:
            run_summary["combined_items"].append({
                "parent": parent_name,
                "children": [id_to_name.get(cid, f"Item {cid}") for cid in represented_ids]
            })

        # After-action on originals
        if represented_ids:
            if AFTER_ACTION == "move" and MOVE_GROUP_ID:
                move_items(represented_ids, MOVE_GROUP_ID)
                moved += len(represented_ids)
                run_summary["moved_items"].extend([id_to_name.get(cid, f"Item {cid}") for cid in represented_ids])
            elif AFTER_ACTION == "archive":
                archive_items(represented_ids)
                archived += len(represented_ids)
                run_summary["archived_items"].extend([id_to_name.get(cid, f"Item {cid}") for cid in represented_ids])

        time.sleep(0.05)  # play nice with API

    run_summary["processed_groups"] = len(groups)
    run_summary["created_subitems"] = total_created
    run_summary["moved_originals"] = moved
    run_summary["archived_originals"] = archived

    # Optional: human-friendly display names from DB aliases
    board_display, _ = get_alias("board", str(BOARD_ID))
    move_group_display, _ = (get_alias("group", MOVE_GROUP_ID) if MOVE_GROUP_ID else (None, {}))

    # Print a terse machine-readable summary for worker logs
    print(json.dumps({
        "board_id": str(BOARD_ID),
        "board_display": board_display,
        "move_group_id": MOVE_GROUP_ID,
        "move_group_display": move_group_display,
        "group_ids": GROUP_IDS,
        "mode": GROUPING,
        "processed_groups": run_summary["processed_groups"],
        "created_subitems": run_summary["created_subitems"],
        "moved_originals": run_summary["moved_originals"],
        "archived_originals": run_summary["archived_originals"],
        "threshold": SIM_THRESHOLD,
        "timestamp_utc": utc_now_z()
    }))
    sys.stdout.flush()

    # Critical: return the full summary so /run-now in web.py can show child names
    return run_summary

if __name__ == "__main__":
    main()
