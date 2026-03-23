import streamlit as st
import pandas as pd
from pyhive import hive
import hashlib
import uuid
import datetime
import re
import time
import boto3
import json
import shutil
import os
import subprocess

# ------------------------------------------------------------------------------
# 1. PAGE CONFIGURATION
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="DBT Rule Manager",
    page_icon="⚙️",
    layout="wide"
)

# ------------------------------------------------------------------------------
# 2. CONFIGURATION & DATABASE CONNECTION
# ------------------------------------------------------------------------------
THRIFT_HOST = "spark-thrift"
THRIFT_PORT = 10000
AUTH_CATALOG = "formula1_catalog"
AUTH_SCHEMA = "default"
AUTH_TABLE = "users"

# --- MINIO CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"  # <-- Update this
MINIO_ACCESS_KEY = "admin"                # <-- Update this
MINIO_SECRET_KEY = "password123"                # <-- Update this
MINIO_BUCKET = "formula1"
FINAL_RULES_SCHEMA = "final_rules"
SNAPSHOT_RETAIN_LAST = 8

@st.cache_resource
def get_s3_client():
    return boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

@st.cache_data(ttl=300) # Caches results for 5 minutes
def get_iceberg_schemas():
    s3_client = get_s3_client()
    
    schemas = {}
    prefix = "iceberg_data/staging/"
    
    # 1. Get all table folders
    result = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix, Delimiter='/')
    table_prefixes = [p.get('Prefix') for p in result.get('CommonPrefixes', [])]
    
    for table_prefix in table_prefixes:
        table_name = table_prefix.split('/')[-2] # e.g., extracts 'drivers'
        
        # 2. Find metadata JSON files
        meta_prefix = f"{table_prefix}metadata/"
        meta_objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=meta_prefix)
        
        json_files = [obj for obj in meta_objects.get('Contents', []) if obj['Key'].endswith('.json')]
        if not json_files:
            continue
            
        # 3. Get the latest metadata JSON file
        latest_json = sorted(json_files, key=lambda x: x['LastModified'])[-1]
        
        # 4. Fetch and parse JSON
        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=latest_json['Key'])
        metadata = json.loads(response['Body'].read().decode('utf-8'))
        
        # 5. Extract column names from Iceberg schema
        columns = []
        if 'schemas' in metadata:
            current_schema_id = metadata.get('current-schema-id', 0)
            current_schema = next((s for s in metadata['schemas'] if s['schema-id'] == current_schema_id), metadata['schemas'][0])
            columns = [field['name'] for field in current_schema.get('fields', [])]
        elif 'schema' in metadata:
            columns = [field['name'] for field in metadata['schema'].get('fields', [])]
            
        if columns:
            schemas[table_name] = columns
            
    return schemas

def get_connection():
    return hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT, username="admin")

def run_query(query):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Database Error: {e}")
        return None
    finally:
        conn.close()

def _extract_write_table_refs(query):
    """Extract target table names from write SQL statements for snapshot housekeeping."""
    if not isinstance(query, str):
        return []

    matches = re.findall(r"(?i)\b(?:insert\s+into|update|delete\s+from|merge\s+into)\s+([a-zA-Z0-9_`.]+)", query)
    refs = []
    for raw in matches:
        token = raw.strip().strip('`')
        parts = [p.strip('`') for p in token.split('.') if p]
        if len(parts) == 3:
            catalog, schema, table = parts
            if catalog.lower() == AUTH_CATALOG.lower():
                refs.append(f"{schema}.{table}")
        elif len(parts) == 2:
            schema, table = parts
            refs.append(f"{schema}.{table}")
        elif len(parts) == 1:
            refs.append(f"{AUTH_SCHEMA}.{parts[0]}")
    return list(dict.fromkeys(refs))

def _expire_snapshots_for_refs(cursor, table_refs):
    """Retain only the latest N snapshots for modified Iceberg tables."""
    for table_ref in table_refs:
        try:
            cursor.execute(
                f"CALL {AUTH_CATALOG}.system.expire_snapshots(table => '{table_ref}', retain_last => {SNAPSHOT_RETAIN_LAST})"
            )
        except Exception:
            # Keep business write successful even if snapshot cleanup is unsupported/intermittent.
            pass

def run_update(query):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        _expire_snapshots_for_refs(cursor, _extract_write_table_refs(query))
        return True
    except Exception as e:
        st.error(f"Database Write Error: {e}")
        return False
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# 3. SECURITY & VALIDATION UTILS
# ------------------------------------------------------------------------------
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def is_valid_password(p):
    return (
        len(p) >= 8 and
        re.search(r"[A-Z]", p) and 
        re.search(r"[a-z]", p) and 
        re.search(r"\d", p) and 
        re.search(r"[!@#$%^&*(),.?\":{}|<>]", p)
    )

def is_valid_phone(num):
    return num.isdigit() and len(num) == 10

def check_user_exists(identifier):
    sql = f"SELECT 1 FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} WHERE username = '{identifier}' OR email = '{identifier}'"
    result = run_query(sql)
    return len(result) > 0 if result else False

# ------------------------------------------------------------------------------
# 4. AUTHENTICATION LOGIC
# ------------------------------------------------------------------------------
def register_user(fname, lname, uname, email, phone, password):
    # HARDCODED ROLE FOR SECURITY
    DEFAULT_ROLE = "CREATOR"
    
    if check_user_exists(uname):
        return False, "⚠️ Username already exists."
    if check_user_exists(email):
        return False, "⚠️ Email already exists."
    
    user_id = str(uuid.uuid4())
    hashed_pw = hash_password(password)
    
    # We generate the time string here
    created_at_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # FIX: We wrap the string in CAST('{...}' AS TIMESTAMP)
    sql = f"""
    INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
    (user_id, firstname, lastname, username, email, mobile, password_hash, role, created_at, last_login)
    VALUES (
        '{user_id}', 
        '{fname}', 
        '{lname}', 
        '{uname}', 
        '{email}', 
        '{phone}', 
        '{hashed_pw}', 
        '{DEFAULT_ROLE}', 
        CAST('{created_at_str}' AS TIMESTAMP), 
        NULL
    )
    """
    
    if run_update(sql):
        return True, "✅ Registration successful! Please log in."
    else:
        return False, "❌ Registration failed due to database error."

def verify_user(identifier, password):
    hashed_pw = hash_password(password)
    
    # 1. Verify Credentials
    # 1. Verify Credentials (ADDED user_id)
    sql = f"""
    SELECT user_id, firstname, lastname, username, role 
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} 
    WHERE (username = '{identifier}' OR email = '{identifier}') 
    AND password_hash = '{hashed_pw}'
    """
    result = run_query(sql)
    
    if result and len(result) > 0:
        row = result[0]
        # Map user_id to the dictionary
        user_data = {"user_id": row[0], "firstname": row[1], "lastname": row[2], "username": row[3], "role": row[4]}
        
        # 2. UPDATE Last Login Timestamp (The Fix)
        # We capture the time now and cast it safely for Iceberg
        login_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        update_sql = f"""
        UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
        SET last_login = CAST('{login_time}' AS TIMESTAMP)
        WHERE username = '{user_data['username']}'
        """
        
        # We run the update but don't block the login if it fails slightly (optional safety)
        run_update(update_sql)
        
        return user_data
        
    return None

# --- PASSWORD RESET LOGIC ---
def verify_user_for_reset(email, mobile):
    """Check if a user exists with this specific Email AND Mobile number."""
    sql = f"""
    SELECT username FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} 
    WHERE email = '{email}' AND mobile = '{mobile}'
    """
    result = run_query(sql)
    return True if result and len(result) > 0 else False

def reset_password(email, new_password):
    """Updates the password for the verified user."""
    new_hash = hash_password(new_password)
    sql = f"""
    UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
    SET password_hash = '{new_hash}'
    WHERE email = '{email}'
    """
    return run_update(sql)

def safe_index(options, value, default=0):
    """Get the index of a value in a list, returning default if not found."""
    try:
        if value is not None and value in options:
            return options.index(value)
    except (ValueError, AttributeError):
        pass
    return default

def is_numeric_literal(value):
    """Return True when value is a valid numeric literal (int/float, incl. negatives)."""
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if text == "":
        return False
    try:
        float(text)
        return True
    except ValueError:
        return False

def normalize_ref_name(value):
    """Normalize '{{ ref(...) }}' strings to raw model names for safe macro input."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    match = re.match(r"^\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}$", text)
    return match.group(1) if match else value

def safe_model_name(value):
    """Convert arbitrary rule/model names to dbt-safe identifiers."""
    if value is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(value).lower())

def force_ephemeral_materialization(sql_text):
    """Ensure a model SQL contains config(materialized='ephemeral')."""
    if not isinstance(sql_text, str):
        return sql_text

    # 1) Replace explicit materialization value if present.
    updated, replaced = re.subn(
        r"materialized\s*=\s*['\"][^'\"]+['\"]",
        "materialized='ephemeral'",
        sql_text,
        count=1,
        flags=re.IGNORECASE
    )
    if replaced > 0:
        return updated

    # 2) If config exists but no materialized key, inject it.
    updated, injected = re.subn(
        r"\{\{\s*config\s*\(",
        "{{ config(materialized='ephemeral', ",
        sql_text,
        count=1,
        flags=re.IGNORECASE
    )
    if injected > 0:
        return updated

    # 3) If no config block, prepend one.
    return "{{ config(materialized='ephemeral') }}\n\n" + sql_text

def ensure_non_sequential_rules_ephemeral(group_id, group_name):
    """Force all rule models in a non-sequential group to ephemeral materialization."""
    rules = get_rules_by_group(group_id)
    if not rules:
        return False, "No rules found in group."

    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    missing_files = []

    for _, rule_name, _ in rules:
        safe_name = safe_model_name(rule_name)
        rule_path = os.path.join('/', 'dbt', 'formula1', 'models', 'rules', safe_group, f"{safe_name}.sql")

        if not os.path.exists(rule_path):
            missing_files.append(rule_name)
            continue

        try:
            with open(rule_path, 'r') as f:
                original = f.read()
            updated = force_ephemeral_materialization(original)
            if updated != original:
                with open(rule_path, 'w') as f:
                    f.write(updated)
        except Exception as e:
            return False, f"Failed to enforce ephemeral for '{rule_name}': {e}"

    if missing_files:
        return False, f"Missing rule SQL file(s): {', '.join(missing_files)}"

    return True, "All non-sequential rules are set to ephemeral."

def build_live_rule_preview_sql(
    macro_tables,
    macro_joins,
    macro_select_cols,
    macro_aggs,
    macro_where,
    macro_group_by,
    macro_having,
    macro_order,
    macro_limit,
):
    """Render clean SQL preview directly from current UI state without macro expansion."""
    lines = []

    if macro_tables:
        cte_chunks = [f"{tbl} AS (\n  SELECT * FROM {tbl}\n)" for tbl in macro_tables]
        lines.append("WITH")
        lines.append(",\n".join(cte_chunks))

    select_items = list(macro_select_cols)
    for agg in macro_aggs:
        if agg.get('func') and agg.get('col'):
            alias = agg.get('alias', '').strip()
            expr = f"{agg['func']}({agg['col']})"
            select_items.append(f"{expr} AS {alias}" if alias else expr)

    lines.append("SELECT")
    if select_items:
        lines.append("  " + ",\n  ".join(select_items))
    else:
        lines.append("  *")

    if macro_tables:
        lines.append(f"FROM {macro_tables[0]}")

    for j in macro_joins:
        if j.get('type') and j.get('table') and j.get('left') and j.get('right'):
            lines.append(f"{j['type']} {j['table']} ON {j['left']} = {j['right']}")

    where_clauses = []
    for idx, w in enumerate(macro_where):
        col = w.get('col', '')
        op = w.get('op', '')
        val = w.get('value', '')
        val2 = w.get('value2', '')
        if not col or not op:
            continue

        if op in ['IS NULL', 'IS NOT NULL']:
            expr = f"{col} {op}"
        elif op == 'BETWEEN':
            expr = f"{col} BETWEEN {val} AND {val2}"
        else:
            expr = f"{col} {op} {val}"

        if idx == 0 or not where_clauses:
            where_clauses.append(expr)
        else:
            where_clauses.append(f"{w.get('logic', 'AND')} {expr}")

    if where_clauses:
        lines.append("WHERE")
        lines.append("  " + "\n  ".join(where_clauses))

    if macro_group_by:
        lines.append("GROUP BY")
        lines.append("  " + ",\n  ".join(macro_group_by))

    having_clauses = []
    for idx, h in enumerate(macro_having):
        if not (h.get('func') and h.get('col') and h.get('op')):
            continue
        expr = f"{h['func']}({h['col']}) {h['op']} {h.get('value', '')}"
        if idx == 0 or not having_clauses:
            having_clauses.append(expr)
        else:
            having_clauses.append(f"{h.get('logic', 'AND')} {expr}")

    if having_clauses:
        lines.append("HAVING")
        lines.append("  " + "\n  ".join(having_clauses))

    if macro_order:
        order_items = [f"{o['col']} {o['dir']}" for o in macro_order if o.get('col') and o.get('dir')]
        if order_items:
            lines.append("ORDER BY")
            lines.append("  " + ",\n  ".join(order_items))

    if macro_limit and str(macro_limit) != 'None':
        lines.append(f"LIMIT {macro_limit}")

    return "\n".join(lines)

def resolve_group_relation_name(value, relation_map):
    """Resolve a table token to either an in-group CTE name or external model name."""
    raw_name = normalize_ref_name(value)
    if not isinstance(raw_name, str):
        return raw_name, False
    cleaned = raw_name.strip()
    if not cleaned:
        return cleaned, False
    if cleaned in relation_map:
        return relation_map[cleaned], True
    return cleaned, False

def rewrite_column_reference(value, relation_map):
    """Rewrite <table>.<column> references so in-group dependencies point to CTE names."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    if "." not in text:
        return text
    table_part, column_part = text.split(".", 1)
    resolved_table, _ = resolve_group_relation_name(table_part, relation_map)
    return f"{resolved_table}.{column_part}" if resolved_table else text

def _clear_dynamic_widget_keys():
    """Remove all dynamic widget keys so freshly loaded state values take effect."""
    prefixes = ('t_right_', 'j_type_', 'l_tbl_', 'l_col_', 'r_col_',
                'rm_j_', 'out_col_', 'primary_cols_', 'logic_', 'w_col_', 'w_op_',
                'w_val_', 'w_val1_', 'w_val2_', 'w_del_', 'o_col_', 'o_dir_', 'o_del_',
                'agg_col_', 'agg_func_', 'agg_alias_', 'agg_del_',
                'h_logic_', 'h_func_', 'h_col_', 'h_op_', 'h_val_', 'h_del_',
                'manual_group_by')
    keys_to_remove = [k for k in st.session_state if any(k.startswith(p) for p in prefixes)]
    for k in keys_to_remove:
        del st.session_state[k]

def clear_session_for_role(role):
    """Clear role-specific session keys not relevant to the current role."""
    creator_keys = [
        'joins', 'where_filters', 'order_filters', 'aggregations', 'having_filters',
        'manual_group_by', 'row_limit', 't1', 'active_group_id', 'active_group_name',
        'active_group_mode', 'create_new_group', 'last_selected_rule',
        'loaded_primary_table', 'loaded_primary_columns', 'loaded_cols_joins',
        'schema_cache_key', 'visual_sequence_input', 'sequence_order'
    ]
    admin_keys = ['user_editor']
    approver_keys = ['approval_comments']
    approver_roles = {'MANAGER', 'TECHNICAL', 'SME'}

    if role != 'CREATOR':
        for key in creator_keys:
            if key in st.session_state:
                del st.session_state[key]
        _clear_dynamic_widget_keys()

    if role != 'ADMIN':
        for key in admin_keys:
            if key in st.session_state:
                del st.session_state[key]

    if role not in approver_roles:
        for key in approver_keys:
            if key in st.session_state:
                del st.session_state[key]

# --- DBT & WORKFLOW HELPER FUNCTIONS ---
def get_next_rule_id():
    """Fetches the highest rule_id from the database and adds 1."""
    sql = f"SELECT MAX(rule_id) FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow"
    result = run_query(sql)
    
    # If the table has records and the max ID is not None, add 1
    if result and result[0][0] is not None:
        return result[0][0] + 1
    # If the table is completely empty, start at 1
    return 1

def get_existing_rules(group_id=None):
    """Fetch rules (optionally by group) and return display model names by status.

    ACTIVE rules are exposed as final_<rule_name>; all others use draft model names.
    """
    base_sql = f"SELECT rule_id, rule_name, status FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow"
    if group_id is not None:
        sql = f"{base_sql} WHERE group_id = {int(group_id)} ORDER BY rule_id ASC"
    else:
        sql = f"{base_sql} ORDER BY rule_id ASC"

    result = run_query(sql)
    if not result:
        return []

    mapped_rules = []
    for rule_id, rule_name, status in result:
        safe_name = safe_model_name(rule_name)
        model_name = f"final_{safe_name}" if status == "ACTIVE" else safe_name
        mapped_rules.append((rule_id, rule_name, status, model_name))

    return mapped_rules

def save_dbt_model(rule_name, group_name, sql_content):
    """Saves the SQL query directly to the shared DBT container volume inside a group folder."""
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    
    dbt_dir = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group)
    os.makedirs(dbt_dir, exist_ok=True)
    
    filepath = os.path.join(dbt_dir, f"{safe_name}.sql")
    with open(filepath, "w") as f:
        f.write(sql_content)
    return filepath

def save_ui_state(rule_name, group_name, state_dict):
    """Saves the UI state as a JSON file in MinIO inside a group folder."""
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    s3_key = f"ui_states/{safe_group}/{safe_name}.json"
    
    s3_client = get_s3_client()
    try:
        json_str = json.dumps(state_dict, indent=4)
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=s3_key, Body=json_str)
        return True
    except Exception as e:
        st.error(f"Failed to save UI state to MinIO: {e}")
        return False

def load_ui_state(rule_name, group_name):
    """Loads the UI state from the JSON file in MinIO from the group folder."""
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    s3_key = f"ui_states/{safe_group}/{safe_name}.json"
    
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=s3_key)
        state = json.loads(response['Body'].read().decode('utf-8'))
        return state
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        st.error(f"Failed to load UI state from MinIO: {e}")
        return None

def load_rule_state(rule_name, group_name):
    """Reads the JSON from MinIO and pre-populates ALL widget keys in session state."""
    state = load_ui_state(rule_name, group_name)
    
    if state:
        
        # Step 1: Clear ALL old dynamic widget keys
        _clear_dynamic_widget_keys()
        
        # Step 2: Load structural data state (Handles older app.py keys too)
        joins = state.get("joins", [])
        where_filters = state.get("where_filters", [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "logic": "AND"}])
        order_filters = state.get("order_filters", [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}])
        having_filters = state.get("having_filters", [])
        
        # Legacy mapping for older rules
        manual_group_by = state.get("manual_group_by", state.get("group_cols", []))
        row_limit = state.get("row_limit", "")
        
        # Ensure each item has an 'id'
        for item in joins + where_filters + order_filters + having_filters:
            if not item.get('id'):
                item['id'] = str(uuid.uuid4())
        
        st.session_state.joins = joins
        st.session_state.where_filters = where_filters
        st.session_state.order_filters = order_filters
        
        if row_limit in (None, "", 0, "0"):
            st.session_state.row_limit = None
        else:
            try:
                parsed_limit = int(row_limit)
                st.session_state.row_limit = parsed_limit if parsed_limit > 0 else None
            except (ValueError, TypeError):
                st.session_state.row_limit = None
        st.session_state.having_filters = having_filters
        
        # Step 3: Set primary table widget key
        primary_table = state.get("primary_table")
        if primary_table and primary_table in TABLE_NAMES:
            st.session_state.t1 = primary_table
        
        # Step 4: Set primary columns multiselect key (Handles old "cols_t1" key)
        primary_cols = state.get("primary_columns", state.get("cols_t1", []))
        valid_primary_cols = [c for c in primary_cols if c in TABLE_SCHEMAS.get(primary_table or "", [])]
        st.session_state[f"primary_cols_{primary_table}"] = valid_primary_cols
        
        # Step 5: Set join widget keys directly
        join_types_list = ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN"]
        for join_data in joins:
            uid = join_data.get("id")
            if not uid: continue
            rt = join_data.get("right_table", "")
            if rt and rt in TABLE_NAMES: st.session_state[f"t_right_{uid}"] = rt
            jt = join_data.get("join_type", "")
            if jt in join_types_list: st.session_state[f"j_type_{uid}"] = jt
            lt = join_data.get("left_table", "")
            if lt and lt in TABLE_NAMES: st.session_state[f"l_tbl_{uid}"] = lt
            lc = join_data.get("left_col", "")
            lt_for_cols = lt if lt else (primary_table or "")
            if lc and lc in TABLE_SCHEMAS.get(lt_for_cols, []): st.session_state[f"l_col_{uid}"] = lc
            rc = join_data.get("right_col", "")
            if rc and rc in TABLE_SCHEMAS.get(rt, []): st.session_state[f"r_col_{uid}"] = rc
        
        # Step 6: Set join output column multiselect keys
        cols_joins_data = state.get("cols_joins", {})
        for table, cols in cols_joins_data.items():
            valid_cols = [c for c in cols if c in TABLE_SCHEMAS.get(table, [])]
            st.session_state[f"out_col_{table}"] = valid_cols
        
        # Step 7: Set WHERE filter widget keys
        op_options_list = ["equals (=)", "greater than (>)", "less than (<)", "not equals (!=)", "greater than equal (>=)", "less than equal (<=)", "not equal to (!=)", "LIKE", "NOT LIKE", "IN", "NOT IN", "IS", "IS NOT", "IS NULL", "IS NOT NULL", "BETWEEN"]
        logic_opts = ["AND", "OR"]
        
        loaded_available_columns = [f"{primary_table}.{c}" for c in TABLE_SCHEMAS.get(primary_table or "", [])]
        for jd in joins:
            rt = jd.get("right_table")
            if rt: loaded_available_columns.extend([f"{rt}.{c}" for c in TABLE_SCHEMAS.get(rt, [])])
        loaded_available_columns.insert(0, "")
        
        st.session_state.manual_group_by = [c for c in manual_group_by if c in loaded_available_columns]
        
        for fd in where_filters:
            uid = fd.get("id")
            if not uid: continue
            if fd.get("op") in op_options_list: st.session_state[f"w_op_{uid}"] = fd["op"]
            if fd.get("logic") in logic_opts: st.session_state[f"logic_{uid}"] = fd["logic"]
            col_val = fd.get("col", "")
            if col_val in loaded_available_columns: st.session_state[f"w_col_{uid}"] = col_val
            if fd.get('val'):
                st.session_state[f"w_val_{uid}"] = fd["val"]
                st.session_state[f"w_val1_{uid}"] = fd["val"]
            if fd.get('val2'): st.session_state[f"w_val2_{uid}"] = fd["val2"]
        
        # Step 8: Set ORDER BY widget keys
        dir_opts = ["ASC", "DESC"]
        for sd in order_filters:
            uid = sd.get("id")
            if not uid: continue
            if sd.get("dir") in dir_opts: st.session_state[f"o_dir_{uid}"] = sd["dir"]
            col_val = sd.get("col", "")
            if col_val in loaded_available_columns: st.session_state[f"o_col_{uid}"] = col_val

        # Step 8.5: Set HAVING filter widget keys
        having_logic_opts = ["AND", "OR"]
        having_func_opts = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
        having_op_opts = ["=", ">", "<", ">=", "<=", "!="]
        for hd in having_filters:
            uid = hd.get("id")
            if not uid: continue
            if hd.get("logic") in having_logic_opts: st.session_state[f"h_logic_{uid}"] = hd["logic"]
            
            # Handle old "agg" key -> new "func" key mapping
            func_val = hd.get("func", hd.get("agg", ""))
            if func_val in having_func_opts: 
                st.session_state[f"h_func_{uid}"] = func_val
                hd["func"] = func_val
            
            col_val = hd.get("col", "")
            if col_val in loaded_available_columns: st.session_state[f"h_col_{uid}"] = col_val
            if hd.get("op") in having_op_opts: st.session_state[f"h_op_{uid}"] = hd["op"]
            if hd.get("val"): st.session_state[f"h_val_{uid}"] = hd["val"]
        
        # Step 9: Keep loaded state for reference
        st.session_state.loaded_primary_table = primary_table
        st.session_state.loaded_primary_columns = primary_cols
        st.session_state.loaded_cols_joins = cols_joins_data
        
        # Step 10: Load aggregations
        aggregations = state.get("aggregations", [])
        for agg in aggregations:
            if not agg.get('id'): agg['id'] = str(uuid.uuid4())
        st.session_state.aggregations = aggregations
        
        agg_func_options = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
        for agg_data in aggregations:
            uid = agg_data.get("id")
            if not uid: continue
            col_val = agg_data.get("col", "")
            if col_val in loaded_available_columns: st.session_state[f"agg_col_{uid}"] = col_val
            func_val = agg_data.get("func", "")
            if func_val in agg_func_options: st.session_state[f"agg_func_{uid}"] = func_val
            if agg_data.get("alias"): st.session_state[f"agg_alias_{uid}"] = agg_data["alias"]
        
        # Step 11: Load Execution Strategy into Session State
        st.session_state.sequence_order = state.get("sequence_order", 1)
        
        return True
    return False

def get_next_approval_id():
    """Fetches the highest approval_id from the database and adds 1."""
    sql = f"SELECT MAX(approval_id) FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow"
    result = run_query(sql)
    
    if result and result[0][0] is not None:
        return result[0][0] + 1
    return 1

def get_manager_user_id():
    """Fetches the user_id of the user with the 'MANAGER' role."""
    sql = f"SELECT user_id FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} WHERE role = 'MANAGER' LIMIT 1"
    result = run_query(sql)
    
    # If a manager exists, return their user_id, otherwise return None
    if result and len(result) > 0:
        return result[0][0]
    return None

def get_role_approval_level(role_name):
    """Return approval level by role for auto-approval routing."""
    level_map = {
        "CREATOR": 0,
        "MANAGER": 1,
        "TECHNICAL": 2,
        "SME": 3,
    }
    return level_map.get(str(role_name or "").upper(), 0)

def get_rules_for_approval(level_id):
    """Fetches rules assigned to a specific level for approval."""
    sql = f"""
    SELECT a.approval_id, a.rule_id, r.rule_name, a.action, a.comments
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow a
    JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r ON a.rule_id = r.rule_id
    WHERE a.level_id = {level_id}
    ORDER BY a.action_date DESC
    """
    return run_query(sql)

def get_pending_groups_for_level(level_id):
    """Return groups that currently have PENDING approvals at the given level."""
    sql = f"""
    SELECT g.group_id, g.group_name, count(*) AS pending_count
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow a
    JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r ON a.rule_id = r.rule_id
    JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id
    WHERE a.level_id = {int(level_id)} AND a.action = 'PENDING'
    GROUP BY g.group_id, g.group_name
    ORDER BY g.group_name ASC
    """
    result = run_query(sql)
    return result if result else []

def get_group_rule_rows(group_id):
    """Return all rules of a group with status."""
    sql = f"""
    SELECT rule_id, rule_name, status
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow
    WHERE group_id = {int(group_id)}
    ORDER BY rule_id ASC
    """
    result = run_query(sql)
    return result if result else []

def upsert_rule_approval_step(rule_id, level_id, action, comments, user_id, action_ts):
    """Upsert one approval_workflow row for a rule+level to keep audit history consistent."""
    user_id_sql = "NULL" if user_id in [None, "", "NULL"] else f"'{user_id}'"
    existing_sql = f"""
    SELECT approval_id
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
    WHERE rule_id = {int(rule_id)} AND level_id = {int(level_id)}
    ORDER BY approval_id DESC
    LIMIT 1
    """
    existing = run_query(existing_sql)

    if existing and len(existing) > 0:
        approval_id = existing[0][0]
        upd_sql = f"""
        UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
        SET action = '{action}', comments = '{comments}', user_id = {user_id_sql}, action_date = CAST('{action_ts}' AS TIMESTAMP)
        WHERE approval_id = {approval_id}
        """
        return run_update(upd_sql)

    new_id = get_next_approval_id()
    ins_sql = f"""
    INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
    (approval_id, rule_id, user_id, level_id, action, comments, action_date)
    VALUES ({new_id}, {int(rule_id)}, {user_id_sql}, {int(level_id)}, '{action}', '{comments}', CAST('{action_ts}' AS TIMESTAMP))
    """
    return run_update(ins_sql)

def get_group_overview_rows():
    """Return group-level counts for admin and manager decisioning."""
    sql = f"""
    SELECT
        g.group_id,
        g.group_name,
        g.execution_mode,
        count(r.rule_id) AS total_rules,
        sum(CASE WHEN r.status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_rules,
        sum(CASE WHEN r.status = 'IN_REVIEW' THEN 1 ELSE 0 END) AS in_review_rules,
        sum(CASE WHEN r.status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_rules
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g
    LEFT JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r ON g.group_id = r.group_id
    GROUP BY g.group_id, g.group_name, g.execution_mode
    ORDER BY g.group_name ASC
    """
    result = run_query(sql)
    return result if result else []

def approve_group_rules(group_id, approver_user_id, approver_role, comments="", force_admin=False):
    """Approve all rules in a group at once, then execute the group's master model."""
    if approver_role != 'MANAGER' and not force_admin:
        return False, "Only MANAGER can bulk-approve a group from this workspace."

    grp_sql = f"SELECT group_name, execution_mode FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group WHERE group_id = {int(group_id)} LIMIT 1"
    grp_res = run_query(grp_sql)
    if not grp_res:
        return False, "Group not found."

    group_name = grp_res[0][0]
    exec_mode = grp_res[0][1]
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    rules = get_group_rule_rows(group_id)
    if not rules:
        return False, "Group has no rules to approve."

    # Pre-validate all required SQL files before mutating DB status.
    missing_files = []
    for _, rule_name, _ in rules:
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(rule_name).lower())
        draft_path = os.path.join('/', 'dbt', 'formula1', 'models', 'rules', safe_group, f"{safe_name}.sql")
        final_path = os.path.join('/', 'dbt', 'formula1', 'models', 'final_rules', f"final_{safe_group}", f"final_{safe_name}.sql")
        if exec_mode == 'non_sequential':
            if not os.path.exists(draft_path):
                missing_files.append(rule_name)
        else:
            if not os.path.exists(draft_path) and not os.path.exists(final_path):
                missing_files.append(rule_name)

    if missing_files:
        return False, f"Cannot bulk-approve. Missing SQL file(s) for: {', '.join(missing_files)}"

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    comment_text = comments.strip() if comments and comments.strip() else f"Bulk group approval by {approver_role}."

    for rule_id, rule_name, _ in rules:
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(rule_name).lower())
        final_path = os.path.join('/', 'dbt', 'formula1', 'models', 'final_rules', f"final_{safe_group}", f"final_{safe_name}.sql")

        # Sequential groups require final_<rule>.sql; non-sequential uses ephemeral draft models.
        if exec_mode != 'non_sequential' and not os.path.exists(final_path):
            success_copy, copy_msg = create_final_rule_file(rule_name)
            if not success_copy:
                return False, f"File finalization failed for '{rule_name}': {copy_msg}"

        if not run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'ACTIVE', updated_at = CAST('{now}' AS TIMESTAMP) WHERE rule_id = {int(rule_id)}"):
            return False, f"Failed to mark rule ACTIVE: {rule_name}"

        # Mark all workflow levels approved for this bulk action.
        for level in (1, 2, 3):
            if not upsert_rule_approval_step(rule_id, level, 'APPROVED', comment_text, approver_user_id, now):
                return False, f"Failed to update approval workflow for rule: {rule_name}"

    success_exec, exec_msg = execute_dbt_group(group_name)
    if not success_exec:
        return False, f"Rules were approved, but group execution failed: {exec_msg}"

    return True, f"Approved {len(rules)} rule(s) in group '{group_name}' and executed the master group model."

def delete_minio_prefix(prefix):
    """Delete all MinIO objects under a prefix and return deleted object count."""
    s3_client = get_s3_client()
    deleted_count = 0
    continuation_token = None

    while True:
        kwargs = {"Bucket": MINIO_BUCKET, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        result = s3_client.list_objects_v2(**kwargs)
        keys = [obj["Key"] for obj in result.get("Contents", [])]
        for key in keys:
            s3_client.delete_object(Bucket=MINIO_BUCKET, Key=key)
            deleted_count += 1

        if not result.get("IsTruncated"):
            break
        continuation_token = result.get("NextContinuationToken")

    return deleted_count

def delete_group_with_assets(group_id, requested_by_role):
    """Admin-only group deletion that removes DB records, local SQL assets, and MinIO UI state."""
    if requested_by_role != 'ADMIN':
        return False, "Only ADMIN can delete groups."

    grp_sql = f"SELECT group_name FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group WHERE group_id = {int(group_id)} LIMIT 1"
    grp_res = run_query(grp_sql)
    if not grp_res:
        return False, "Group not found."

    group_name = grp_res[0][0]
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    rules = get_group_rule_rows(group_id)

    failed_rules = []
    for rule_id, rule_name, _ in rules:
        ok_approval = run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow WHERE rule_id = {int(rule_id)}")
        ok_rule = run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE rule_id = {int(rule_id)}")
        if not (ok_approval and ok_rule):
            failed_rules.append(rule_name)

    if failed_rules:
        return False, f"Failed to delete DB rows for: {', '.join(failed_rules)}"

    if not run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group WHERE group_id = {int(group_id)}"):
        return False, "Failed to delete group metadata row."

    warnings = []
    rules_dir = os.path.join('/', 'dbt', 'formula1', 'models', 'rules', safe_group)
    final_dir = os.path.join('/', 'dbt', 'formula1', 'models', 'final_rules', f"final_{safe_group}")

    try:
        if os.path.isdir(rules_dir):
            shutil.rmtree(rules_dir)
    except Exception as e:
        warnings.append(f"Could not remove rules directory: {e}")

    try:
        if os.path.isdir(final_dir):
            shutil.rmtree(final_dir)
    except Exception as e:
        warnings.append(f"Could not remove final_rules directory: {e}")

    try:
        deleted_objects = delete_minio_prefix(f"ui_states/{safe_group}/")
    except Exception as e:
        deleted_objects = 0
        warnings.append(f"Could not clear MinIO ui_states for group: {e}")

    summary = f"Group '{group_name}' deleted. Rules removed: {len(rules)}. MinIO objects removed: {deleted_objects}."
    if warnings:
        summary = f"{summary} Warnings: {' | '.join(warnings)}"
    return True, summary

def read_dbt_sql(rule_name):
    """Reads the generated SQL file from the DBT folder."""
    group_name = get_group_name_for_rule(rule_name)
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    
    filepath = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group, f"{safe_name}.sql")
    try:
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return f.read()
        return f"-- SQL file not found at path: {filepath}"
    except Exception as e:
        return f"-- Error reading SQL file: {e}"

def process_approval(approval_id, rule_id, rule_name, level_id, user_id, action, comments):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    upd_sql = f"""
    UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
    SET action = '{action}', comments = '{comments}', user_id = '{user_id}', action_date = CAST('{now}' AS TIMESTAMP)
    WHERE approval_id = {approval_id}
    """
    if not run_update(upd_sql): 
        return False, "Database update failed."

    if action == 'REJECTED':
        run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'REJECTED' WHERE rule_id = {rule_id}")
        return True, "Rule Rejected."
        
    elif action == 'APPROVED':
        if level_id < 3:
            next_level = level_id + 1
            existing_next_sql = f"""
            SELECT approval_id
            FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
            WHERE rule_id = {rule_id} AND level_id = {next_level}
            ORDER BY approval_id DESC
            LIMIT 1
            """
            existing_next = run_query(existing_next_sql)

            if existing_next and len(existing_next) > 0:
                next_approval_id = existing_next[0][0]
                reuse_sql = f"""
                UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
                SET action = 'PENDING', comments = 'Awaiting Level {next_level} review.', user_id = NULL, action_date = CAST('{now}' AS TIMESTAMP)
                WHERE approval_id = {next_approval_id}
                """
                run_update(reuse_sql)
            else:
                next_app_id = get_next_approval_id()
                ins_sql = f"""
                INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
                (approval_id, rule_id, user_id, level_id, action, comments, action_date)
                VALUES ({next_app_id}, {rule_id}, NULL, {next_level}, 'PENDING', 'Awaiting Level {next_level} review.', CAST('{now}' AS TIMESTAMP))
                """
                run_update(ins_sql)
            return True, "Rule routed forward."
        else:
            # Level 3 (SME) approved it -> Rule is now ACTIVE
            run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'ACTIVE' WHERE rule_id = {rule_id}")
            success_copy, copy_msg = create_final_rule_file(rule_name)
            
            if success_copy:
                # Fetch Group Info
                grp_sql = f"SELECT g.group_id, g.group_name, g.execution_mode FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id WHERE r.rule_id = {rule_id}"
                grp_res = run_query(grp_sql)
                g_id, g_name, exec_mode = grp_res[0]
                
                # UNIFIED EXECUTION LOGIC: Both modes wait for full group approval to run the Master file
                if check_group_fully_approved(g_id):
                    generate_group_sql(g_id, g_name)
                    success_exec, exec_msg = execute_dbt_group(g_name)
                    return (True, f"✅ Group Fully Approved! Master group compiled and executed! {exec_msg}") if success_exec else (False, f"⚠️ Master compiled, but execution failed: {exec_msg}")
                else:
                    return True, "✅ Rule ACTIVE. Waiting for other rules in the group to be approved before executing the Master group file."
            else:
                return False, f"❌ Rule ACTIVE, but file copy failed: {copy_msg}"
            
    return True, "Processed."

def get_latest_rule_comment(rule_id):
    """Fetches the most recent comment and status for a given rule from the approval workflow."""
    sql = f"""
    SELECT a.action, a.comments, u.username, a.level_id
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow a
    LEFT JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} u ON a.user_id = u.user_id
    WHERE a.rule_id = {rule_id}
    ORDER BY a.action_date DESC
    LIMIT 1
    """
    result = run_query(sql)
    return result[0] if result and len(result) > 0 else None

def create_final_rule_file(rule_name):
    """Copies the approved rule from /rules to /final_rules with the 'final_' prefix."""
    group_name = get_group_name_for_rule(rule_name)
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    
    draft_path = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group, f"{safe_name}.sql")
    
    # --- UPDATE THIS LINE to include 'final_' in the folder name ---
    final_dir = os.path.join("/", "dbt", "formula1", "models", "final_rules", f"final_{safe_group}")
    
    final_path = os.path.join(final_dir, f"final_{safe_name}.sql")

    try:
        if not os.path.exists(draft_path):
            return False, f"Source file not found at: {draft_path}"
        os.makedirs(final_dir, exist_ok=True)
        shutil.copy2(draft_path, final_path)
        
        if os.path.exists(final_path):
            return True, f"✅ Success! File created at: {final_path}"
        else:
            return False, "Copy command executed, but file is missing."
    except Exception as e:
        return False, f"File creation error: {e}"

def execute_dbt_rule(rule_name):
    """Executes the finalized rule via Docker exec from within the Streamlit container."""
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    
    # --- FIXED: Join with rule_group to get the execution_mode ---
    mode_sql = f"""
    SELECT g.execution_mode 
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r
    JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id
    WHERE r.rule_name = '{rule_name}'
    """
    mode_result = run_query(mode_sql)
    
    execution_mode = "non_sequential"
    if mode_result and len(mode_result) > 0 and mode_result[0][0]:
        execution_mode = mode_result[0][0]
    
    # Append '+' if sequential to trigger DAG dependencies
    dbt_target = f"+final_{safe_name}" if execution_mode == 'sequential' else f"final_{safe_name}"
    
    command = f'docker exec dbt bash -c "cd formula1 && dbt run --select {dbt_target}"'
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=False
        )

        if result.returncode == 0:
            return True, f"✅ DBT Executed Successfully (Target: {dbt_target})!\n\nExecution Logs:\n{result.stdout}"
        else:
            error_output = result.stderr if result.stderr else result.stdout
            return False, f"DBT execution failed (Target: {dbt_target}). Logs:\n{error_output}"
            
    except Exception as e:
        return False, f"System error calling Docker: {e}"
    
def get_all_groups():
    """Fetches all rule groups from the database."""
    sql = f"SELECT group_id, group_name, execution_mode FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group ORDER BY group_name ASC"
    result = run_query(sql)
    return result if result else []

def get_next_group_id():
    """Fetches the highest group_id from the database and adds 1."""
    sql = f"SELECT MAX(group_id) FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group"
    result = run_query(sql)
    if result and result[0][0] is not None:
        return result[0][0] + 1
    return 1

def get_rules_by_group(group_id):
    """Fetches existing rules that belong to a specific group."""
    sql = f"SELECT rule_id, rule_name, sequence_order FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE group_id = {group_id} ORDER BY sequence_order ASC"
    result = run_query(sql)
    return result if result else []

def get_group_rules_for_sequence(group_id, exclude_rule_id=None):
    """Return rules sorted by internal sequence for sequence-planning operations."""
    rows = get_rules_by_group(group_id)
    normalized = []
    for rule_id, rule_name, sequence_order in rows:
        if exclude_rule_id is not None and rule_id == exclude_rule_id:
            continue
        normalized.append({
            "rule_id": rule_id,
            "rule_name": rule_name,
            "sequence_order": int(sequence_order or 0)
        })
    normalized.sort(key=lambda x: x["sequence_order"])
    return normalized

def map_rules_to_visual_sequence(sorted_rules):
    """Map sorted internal sequence rows to a 1-based visual sequence for the UI."""
    mapped = []
    for idx, item in enumerate(sorted_rules, start=1):
        mapped.append({
            "visual_sequence": idx,
            "rule_id": item["rule_id"],
            "rule_name": item["rule_name"],
            "sequence_order": item["sequence_order"]
        })
    return mapped

def get_rule_visual_position(group_id, rule_id):
    """Get current 1-based visual position for a rule in its group."""
    mapped = map_rules_to_visual_sequence(get_group_rules_for_sequence(group_id))
    for row in mapped:
        if row["rule_id"] == rule_id:
            return row["visual_sequence"]
    return len(mapped) + 1

def calculate_sequence_plan(sorted_rules, target_visual_sequence, moving_rule):
    """Calculate sequence updates for insert/move operations.

    Scenario A: append/no collision -> internal = target * 1000
    Scenario B: collision at visual position > 1 -> floor(avg(prev, current))
    Scenario C: insert at visual 1 -> full reindex to 1000 steps
    Rebalance fallback: if there is no gap between adjacent values, full reindex.
    """
    base = list(sorted_rules)
    target = max(1, int(target_visual_sequence))

    # Scenario C: insert/move to first position with full reindex.
    if target == 1:
        ordered = [moving_rule] + base
        batch_updates = []
        for idx, row in enumerate(ordered, start=1):
            batch_updates.append({
                "rule_id": row["rule_id"],
                "rule_name": row["rule_name"],
                "sequence_order": idx * 1000,
                "visual_sequence": idx
            })
        return {
            "scenario": "C",
            "mode": "batch",
            "new_sequence_order": 1000,
            "batch_updates": batch_updates
        }

    # Scenario A: append or position beyond current max visual index.
    if target > len(base):
        return {
            "scenario": "A",
            "mode": "single",
            "new_sequence_order": target * 1000,
            "batch_updates": []
        }

    # Scenario B: collision at an existing visual index > 1.
    prev_seq = int(base[target - 2]["sequence_order"])
    curr_seq = int(base[target - 1]["sequence_order"])

    # If no integer gap exists, rebalance the entire ordering to 1000 increments.
    if curr_seq - prev_seq <= 1:
        ordered = base[:target - 1] + [moving_rule] + base[target - 1:]
        batch_updates = []
        for idx, row in enumerate(ordered, start=1):
            batch_updates.append({
                "rule_id": row["rule_id"],
                "rule_name": row["rule_name"],
                "sequence_order": idx * 1000,
                "visual_sequence": idx
            })
        return {
            "scenario": "B_REBALANCE",
            "mode": "batch",
            "new_sequence_order": target * 1000,
            "batch_updates": batch_updates
        }

    new_seq = (prev_seq + curr_seq) // 2
    return {
        "scenario": "B",
        "mode": "single",
        "new_sequence_order": new_seq,
        "batch_updates": []
    }

def update_rule_sequence(rule_id, sequence_order):
    """Single-record backend update handler for sequence_order."""
    sql = f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET sequence_order = {int(sequence_order)} WHERE rule_id = {int(rule_id)}"
    return run_update(sql)

def update_rule_sequence_batch(sequence_updates):
    """Batch backend update handler for full reindex operations."""
    ok = True
    for row in sequence_updates:
        ok = update_rule_sequence(row["rule_id"], row["sequence_order"]) and ok
    return ok

def run_sequence_algorithm_tests():
    """Validation helper for the required A/B/C sequence test flow."""
    initial = [
        {"rule_id": 1, "rule_name": "rule1", "sequence_order": 1000},
        {"rule_id": 2, "rule_name": "rule2", "sequence_order": 2000},
        {"rule_id": 3, "rule_name": "rule3", "sequence_order": 3000},
    ]

    action1 = calculate_sequence_plan(initial, 2, {"rule_id": 4, "rule_name": "rule4"})
    action1_seq = action1["new_sequence_order"]
    after_action1 = [
        {"rule_id": 1, "rule_name": "rule1", "sequence_order": 1000},
        {"rule_id": 4, "rule_name": "rule4", "sequence_order": action1_seq},
        {"rule_id": 2, "rule_name": "rule2", "sequence_order": 2000},
        {"rule_id": 3, "rule_name": "rule3", "sequence_order": 3000},
    ]
    after_action1.sort(key=lambda x: x["sequence_order"])

    action2 = calculate_sequence_plan(after_action1, 1, {"rule_id": 5, "rule_name": "rule5"})
    after_action2 = action2["batch_updates"]

    action1_pass = (
        action1["scenario"] == "B"
        and action1_seq == 1500
        and [x["rule_name"] for x in after_action1] == ["rule1", "rule4", "rule2", "rule3"]
    )
    action2_pass = (
        action2["scenario"] == "C"
        and [x["sequence_order"] for x in after_action2] == [1000, 2000, 3000, 4000, 5000]
        and [x["rule_name"] for x in after_action2] == ["rule5", "rule1", "rule4", "rule2", "rule3"]
    )

    return action1_pass and action2_pass

def get_group_name_for_rule(rule_name):
    """Fetches the group_name for a given rule_name from the database."""
    sql = f"""
    SELECT g.group_name 
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r
    JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id
    WHERE r.rule_name = '{rule_name}'
    """
    result = run_query(sql)
    return result[0][0] if result and len(result) > 0 else "ungrouped"

def setup_new_group_environment(group_name):
    """Creates physical folders and updates dbt_project.yml without losing comments."""
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    
    # 1. Create Local DBT Folders
    rules_dir = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group)
    final_rules_dir = os.path.join("/", "dbt", "formula1", "models", "final_rules", f"final_{safe_group}")
    
    os.makedirs(rules_dir, exist_ok=True)
    os.makedirs(final_rules_dir, exist_ok=True)
    
    # 2. Safely Update dbt_project.yml
    yml_path = os.path.join("/", "dbt", "formula1", "dbt_project.yml")
    if not os.path.exists(yml_path):
        return False, "dbt_project.yml not found."
        
    with open(yml_path, 'r') as f:
        lines = f.readlines()
        
    # The exact strings to inject
    rules_str = f"      {safe_group}:\n        +schema: {safe_group}\n"
    
    # Prevent duplicate entries if a user tries to recreate a group
    content_str = "".join(lines)
    if rules_str in content_str:
        return True, "Folders created; YAML already configured."
        
    new_lines = []
    in_rules = False
    in_final_rules = False
    
    for line in lines:
        new_lines.append(line)
        
        # Inject under rules
        if line.strip() == "rules:":
            in_rules = True
        elif in_rules and line.strip() == "+schema: rules":
            new_lines.append(rules_str)
            in_rules = False
            
        # Keep all final outputs in shared final_rules schema via per-model config.
        if line.strip() == "final_rules:":
            in_final_rules = True
        elif in_final_rules and line.strip() == "+schema: final_rules":
            in_final_rules = False
            
    # Write the updated lines back to the file
    with open(yml_path, 'w') as f:
        f.writelines(new_lines)
        
    return True, "Folders and YAML updated successfully."

def generate_group_sql(group_id, group_name):
    """
    Fetches rules for ANY group, checks the execution mode, and 
    generates a single master SQL file using the appropriate macro.
    """
    # 1. Fetch rules in the correct order
    rules = get_rules_by_group(group_id)
    if not rules:
        return False, "No rules found in group."
        
    # 2. Fetch group details including our new non-sequential fields
    grp_sql = f"SELECT execution_mode, key_column, target_column FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group WHERE group_id = {group_id}"
    grp_res = run_query(grp_sql)
    if not grp_res:
        return False, "Group configuration not found."

    exec_mode = grp_res[0][0]
    key_column = grp_res[0][1]
    target_column = grp_res[0][2]

    def normalize_column_token(col_name):
        """Strip qualifiers/quotes so macro receives a raw output column name."""
        if not isinstance(col_name, str):
            return ""
        token = col_name.strip().strip('`').strip('"')
        if "." in token:
            token = token.split(".")[-1]
        return token

    key_column = normalize_column_token(key_column)
    target_column = normalize_column_token(target_column)
    if not key_column or not target_column:
        return False, "Group key/target columns are required for non-sequential execution."
    
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    
    # ==========================================
    # BRANCH A: NON-SEQUENTIAL EXECUTION
    # ==========================================
    if exec_mode == 'non_sequential':
        ok_ephemeral, ep_msg = ensure_non_sequential_rules_ephemeral(group_id, group_name)
        if not ok_ephemeral:
            return False, ep_msg

        # Reference group rule model names directly so ephemeral models compile as CTEs.
        rule_models = [safe_model_name(r[1]) for r in rules]
        
        generated_sql = f"""{{{{ config(materialized='table', schema='{FINAL_RULES_SCHEMA}', alias='{safe_group}') }}}}

{{{{ non_sequential_rule_engine(
    rule_models={rule_models},
    key_column='{key_column}',
    target_column='{target_column}'
) }}}}"""

    # ==========================================
    # BRANCH B: SEQUENTIAL EXECUTION (Your existing logic)
    # ==========================================
    else:
        group_steps = []
        rule_cte_map = {r[1]: safe_model_name(r[1]) for r in rules}
        relation_map = {}
        for rule_name, cte_name in rule_cte_map.items():
            safe_name = safe_model_name(rule_name)
            relation_map[rule_name] = cte_name
            relation_map[safe_name] = cte_name
            relation_map[f"final_{safe_name}"] = cte_name
        
        for row in rules:
            rule_id, rule_name, seq_order = row
            state = load_ui_state(rule_name, group_name)
            
            if not state:
                continue
                
            t1_raw = state.get("primary_table")
            t1, is_primary_cte = resolve_group_relation_name(t1_raw, relation_map)
            if not t1:
                continue
            
            from_table = t1

            formatted_joins = []
            for j in state.get("joins", []):
                if j.get("right_table") and j.get("join_type") != "CROSS JOIN":
                    l_table = normalize_ref_name(j.get("left_table")) if j.get("left_table") else t1
                    r_table = normalize_ref_name(j.get("right_table"))
                    l_table_resolved, _ = resolve_group_relation_name(l_table, relation_map)
                    r_table_resolved, r_is_cte = resolve_group_relation_name(r_table, relation_map)
                    if not r_table_resolved:
                        continue
                    
                    formatted_joins.append({
                        'type': j.get("join_type"),
                        'table': r_table_resolved,
                        'is_cte': r_is_cte,
                        'left': f"{l_table_resolved}.{j.get('left_col')}",
                        'right': f"{r_table_resolved}.{j.get('right_col')}"
                    })

            select_cols = [f"{t1}.{c}" for c in state.get("primary_columns", [])]
            for r_table, cols in state.get("cols_joins", {}).items():
                resolved_r_table, _ = resolve_group_relation_name(r_table, relation_map)
                if not resolved_r_table:
                    continue
                select_cols.extend([f"{resolved_r_table}.{c}" for c in cols])

            macro_group_by = []
            aggs = state.get("aggregations", [])
            manual_gb = [rewrite_column_reference(c, relation_map) for c in state.get("manual_group_by", [])]
            having = state.get("having_filters", [])
            if aggs or having or manual_gb:
                macro_group_by = list(dict.fromkeys(select_cols + manual_gb))

            macro_where = []
            for f in state.get("where_filters", []):
                if f['col']:
                    # --- NEW: Safely extract mathematical operator from parentheses ---
                    raw_op = f.get('op', '')
                    if '(' in raw_op and ')' in raw_op:
                        op_val = raw_op[raw_op.find("(")+1:raw_op.find(")")]
                    else:
                        op_val = raw_op.split(" ")[0] if " " in raw_op and "equals" in raw_op else raw_op
                    
                    if op_val == "equals": op_val = "="
                    
                    val = str(f.get('val', ''))
                    if val and not is_numeric_literal(val) and op_val not in ['IS NULL', 'IS NOT NULL']:
                        val = f"'{val}'"
                        
                    # --- NEW: Safely extract val2 for BETWEEN clauses ---
                    val2 = str(f.get('val2', ''))
                    if val2 and not is_numeric_literal(val2) and op_val not in ['IS NULL', 'IS NOT NULL']:
                        val2 = f"'{val2}'"

                    macro_where.append({
                        'col': rewrite_column_reference(f['col'], relation_map),
                        'op': op_val,
                        'value': val,
                        'value2': val2,
                        'logic': f.get('logic', '')
                    })

            macro_order_by = []
            for o in state.get("order_filters", []):
                if o.get('col'):
                    macro_order_by.append({
                        'col': rewrite_column_reference(o['col'], relation_map),
                        'dir': o['dir']
                    })

            step_dict = {
                "rule_name": rule_cte_map.get(rule_name, safe_model_name(rule_name)),
                "sequence_order": seq_order,
                "primary_table": from_table,
                "is_primary_cte": is_primary_cte,
                "select_columns": select_cols,
                "aggregations": [{'func': a['func'], 'col': a['col'], 'alias': a.get('alias', '')} for a in aggs if a['col'] and a['func']],
                "joins": formatted_joins,
                "where_filters": macro_where,
                "group_by": macro_group_by,
                "having": macro_having,
                "order_by": macro_order_by,
                "limit_rows": state.get("row_limit") or 'None'
            }
            group_steps.append(step_dict)

        generated_sql = f"""{{{{ config(materialized='table', schema='{FINAL_RULES_SCHEMA}', alias='{safe_group}') }}}}

{{{{ sequential_rule_engine(
    steps={group_steps}
) }}}}"""

    # ==========================================
    # 3. SAVE TO FINAL_RULES FOLDER
    # ==========================================
    dbt_dir = os.path.join("/", "dbt", "formula1", "models", "final_rules", f"final_{safe_group}")
    os.makedirs(dbt_dir, exist_ok=True)
    
    filepath = os.path.join(dbt_dir, f"final_{safe_group}.sql")
    with open(filepath, "w") as f:
        f.write(generated_sql)
        
    return True, filepath

def check_group_fully_approved(group_id):
    """Checks if all rules in a specific group have 'ACTIVE' status."""
    sql = f"SELECT count(*) FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE group_id = {group_id} AND status != 'ACTIVE'"
    res = run_query(sql)
    return True if (res and res[0][0] == 0) else False

def execute_dbt_group(group_name):
    """Executes the unified group SQL file."""
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    dbt_target = f"final_{safe_group}"

    grp_sql = f"SELECT group_id FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group WHERE group_name = '{group_name}' LIMIT 1"
    grp_res = run_query(grp_sql)
    if not grp_res:
        return False, f"Group not found: {group_name}"

    group_id = grp_res[0][0]
    generated_ok, generated_msg = generate_group_sql(group_id, group_name)
    if not generated_ok:
        return False, f"Failed to generate master group SQL: {generated_msg}"
    
    command = f'docker exec dbt bash -c "cd formula1 && dbt run --select {dbt_target}"'
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return True, f"✅ Group SQL generated at {generated_msg} and executed successfully!\n\nLogs:\n{result.stdout}"
        else:
            error_output = result.stderr if result.stderr else result.stdout
            return False, f"Group SQL generated at {generated_msg}, but execution failed. Logs:\n{error_output}"
    except Exception as e:
        return False, f"System error calling Docker: {e}"



# ------------------------------------------------------------------------------
# 5. MAIN APPLICATION UI
# ------------------------------------------------------------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user_info = None

# --- NOT LOGGED IN ---
if not st.session_state.logged_in:
    st.title("🛡️ DBT Rule Engine Access")
    
    tab1, tab2 = st.tabs(["🔑 Login", "📝 Register"])

    # Login Tab
    with tab1:
        st.subheader("Login to your account")
        
        login_id = st.text_input("Username or Email", key="login_id")
        login_pw = st.text_input("Password", type="password", key="login_pw")
        
        # 1. FORGOT PASSWORD POPOVER (Left Aligned)
        with st.popover("Forgot Password?"):
            st.markdown("### 🔐 Reset Password")
            st.caption("Verify your identity to set a new password.")
            
            # Input fields inside the popup
            fp_email = st.text_input("Registered Email", key="fp_email")
            fp_mobile = st.text_input("Registered Mobile", key="fp_mobile")
            
            st.divider()
            
            fp_new_pw = st.text_input("New Password", type="password", key="fp_new")
            fp_conf_pw = st.text_input("Confirm New Password", type="password", key="fp_conf")
            
            # Action Button
            if st.button("Update Password", type="primary"):
                if not (fp_email and fp_mobile and fp_new_pw and fp_conf_pw):
                    st.error("All fields are required.")
                elif fp_new_pw != fp_conf_pw:
                    st.error("Passwords do not match.")
                elif not is_valid_password(fp_new_pw):
                    st.error("Password too weak (Min 8 chars, Upper, Lower, Digit, Special).")
                elif not verify_user_for_reset(fp_email, fp_mobile):
                    st.error("❌ Details do not match our records.")
                else:
                    if reset_password(fp_email, fp_new_pw):
                        st.success("✅ Password Updated! You can now log in.")
                    else:
                        st.error("Database Error.")

        # 2. LOGIN BUTTON (Default Left Alignment, Small Size)
        st.write("") # Small vertical spacer
        if st.button("Login", type="primary"):
            with st.spinner("Verifying credentials..."):
                user = verify_user(login_id, login_pw)
                if user:
                    st.session_state.logged_in = True
                    st.session_state.user_info = user
                    st.success(f"Welcome back, {user['firstname']}!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Invalid Credentials.")

    # Register Tab (SECURED)
    with tab2:
        st.subheader("Create New Account")
        st.caption("New accounts are assigned the 'CREATOR' role by default.")
        
        col1, col2 = st.columns(2)
        with col1:
            r_fname = st.text_input("First Name")
            r_uname = st.text_input("Username")
            r_phone = st.text_input("Mobile Number")
        with col2:
            r_lname = st.text_input("Last Name")
            r_email = st.text_input("Email")
            # Role field removed from UI
        
        r_pw = st.text_input("Password", type="password", help="Min 8 chars, 1 Upper, 1 Lower, 1 Digit, 1 Special")
        r_pw_conf = st.text_input("Confirm Password", type="password")

        if st.button("Register"):
            if not (r_fname and r_lname and r_uname and r_email and r_pw):
                st.warning("All fields are required.")
            elif r_pw != r_pw_conf:
                st.error("❌ Passwords do not match.")
            elif not is_valid_password(r_pw):
                st.error("❌ Password complexity requirements not met.")
            elif not is_valid_phone(r_phone):
                st.error("❌ Invalid Mobile Number.")
            else:
                with st.spinner("Creating account..."):
                    # Role is now handled internally
                    success, msg = register_user(r_fname, r_lname, r_uname, r_email, r_phone, r_pw)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)

# --- LOGGED IN DASHBOARD (NEW UI) ---
else:
    user = st.session_state.user_info

    # --- SIDEBAR NAV ---
    with st.sidebar:
        st.markdown("### 🔀 DBT Rule Engine")
        st.caption("Query Builder Environment")
        st.divider()
        st.markdown("📂 Data Sources")
        st.markdown("🔖 Saved Rules")
        st.markdown("**🔗 Query Builder**") # Highlighted current page
        st.markdown("📈 Risk Analytics")
        st.markdown("⚙️ Settings")
        st.divider()
        st.header(f"👤 {user['firstname']} {user['lastname']}")
        st.caption(f"Role: {user['role']}")
        if st.button("🚪 Logout", use_container_width=True):
            keys_to_keep = {'logged_in', 'user_info'}
            for key in list(st.session_state.keys()):
                if key not in keys_to_keep:
                    del st.session_state[key]
            st.session_state.logged_in = False
            st.session_state.user_info = None
            st.rerun()

    role = user["role"]

    previous_role = st.session_state.get('_last_active_role')
    if previous_role != role:
        clear_session_for_role(role)
        st.session_state._last_active_role = role

    if 'mgr_show_rule_builder' not in st.session_state:
        st.session_state.mgr_show_rule_builder = False

    if role == 'CREATOR' or (role in ['MANAGER', 'TECHNICAL', 'SME'] and st.session_state.get('mgr_show_rule_builder', False)):
        # Fetch DB F1 Schemas dynamically from MinIO Iceberg Metadata
        # Fetch DB F1 Schemas dynamically from MinIO Iceberg Metadata
        try:
            TABLE_SCHEMAS = get_iceberg_schemas()
            if not TABLE_SCHEMAS:
                st.warning("No schemas found in MinIO. Please check your connection and paths.")
        except Exception as e:
            st.error(f"Failed to fetch schemas from MinIO: {e}")
            TABLE_SCHEMAS = {}
            
        # --- NEW: Inject saved rules (including Ephemeral) into the UI Dropdowns ---
        selected_group_id = st.session_state.get("active_group_id")

        @st.cache_data
        def get_simulated_schemas(group_id, cache_bust_key):
            rules = get_existing_rules(group_id=group_id)
            extra_schemas = {}
            for r in rules:
                rule_nm = r[1]
                target_model_name = r[3]
                rule_group_name = get_group_name_for_rule(rule_nm)
                state = load_ui_state(rule_nm, rule_group_name)
                if state:
                    simulated_cols = state.get("primary_columns", [])
                    for j_cols in state.get("cols_joins", {}).values():
                        simulated_cols.extend(j_cols)
                    for agg in state.get("aggregations", []):
                        if agg.get("alias"):
                            simulated_cols.append(agg["alias"])
                    if simulated_cols:
                        extra_schemas[target_model_name] = list(set(simulated_cols))
            return extra_schemas

        simulated = get_simulated_schemas(
            selected_group_id,
            st.session_state.get("schema_cache_key", 0)
        )
        for model_name, cols in simulated.items():
            if model_name not in TABLE_SCHEMAS:
                TABLE_SCHEMAS[model_name] = cols

        TABLE_NAMES = list(TABLE_SCHEMAS.keys())
        if not TABLE_NAMES:
            st.error("No tables available to build query.")
            st.stop()

        # Initialize dynamic session states for UI forms
        if 'schema_cache_key' not in st.session_state:
            st.session_state.schema_cache_key = 0
        if 'joins' not in st.session_state:
            st.session_state.joins = []
        if 'where_filters' not in st.session_state:
            st.session_state.where_filters = [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "logic": "AND"}]
        if 'order_filters' not in st.session_state:
            st.session_state.order_filters = [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}]
        if 'aggregations' not in st.session_state:
            st.session_state.aggregations = []
        if 'having_filters' not in st.session_state:
            st.session_state.having_filters = []
        if 'manual_group_by' not in st.session_state:
            st.session_state.manual_group_by = []
        if 'row_limit' not in st.session_state:
            st.session_state.row_limit = None
    elif role == 'ADMIN':
        TABLE_SCHEMAS = {}
        TABLE_NAMES = []
    elif role in ['MANAGER', 'TECHNICAL', 'SME']:
        TABLE_SCHEMAS = {}
        TABLE_NAMES = []

    if role == "ADMIN":
        st.title("🛡️ Admin Control Panel")
        
        tab1, tab2 = st.tabs(["👥 User Management", "🗂️ Global Rule Oversight"])
        
        # ------------------------------------------------------------------------------
        # PART 1: USER & ROLE MANAGEMENT
        # ------------------------------------------------------------------------------
        with tab1:
            st.subheader("Manage Users & Roles")

            # --- NEW: ADD USER FEATURE ---
            with st.expander("➕ Add New User", expanded=False):
                with st.form("admin_create_user_form", clear_on_submit=True):
                    st.caption("Create a new user and assign their system role directly.")
                    c1, c2 = st.columns(2)
                    with c1:
                        n_fname = st.text_input("First Name*")
                        n_uname = st.text_input("Username*")
                        n_phone = st.text_input("Mobile Number*")
                        n_role = st.selectbox("Assign Role*", ["CREATOR", "MANAGER", "TECHNICAL", "SME", "ADMIN"])
                    with c2:
                        n_lname = st.text_input("Last Name*")
                        n_email = st.text_input("Email*")
                        n_pw = st.text_input("Temporary Password*", type="password", help="User can reset this later.")
                    
                    submit_user = st.form_submit_button("Create User", type="primary")
                    
                    if submit_user:
                        if not (n_fname and n_lname and n_uname and n_email and n_phone and n_pw):
                            st.error("⚠️ All fields are required.")
                        elif check_user_exists(n_uname):
                            st.error("⚠️ Username already exists.")
                        elif check_user_exists(n_email):
                            st.error("⚠️ Email already exists.")
                        else:
                            new_id = str(uuid.uuid4())
                            n_hash = hash_password(n_pw)
                            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            insert_sql = f"""
                            INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
                            (user_id, firstname, lastname, username, email, mobile, password_hash, role, created_at, last_login)
                            VALUES (
                                '{new_id}', '{n_fname}', '{n_lname}', '{n_uname}', 
                                '{n_email}', '{n_phone}', '{n_hash}', '{n_role}', 
                                CAST('{now}' AS TIMESTAMP), NULL
                            )
                            """
                            if run_update(insert_sql):
                                st.success(f"✅ User '{n_uname}' created successfully with role '{n_role}'!")
                                time.sleep(1.5)
                                st.rerun()

            st.divider()
            
            users_sql = f"SELECT user_id, username, email, role, CAST(last_login AS STRING) as last_login FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} ORDER BY created_at DESC"

            st.caption("Edit the 'System Role' column to change permissions. Set a user to 'SUSPENDED' to deactivate their access.")
            
            # Fetch all users
            users_sql = f"SELECT user_id, username, email, role, CAST(last_login AS STRING) as last_login FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} ORDER BY created_at DESC"
            users_data = run_query(users_sql)
            
            if users_data:
                df_users = pd.DataFrame(users_data, columns=["user_id", "username", "email", "role", "last_login"])
                
                # Render interactive editable dataframe
                edited_df = st.data_editor(
                    df_users,
                    column_config={
                        "role": st.column_config.SelectboxColumn(
                            "System Role",
                            options=["CREATOR", "MANAGER", "TECHNICAL", "SME", "ADMIN", "SUSPENDED"],
                            required=True,
                        ),
                        "user_id": None, # Hide the UUID from the UI
                        "username": "Username",
                        "email": "Email Address",
                        "last_login": "Last Login"
                    },
                    disabled=["username", "email", "last_login"], # Prevent editing of core identity fields
                    hide_index=True,
                    key="user_editor",
                    use_container_width=True
                )
                
                # Save changes button
                if st.button("💾 Save Role Changes", type="primary"):
                    changes_made = False
                    with st.spinner("Updating user roles..."):
                        for index, row in edited_df.iterrows():
                            orig_role = df_users.loc[index, 'role']
                            new_role = row['role']
                            
                            # Only execute update if the role was actually changed
                            if orig_role != new_role:
                                uid = row['user_id']
                                upd_sql = f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} SET role = '{new_role}' WHERE user_id = '{uid}'"
                                if run_update(upd_sql):
                                    changes_made = True
                                    
                    if changes_made:
                        st.success("✅ User roles updated successfully!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.info("No changes detected.")
            else:
                st.warning("No users found in the system.")

        # ------------------------------------------------------------------------------
        # PART 2: GLOBAL RULE OVERSIGHT & GOD-MODE
        # ------------------------------------------------------------------------------
        with tab2:
            st.subheader("Master Rule View")
            st.caption("Overview of all rules generated across the platform.")
            
            rules_sql = f"""
            SELECT rule_id, rule_name, status, created_by, CAST(created_at AS STRING) as created_at 
            FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow 
            ORDER BY rule_id DESC
            """
            rules_data = run_query(rules_sql)
            
            if rules_data:
                df_rules = pd.DataFrame(rules_data, columns=["Rule ID", "Rule Name", "Status", "Created By", "Created At"])
                st.dataframe(df_rules, use_container_width=True, hide_index=True)
                
                st.divider()
                st.markdown("### ⚡ God-Mode Actions")
                st.caption("Force status changes or permanently delete rules and their associated files.")
                
                # Rule selection dropdown for actions
                rule_dict = {f"ID: {r[0]} | {r[1]} (Status: {r[2]})": r for r in rules_data}
                selected_rule_str = st.selectbox("Select Rule to Modify:", list(rule_dict.keys()))
                selected_rule = rule_dict[selected_rule_str]
                
                sel_rule_id = selected_rule[0]
                sel_rule_name = selected_rule[1]
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("✅ Force Approve", use_container_width=True):
                        if run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'ACTIVE' WHERE rule_id = {sel_rule_id}"):
                            action_success = False
                            
                            # --- NEW: Add the Level 3 Approval Record so UI updates! ---
                            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            admin_user_id = user['user_id']
                            next_app_id = get_next_approval_id()
                            
                            force_approve_sql = f"""
                            INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow
                            (approval_id, rule_id, user_id, level_id, action, comments, action_date)
                            VALUES (
                                {next_app_id}, {sel_rule_id}, '{admin_user_id}', 3, 
                                'APPROVED', 'Force Approved by System Admin.', CAST('{now}' AS TIMESTAMP)
                            )
                            """
                            run_update(force_approve_sql)
                            # ------------------------------------------------------------

                            with st.spinner("Finalizing file and checking group status..."):
                                success_copy, copy_msg = create_final_rule_file(sel_rule_name)
                                
                                if success_copy:
                                    grp_sql = f"SELECT g.group_id, g.group_name, g.execution_mode FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id WHERE r.rule_id = {sel_rule_id}"
                                    grp_res = run_query(grp_sql)
                                    g_id, g_name, exec_mode = grp_res[0]

                                    if check_group_fully_approved(g_id):
                                        success_exec, exec_msg = execute_dbt_group(g_name)
                                        if success_exec:
                                            st.success("✅ Group fully approved. Master group file generated and executed.")
                                            action_success = True
                                        else:
                                            st.error(f"⚠️ Group generation/execution failed:\n{exec_msg}")
                                    else:
                                        st.success("✅ Rule forced to ACTIVE. Execution paused until all rules in group are approved.")
                                        action_success = True
                                else:
                                    st.error(f"❌ Forced to ACTIVE, but file issue: {copy_msg}")

                            if action_success:
                                time.sleep(2)
                                st.rerun()
                            
                with col2:
                    if st.button("❌ Force Reject", use_container_width=True):
                        if run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'REJECTED' WHERE rule_id = {sel_rule_id}"):
                            st.warning(f"Rule ID {sel_rule_id} forced to REJECTED.")
                            time.sleep(1)
                            st.rerun()
                            
                with col3:
                    if st.button("🗑️ Delete Rule", type="primary", use_container_width=True):
                        group_name = get_group_name_for_rule(sel_rule_name)
                        safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower()) if group_name else "ungrouped"
                        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', sel_rule_name.lower())

                        # 1. Delete associated approval records first (Foreign Key constraint logic)
                        run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow WHERE rule_id = {sel_rule_id}")
                        
                        # 2. Delete the rule from the main table
                        if run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE rule_id = {sel_rule_id}"):

                            # 3. Physically delete the generated files
                            # A. Delete Draft SQL file from /rules folder
                            sql_path = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group, f"{safe_name}.sql")
                            if os.path.exists(sql_path): 
                                os.remove(sql_path)
                                
                            # B. Delete Final SQL file from /final_rules folder (if it was approved)
                            final_sql_path = os.path.join("/", "dbt", "formula1", "models", "final_rules", f"final_{safe_group}", f"final_{safe_name}.sql")
                            if os.path.exists(final_sql_path):
                                os.remove(final_sql_path)
                                
                            # C. Delete JSON State from MinIO
                            try:
                                s3_client = get_s3_client()
                                s3_client.delete_object(Bucket=MINIO_BUCKET, Key=f"ui_states/{safe_group}/{safe_name}.json")
                            except Exception as e:
                                st.warning(f"Note: Could not delete UI state from MinIO: {e}")
                                
                            st.error(f"Rule ID {sel_rule_id} and all associated files (Draft & Final) have been permanently deleted.")
                            time.sleep(1.5)
                            st.rerun()
            else:
                st.info("No rules currently exist in the database.")

            st.divider()
            st.markdown("### ⚡ Group-Level God-Mode Actions")
            st.caption("Bulk-approve an entire group or permanently delete a group and all linked assets.")

            group_rows = get_group_overview_rows()
            if group_rows:
                group_df = pd.DataFrame(
                    group_rows,
                    columns=["Group ID", "Group Name", "Mode", "Total Rules", "Active", "In Review", "Rejected"]
                )
                st.dataframe(group_df, use_container_width=True, hide_index=True)

                group_dict = {
                    f"Group ID: {row[0]} | {row[1]} ({row[2]}) | Rules: {row[3]}": row
                    for row in group_rows
                }
                selected_group_label = st.selectbox(
                    "Select Group to Modify:",
                    list(group_dict.keys()),
                    key="god_mode_group_selector"
                )
                selected_group_row = group_dict[selected_group_label]
                selected_group_id = selected_group_row[0]
                selected_group_name = selected_group_row[1]

                group_comments = st.text_area(
                    "Group action comments (optional):",
                    key="god_mode_group_comments",
                    placeholder="Reason for force-approving or deleting this group..."
                )

                g_col1, g_col2 = st.columns(2)
                with g_col1:
                    if st.button("✅ Force Approve Whole Group", use_container_width=True, key="god_force_approve_group"):
                        with st.spinner("Approving all rules in group and running master model..."):
                            success, msg = approve_group_rules(
                                selected_group_id,
                                user['user_id'],
                                user['role'],
                                comments=group_comments,
                                force_admin=True
                            )
                            if success:
                                st.success(msg)
                                time.sleep(1.5)
                                st.rerun()
                            else:
                                st.error(msg)

                with g_col2:
                    confirm_delete_group = st.checkbox(
                        f"I understand this permanently deletes group '{selected_group_name}' and all associated rules/assets.",
                        key="confirm_group_delete"
                    )
                    if st.button("🗑️ Delete Group (Admin Only)", type="primary", use_container_width=True, key="god_delete_group"):
                        if not confirm_delete_group:
                            st.error("Please confirm group deletion first.")
                        else:
                            with st.spinner("Deleting group, related rules, SQL assets, and MinIO states..."):
                                success, msg = delete_group_with_assets(selected_group_id, user['role'])
                                if success:
                                    st.success(msg)
                                    time.sleep(1.5)
                                    st.rerun()
                                else:
                                    st.error(msg)
            else:
                st.info("No groups currently exist in the database.")
    
    elif role in ["MANAGER", "TECHNICAL", "SME"] and not st.session_state.get("mgr_show_rule_builder", False):
        st.title(f"✅ {role.capitalize()} Approval Workspace")
        
        # Map role to numeric level
        level_map = {"MANAGER": 1, "TECHNICAL": 2, "SME": 3}
        my_level = level_map[role]

        st.divider()
        st.markdown("### Rule Builder Access")
        st.caption("Enable this to create/update rules. New/updated rules are auto-approved up to your role level.")
        st.checkbox(
            "Open Rule Builder",
            key="mgr_show_rule_builder"
        )

        if role == "MANAGER":
            st.markdown("### Group Approval")
            st.caption("Approve all rules in a group at once and trigger group execution.")

            pending_groups = get_pending_groups_for_level(my_level)
            if pending_groups:
                pending_group_dict = {
                    f"Group ID: {g[0]} | {g[1]} | Pending at L{my_level}: {g[2]}": g
                    for g in pending_groups
                }
                selected_group_label = st.selectbox(
                    "Select Group to Approve in Bulk:",
                    list(pending_group_dict.keys()),
                    key="manager_group_approval_selector"
                )
                selected_group = pending_group_dict[selected_group_label]
                selected_group_id = selected_group[0]

                manager_group_comments = st.text_area(
                    "Group Approval Comments (optional):",
                    key="manager_group_approval_comments",
                    placeholder="Optional audit note for bulk approval"
                )

                if st.button("Approve Whole Group", type="primary", key="manager_bulk_approve_btn"):
                    with st.spinner("Approving all rules in this group and running master model..."):
                        success, msg = approve_group_rules(
                            selected_group_id,
                            user['user_id'],
                            role,
                            comments=manager_group_comments,
                            force_admin=False
                        )
                        if success:
                            st.success(msg)
                            time.sleep(1.5)
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                st.info("No groups are pending at Manager level for bulk approval.")

            st.divider()
            st.markdown("### Rule-by-Rule Approval")
        
        pending_rules = get_rules_for_approval(my_level)
        
        if not pending_rules:
            st.success("🎉 Your queue is empty! No rules pending for your review.")
        else:
            rule_options = {f"Rule ID: {r[1]} | {r[2]} (Status: {r[3]})": r for r in pending_rules}
            selected_rule_label = st.selectbox("Select Rule to Review:", list(rule_options.keys()))
            selected_rule = rule_options[selected_rule_label]
            
            app_id, r_id, r_name, r_action, r_comments = selected_rule
            
            st.markdown(f"### Reviewing Logic: `{r_name}`")
            sql_content = read_dbt_sql(r_name)
            st.code(sql_content, language="sql")
            
            if r_action != "PENDING":
                st.info(f"You have already **{r_action}** this rule.")
                st.text_area("Your previous comments:", value=r_comments, disabled=True)
            else:
                st.markdown("### Action Panel")
                review_comments = st.text_area("Review Comments (Required for Rejection):")
                
                col1, col2 = st.columns([1, 8])
                with col1:
                    if st.button("Approve", type="primary"):
                        success, msg = process_approval(app_id, r_id, r_name, my_level, user['user_id'], 'APPROVED', review_comments)
                        if success:
                            st.success(msg)
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error(msg)
                with col2:
                    if st.button("Reject"):
                        if not review_comments.strip():
                            st.error("You must provide a comment when rejecting a rule.")
                        else:
                            success, msg = process_approval(app_id, r_id, r_name, my_level, user['user_id'], 'REJECTED', review_comments)
                            if success:
                                st.warning(msg)
                                time.sleep(1.5)
                                st.rerun()
        
    elif role == "CREATOR" or (role in ["MANAGER", "TECHNICAL", "SME"] and st.session_state.get("mgr_show_rule_builder", False)):
        # --- MAIN QUERY BUILDER AREA ---
        st.title("Rule Registry: Manage Business Logic")

        if role in ["MANAGER", "TECHNICAL", "SME"]:
            st.caption(f"Signed in as {role}. Rules saved here are auto-approved up to your level.")
            if st.button("Return to Approval Workspace", key="back_to_mgr_workspace"):
                st.session_state.mgr_show_rule_builder = False
                st.rerun()

        clean_name = lambda x: str(x) if x else ""
        
        # Initialize session state for UI toggles
        if "create_new_group" not in st.session_state:
            st.session_state.create_new_group = False
        if "active_group_id" not in st.session_state:
            st.session_state.active_group_id = None
        
        # ---------------------------------------------------------
        # 1. RULE CONTEXT & GROUPING (Matches your Mockup)
        # ---------------------------------------------------------
        with st.container(border=True):
            st.markdown("#### 1. Rule Context & Grouping")
            
            col1, col2 = st.columns([8, 2])
            
            with col1:
                if not st.session_state.create_new_group:
                    # EXISTING GROUP DROPDOWN
                    groups = get_all_groups()
                    if groups:
                        group_dict = {f"{g[1]} ({g[2]})": g for g in groups}
                        selected_group_label = st.selectbox("Select Rule Group:", list(group_dict.keys()))
                        active_group = group_dict[selected_group_label]
                        
                        # Store the active group in state
                        st.session_state.active_group_id = active_group[0]
                        st.session_state.active_group_name = active_group[1]
                        st.session_state.active_group_mode = active_group[2]
                    else:
                        st.info("No groups found. Please create a new group.")
                        st.session_state.active_group_id = None
                else:
                    # NEW GROUP CREATION FORM
                    st.info("🆕 Creating a New Rule Group")
                    new_g_name = st.text_input("New Group Name:", placeholder="e.g., Fraud_Risk_Scoring")
                    new_g_mode = st.radio("Execution Mode:", ["non_sequential", "sequential"], horizontal=True)
                    
                    # --- NEW UI FOR NON-SEQUENTIAL FIELDS ---
                    source_tbl = ""
                    key_col = ""
                    target_col = ""
                    
                    if new_g_mode == 'non_sequential':
                        with st.container(border=True):
                            st.markdown("##### ⚙️ Non-Sequential Configuration")
                            st.caption("Define the base table and how parallel rules should be aggregated.")
                            
                            # 1. Select the base table
                            source_tbl = st.selectbox("Base Source Table:", TABLE_NAMES, key="new_grp_src", format_func=clean_name)
                            
                            # 2. Dynamically load columns for the selected table to pick the Primary Key
                            avail_cols = TABLE_SCHEMAS.get(source_tbl, []) if source_tbl else []
                            key_col = st.selectbox("Grouping Key (e.g., id):", avail_cols, key="new_grp_key", help="The primary key used to GROUP BY and stack the rules.")
                            
                            # 3. Name the output column for the LISTAGG function
                            target_col = st.text_input("Target Column to ListAgg:", placeholder="e.g., triggered_rules", key="new_grp_tgt", help="The name of the new column that will hold the comma-separated values.")
            
            with col2:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                if not st.session_state.create_new_group:
                    if st.button("➕ New Group", use_container_width=True):
                        st.session_state.create_new_group = True
                        st.rerun()
                else:
                    if st.button("💾 Save Group", type="primary", use_container_width=True):
                        if new_g_name:
                            # --- 1. Validation for Non-Sequential Requirements ---
                            if new_g_mode == 'non_sequential' and not (source_tbl and key_col and target_col):
                                st.error("⚠️ Please fill in the Source Table, Grouping Key, and Target Column for non-sequential execution.")
                            else:
                                new_id = get_next_group_id()
                                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                current_user = user['username']
                                
                                # --- 2. Safe SQL Formatting ---
                                # If the mode is sequential, these fields will be empty strings. 
                                # We must format them as NULL to prevent Hive syntax errors.
                                st_val = f"'{source_tbl}'" if source_tbl else "NULL"
                                kc_val = f"'{key_col}'" if key_col else "NULL"
                                tc_val = f"'{target_col}'" if target_col else "NULL"
                                
                                # --- 3. Updated Insert Query ---
                                sql = f"""
                                INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group 
                                (group_id, group_name, execution_mode, created_by, created_at, source_table, key_column, target_column) 
                                VALUES 
                                ({new_id}, '{new_g_name}', '{new_g_mode}', '{current_user}', CAST('{now}' AS TIMESTAMP), {st_val}, {kc_val}, {tc_val})
                                """
                                
                                if run_update(sql):
                                    # Setup the DBT environment
                                    success, msg = setup_new_group_environment(new_g_name)
                                    
                                    if success:
                                        st.success(f"Group created! {msg}")
                                    else:
                                        st.warning(f"Group saved to DB, but env setup failed: {msg}")
                                    
                                    st.session_state.create_new_group = False
                                    time.sleep(1.5)
                                    st.rerun()
                        else:
                            st.error("Group Name required.")

                    if st.button("Cancel", use_container_width=True):
                        st.session_state.create_new_group = False
                        st.rerun()

        # ---------------------------------------------------------
        # 2. RULE DEFINITIONS (Only show if a group is selected)
        # ---------------------------------------------------------
        if st.session_state.active_group_id and not st.session_state.create_new_group:
            with st.container(border=True):
                st.markdown(f"#### 2. Rule Definitions for: {st.session_state.active_group_name}")
                
                action_mode = st.radio("Action:", ["Create New Rule", "Update Existing Rule"], horizontal=True)
                
                if action_mode == "Create New Rule":
                    st.session_state.currently_loaded_rule = None
                    current_id = get_next_rule_id()
                    st.info(f"🆕 Creating Rule ID: **{current_id}**")
                    r_name = st.text_input("Rule Name (Compulsory)", placeholder="e.g., fraud_check_amount")
                    
                else:
                    # UPDATE EXISTING RULE (Filtered by active group)
                    group_rules = get_rules_by_group(st.session_state.active_group_id)
                    if group_rules:
                        rule_dict = {f"ID: {row[0]} - {row[1]}": row[0] for row in group_rules}
                        selected_rule_label = st.selectbox("Select Rule to Update:", list(rule_dict.keys()), key="rule_selector")
                        current_id = rule_dict[selected_rule_label]
                        st.info(f"🔄 Updating Rule ID: **{current_id}**")
                        
                        # Extract just the name for the input field
                        selected_rule_name = [r[1] for r in group_rules if r[0] == current_id][0]
                        r_name = st.text_input("Rule Name", value=selected_rule_name, disabled=True)

                        # Keep sequence input aligned with the selected rule's current visual position.
                        selected_rule_visual_seq = get_rule_visual_position(st.session_state.active_group_id, current_id)
                        
                        # Load state cleanly
                        if st.session_state.get("last_selected_rule") != selected_rule_name:
                            st.session_state.last_selected_rule = selected_rule_name
                            st.session_state.visual_sequence_input = int(selected_rule_visual_seq)
                            if load_rule_state(selected_rule_name, st.session_state.active_group_name):
                                st.rerun()
                    else:
                        st.warning(f"No existing rules found in '{st.session_state.active_group_name}'.")
                        current_id = None
                        r_name = ""

                # --- CONDITIONAL SEQUENCE & MATERIALIZATION INPUT ---
                # This now applies to both Create and Update!
                # --- CONDITIONAL SEQUENCE INPUT ---
                # This now applies to both Create and Update!
                if r_name or action_mode == "Create New Rule":
                    st.divider()
                    
                    if st.session_state.active_group_mode == 'sequential':
                        st.markdown("**Sequential Execution Setup**")

                        default_visual = get_rule_visual_position(st.session_state.active_group_id, current_id) if action_mode == "Update Existing Rule" else (len(get_group_rules_for_sequence(st.session_state.active_group_id)) + 1)
                        visual_sequence_input = st.session_state.get("visual_sequence_input", int(default_visual))

                        seq_left, seq_right = st.columns([1, 1], gap="large")
                        with seq_left:
                            with st.container(border=True):
                                sequence_visual_order = st.number_input(
                                    "Sequence Position",
                                    min_value=1,
                                    step=1,
                                    value=int(visual_sequence_input),
                                    key="visual_sequence_input"
                                )

                        with seq_right:
                            with st.container(border=True):
                                rules_for_map = get_group_rules_for_sequence(
                                    st.session_state.active_group_id
                                )
                                mapped_rules = map_rules_to_visual_sequence(rules_for_map)
                                if mapped_rules:
                                    df_seq = pd.DataFrame([
                                        {
                                            "Sequence Order": row["visual_sequence"],
                                            "Rule Name": row["rule_name"]
                                        }
                                        for row in mapped_rules
                                    ])
                                    st.dataframe(df_seq, hide_index=True, use_container_width=True)
                                else:
                                    st.info("No existing rules in this group yet.")

                        sequence_order = int(sequence_visual_order)

                        # with st.expander("Sequence Logic Validation", expanded=False):
                        #     test_ok = run_sequence_algorithm_tests()
                        #     if test_ok:
                        #         st.success("Sequence calculation test flow passed for Scenario B and Scenario C.")
                        #     else:
                        #         st.error("Sequence calculation test flow failed. Please review sequence helper logic.")
                    else:
                        st.info("ℹ️ Group is non-sequential. All rules run in parallel.")
                        sequence_order = 0
            # ---------------------------------------------------------
        # ---------------------------------------------------------
        # 3 & 4. DATA SOURCES, JOINS & OUTPUT COLUMNS (50-50 Split)
        # ---------------------------------------------------------

        # Restrict rule-model entries in dropdown to the currently selected group.
        all_rules_global = get_existing_rules()
        group_rules_only = get_existing_rules(group_id=st.session_state.active_group_id)

        all_rule_models = set()
        for _, rule_name, _, _ in all_rules_global:
            safe_name = safe_model_name(rule_name)
            all_rule_models.add(safe_name)
            all_rule_models.add(f"final_{safe_name}")

        group_rule_models = set()
        for _, rule_name, _, _ in group_rules_only:
            safe_name = safe_model_name(rule_name)
            group_rule_models.add(safe_name)
            group_rule_models.add(f"final_{safe_name}")

        TABLE_NAMES = [
            t for t in TABLE_SCHEMAS.keys()
            if (t not in all_rule_models) or (t in group_rule_models)
        ]

        if st.session_state.get("t1") and st.session_state["t1"] not in TABLE_NAMES:
            del st.session_state["t1"]
        
        # Keep final_ prefix visible in the UI for approved rule-based models.
        clean_name = lambda x: x
        
        col_src1, col_src2 = st.columns(2)
        
        with col_src1:
            with st.container(border=True):
                st.markdown("#### 3. Data Sources & Joins")
                
                # Check if the widget's key is already populated from the loaded JSON
                if "t1" in st.session_state:
                    t1 = st.selectbox("Select Primary Table:", TABLE_NAMES, key="t1", format_func=clean_name)
                else:
                    t1 = st.selectbox("Select Primary Table:", TABLE_NAMES, index=0, key="t1", format_func=clean_name)
                
                st.markdown("**Joins Configuration**")
                joins_to_remove = []
                available_left_tables = [t1] + [j.get("right_table") for j in st.session_state.joins if j.get("right_table")]

                for i, join_data in enumerate(st.session_state.joins):
                    uid = join_data["id"]
                    st.markdown(f"**Join {i+1}**")
                    
                    t_right_key = f"t_right_{uid}"
                    if t_right_key in st.session_state:
                        join_data["right_table"] = st.selectbox("Table to Join:", TABLE_NAMES, key=t_right_key, format_func=clean_name)
                    else:
                        right_table_idx = safe_index(TABLE_NAMES, join_data.get("right_table"))
                        join_data["right_table"] = st.selectbox("Table to Join:", TABLE_NAMES, index=right_table_idx, key=t_right_key, format_func=clean_name)
                    
                    join_types = ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN"]
                    j_type_key = f"j_type_{uid}"
                    if j_type_key in st.session_state:
                        join_data["join_type"] = st.selectbox("Join Type:", join_types, key=j_type_key)
                    else:
                        join_type_idx = safe_index(join_types, join_data.get("join_type"))
                        join_data["join_type"] = st.selectbox("Join Type:", join_types, index=join_type_idx, key=j_type_key)
                    
                    if join_data["join_type"] != "CROSS JOIN":
                        sub_col1, sub_col2 = st.columns(2)
                        with sub_col1:
                            left_tbl_opts = available_left_tables[:i+1]
                            l_tbl_key = f"l_tbl_{uid}"
                            if l_tbl_key in st.session_state:
                                join_data["left_table"] = st.selectbox("Join to Table:", left_tbl_opts, key=l_tbl_key, format_func=clean_name)
                            else:
                                left_tbl_idx = safe_index(left_tbl_opts, join_data.get("left_table"))
                                join_data["left_table"] = st.selectbox("Join to Table:", left_tbl_opts, index=left_tbl_idx, key=l_tbl_key, format_func=clean_name)
                            
                            l_table_actual = join_data["left_table"] if join_data["left_table"] else t1
                            l_cols = TABLE_SCHEMAS.get(l_table_actual, [])
                            l_col_key = f"l_col_{uid}"
                            if l_col_key in st.session_state:
                                join_data["left_col"] = st.selectbox(f"Column in {clean_name(l_table_actual)}:", l_cols, key=l_col_key)
                            else:
                                left_col_idx = safe_index(l_cols, join_data.get("left_col"))
                                join_data["left_col"] = st.selectbox(f"Column in {clean_name(l_table_actual)}:", l_cols, index=left_col_idx, key=l_col_key)
                        with sub_col2:
                            r_table_actual = join_data["right_table"]
                            r_cols = TABLE_SCHEMAS.get(r_table_actual, [])
                            r_col_key = f"r_col_{uid}"
                            if r_col_key in st.session_state:
                                join_data["right_col"] = st.selectbox(f"Column in {clean_name(r_table_actual)}:", r_cols, key=r_col_key)
                            else:
                                right_col_idx = safe_index(r_cols, join_data.get("right_col"))
                                join_data["right_col"] = st.selectbox(f"Column in {clean_name(r_table_actual)}:", r_cols, index=right_col_idx, key=r_col_key)
                    
                    if st.button("➖ Remove Join", key=f"rm_j_{uid}"):
                        joins_to_remove.append(i)
                    st.divider()
                        
                for index in sorted(joins_to_remove, reverse=True):
                    st.session_state.joins.pop(index)
                    st.rerun()

                if st.button("➕ Add Join"):
                    st.session_state.joins.append({
                        "id": str(uuid.uuid4()), "right_table": "", "join_type": "LEFT JOIN", 
                        "left_table": "", "left_col": "", "right_col": ""
                    })
                    st.rerun()

        with col_src2:
            with st.container(border=True):
                st.markdown("#### 4. Output Columns Selection")
                st.caption("Select the columns you want to pass through to the final rule output.")
                
                primary_cols_key = f"primary_cols_{t1}"
                if primary_cols_key not in st.session_state:
                    st.session_state[primary_cols_key] = TABLE_SCHEMAS.get(t1, [])[:3]
                
                # Apply clean_name to the label
                cols_t1 = st.multiselect(f"Output Columns for {clean_name(t1)}:", TABLE_SCHEMAS.get(t1, []), key=primary_cols_key)
                
                cols_joins = {}
                for join_data in st.session_state.joins:
                    r_table = join_data.get("right_table")
                    if r_table and r_table not in cols_joins:
                        out_col_key = f"out_col_{r_table}"
                        if out_col_key not in st.session_state:
                            st.session_state[out_col_key] = TABLE_SCHEMAS.get(r_table, [])[:3]
                        
                        # Apply clean_name to the label
                        cols_joins[r_table] = st.multiselect(f"Output Columns for {clean_name(r_table)}:", TABLE_SCHEMAS.get(r_table, []), key=out_col_key)

        available_columns = [f"{t1}.{c}" for c in TABLE_SCHEMAS.get(t1, [])]
        for r_table in cols_joins.keys():
            available_columns.extend([f"{r_table}.{c}" for c in TABLE_SCHEMAS.get(r_table, [])])
        available_columns.insert(0, "") 

        selected_output_columns = [f"{t1}.{c}" for c in cols_t1]
        for r_table, cols in cols_joins.items():
            selected_output_columns.extend([f"{r_table}.{c}" for c in cols])

        available_agg_columns = [col for col in available_columns if col not in selected_output_columns]

        with st.container(border=True):
            st.markdown("#### 1.5 Aggregate Functions")
            st.caption("⚠️ Adding aggregations will auto-generate a GROUP BY clause for all non-aggregated columns.")
            
            aggs_to_remove = []
            agg_func_options = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
            
            for i, agg_data in enumerate(st.session_state.aggregations):
                uid = agg_data["id"]
                agg_col1, agg_col2, agg_col3, agg_col4 = st.columns([3, 2, 3, 1])
                
                with agg_col1:
                    agg_col_key = f"agg_col_{uid}"
                    if agg_col_key in st.session_state:
                        agg_data['col'] = st.selectbox("Column", available_agg_columns, key=agg_col_key, label_visibility="collapsed" if i > 0 else "visible")
                    else:
                        col_idx = safe_index(available_agg_columns, agg_data.get('col'))
                        agg_data['col'] = st.selectbox("Column", available_agg_columns, index=col_idx, key=agg_col_key, label_visibility="collapsed" if i > 0 else "visible")
                
                with agg_col2:
                    agg_func_key = f"agg_func_{uid}"
                    if agg_func_key in st.session_state:
                        agg_data['func'] = st.selectbox("Function", agg_func_options, key=agg_func_key, label_visibility="collapsed" if i > 0 else "visible")
                    else:
                        func_idx = safe_index(agg_func_options, agg_data.get('func'))
                        agg_data['func'] = st.selectbox("Function", agg_func_options, index=func_idx, key=agg_func_key, label_visibility="collapsed" if i > 0 else "visible")
                
                with agg_col3:
                    agg_alias_key = f"agg_alias_{uid}"
                    agg_data['alias'] = st.text_input("Alias (AS)", value=agg_data.get('alias', ''), key=agg_alias_key, label_visibility="collapsed" if i > 0 else "visible", placeholder="e.g., total_points")
                
                with agg_col4:
                    if i == 0:
                        st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                    if st.button("🗑️", key=f"agg_del_{uid}"):
                        aggs_to_remove.append(i)
            
            for index in sorted(aggs_to_remove, reverse=True):
                st.session_state.aggregations.pop(index)
                st.rerun()
            
            if st.button("➕ Add Aggregation"):
                st.session_state.aggregations.append({"id": str(uuid.uuid4()), "col": "", "func": "SUM", "alias": ""})
                st.rerun()

        with st.container(border=True):
            st.markdown("#### 1.75 Manual Group BY (Optional)")
            
            # 1. Identify columns already going into the GROUP BY automatically 
            auto_grouped_cols = selected_output_columns
            
            # 2. Remove them from the manual dropdown options (and filter out empty strings)
            manual_gb_options = [c for c in available_columns if c and c not in auto_grouped_cols]
            
            if auto_grouped_cols:
                st.info(f"Automatically grouping by selected columns: {', '.join(auto_grouped_cols)}")
                
            st.multiselect("Select additional columns to Group By:", manual_gb_options, key="manual_group_by")

        with st.container(border=True):
            st.markdown("#### 2. Filtering Rules (WHERE Clause)")
            
            filters_to_remove = []
            for i, filter_data in enumerate(st.session_state.where_filters):
                uid = filter_data["id"]
                row_col1, row_col2, row_col3, row_col4, row_col5 = st.columns([1, 3, 3, 3, 1])
                
                with row_col1:
                    if i > 0:
                        logic_opts = ["AND", "OR"]
                        logic_key = f"logic_{uid}"
                        if logic_key in st.session_state:
                            filter_data['logic'] = st.selectbox("AND/OR", logic_opts, key=logic_key, label_visibility="collapsed")
                        else:
                            logic_idx = safe_index(logic_opts, filter_data.get('logic'))
                            filter_data['logic'] = st.selectbox("AND/OR", logic_opts, index=logic_idx, key=logic_key, label_visibility="collapsed")
                    else:
                        st.write("WHERE")
                with row_col2:
                    w_col_key = f"w_col_{uid}"
                    if w_col_key in st.session_state:
                        filter_data['col'] = st.selectbox("Column", available_columns, key=w_col_key)
                    else:
                        col_idx = safe_index(available_columns, filter_data.get('col'))
                        filter_data['col'] = st.selectbox("Column", available_columns, index=col_idx, key=w_col_key)
                with row_col3:
                    op_options = ["equals (=)", "greater than (>)", "less than (<)", "not equals (!=)", "greater than equal (>=)", "less than equal (<=)", "not equal to (!=)", "LIKE", "NOT LIKE", "IN", "NOT IN", "IS", "IS NOT", "IS NULL", "IS NOT NULL", "BETWEEN"]
                    w_op_key = f"w_op_{uid}"
                    if w_op_key in st.session_state:
                        filter_data['op'] = st.selectbox("Operator", op_options, key=w_op_key)
                    else:
                        op_idx = safe_index(op_options, filter_data.get('op'))
                        filter_data['op'] = st.selectbox("Operator", op_options, index=op_idx, key=w_op_key)
                with row_col4:
                    if filter_data['op'] in ["IS NULL", "IS NOT NULL"]:
                        st.write("") 
                        filter_data['val'] = ""
                        filter_data['val2'] = ""
                    elif filter_data['op'] == "BETWEEN":
                        sub_col1, sub_col2 = st.columns(2)
                        with sub_col1:
                            w_val1_key = f"w_val1_{uid}"
                            # 1. Force the saved string into session state BEFORE drawing
                            if w_val1_key not in st.session_state:
                                st.session_state[w_val1_key] = filter_data.get('val', '')
                            # 2. Draw the widget relying entirely on the state key
                            filter_data['val'] = st.text_input("From", key=w_val1_key)
                        with sub_col2:
                            w_val2_key = f"w_val2_{uid}"
                            if w_val2_key not in st.session_state:
                                st.session_state[w_val2_key] = filter_data.get('val2', '')
                            filter_data['val2'] = st.text_input("To", key=w_val2_key)
                    else:
                        w_val_key = f"w_val_{uid}"
                        if w_val_key not in st.session_state:
                            st.session_state[w_val_key] = filter_data.get('val', '')
                        filter_data['val'] = st.text_input("Value", key=w_val_key)
                        filter_data['val2'] = ""

                with row_col5:
                    st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                    if st.button("🗑️", key=f"w_del_{uid}"):
                        filters_to_remove.append(i)
            
            for index in sorted(filters_to_remove, reverse=True):
                st.session_state.where_filters.pop(index)
                st.rerun()

            if st.button("➕ Add Condition"):
                st.session_state.where_filters.append({"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "","val2": "", "logic": "AND"})
                st.rerun()

        with st.container(border=True):
            st.markdown("#### 2.5 (HAVING Clause)")
            having_to_remove = []
            having_logic_opts = ["AND", "OR"]
            having_func_opts = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
            having_op_opts = ["=", ">", "<", ">=", "<=", "!="]

            for i, having_data in enumerate(st.session_state.having_filters):
                uid = having_data["id"]
                h_col1, h_col2, h_col3, h_col4, h_col5, h_col6 = st.columns([1.2, 2, 3, 1.5, 3, 0.8])

                with h_col1:
                    if i > 0:
                        h_logic_key = f"h_logic_{uid}"
                        if h_logic_key in st.session_state:
                            having_data['logic'] = st.selectbox("AND/OR", having_logic_opts, key=h_logic_key, label_visibility="collapsed")
                        else:
                            h_logic_idx = safe_index(having_logic_opts, having_data.get('logic'))
                            having_data['logic'] = st.selectbox("AND/OR", having_logic_opts, index=h_logic_idx, key=h_logic_key, label_visibility="collapsed")
                    else:
                        st.write("HAVING")

                with h_col2:
                    h_func_key = f"h_func_{uid}"
                    if h_func_key in st.session_state:
                        having_data['func'] = st.selectbox("Function", having_func_opts, key=h_func_key)
                    else:
                        h_func_idx = safe_index(having_func_opts, having_data.get('func'))
                        having_data['func'] = st.selectbox("Function", having_func_opts, index=h_func_idx, key=h_func_key)

                with h_col3:
                    h_col_key = f"h_col_{uid}"
                    if h_col_key in st.session_state:
                        having_data['col'] = st.selectbox("Column", available_columns, key=h_col_key)
                    else:
                        h_col_idx = safe_index(available_columns, having_data.get('col'))
                        having_data['col'] = st.selectbox("Column", available_columns, index=h_col_idx, key=h_col_key)

                with h_col4:
                    h_op_key = f"h_op_{uid}"
                    if h_op_key in st.session_state:
                        having_data['op'] = st.selectbox("Operator", having_op_opts, key=h_op_key)
                    else:
                        h_op_idx = safe_index(having_op_opts, having_data.get('op'))
                        having_data['op'] = st.selectbox("Operator", having_op_opts, index=h_op_idx, key=h_op_key)

                with h_col5:
                    having_data['val'] = st.text_input("Value", value=having_data.get('val', ''), key=f"h_val_{uid}")

                with h_col6:
                    st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                    if st.button("🗑️", key=f"h_del_{uid}"):
                        having_to_remove.append(i)

            for index in sorted(having_to_remove, reverse=True):
                st.session_state.having_filters.pop(index)
                st.rerun()

            if st.button("➕ Add Having Condition"):
                st.session_state.having_filters.append({"id": str(uuid.uuid4()), "logic": "AND", "func": "COUNT", "col": "", "op": ">", "val": ""})
                st.rerun()

        with st.container(border=True):
            st.markdown("#### 3. Sorting (ORDER BY)")
            sorts_to_remove = []
            for i, sort_data in enumerate(st.session_state.order_filters):
                uid = sort_data["id"]
                row_col1, row_col2, row_col3 = st.columns([5, 5, 2])
                with row_col1:
                    o_col_key = f"o_col_{uid}"
                    if o_col_key in st.session_state:
                        sort_data['col'] = st.selectbox("Sort Column", available_columns, key=o_col_key)
                    else:
                        sort_col_idx = safe_index(available_columns, sort_data.get('col'))
                        sort_data['col'] = st.selectbox("Sort Column", available_columns, index=sort_col_idx, key=o_col_key)
                with row_col2:
                    dir_opts = ["ASC", "DESC"]
                    o_dir_key = f"o_dir_{uid}"
                    if o_dir_key in st.session_state:
                        sort_data['dir'] = st.selectbox("Direction", dir_opts, key=o_dir_key)
                    else:
                        dir_idx = safe_index(dir_opts, sort_data.get('dir'))
                        sort_data['dir'] = st.selectbox("Direction", dir_opts, index=dir_idx, key=o_dir_key)
                with row_col3:
                    st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                    if st.button("🗑️", key=f"o_del_{uid}"):
                        sorts_to_remove.append(i)
                        
            for index in sorted(sorts_to_remove, reverse=True):
                st.session_state.order_filters.pop(index)
                st.rerun()

            if st.button("➕ Add Sort Column"):
                st.session_state.order_filters.append({"id": str(uuid.uuid4()), "col": "", "dir": "ASC"})
                st.rerun()

        with st.container(border=True):
            st.markdown("#### 3.5 Row Limit (Optional)")
            st.number_input("Maximum rows to return (leave blank or 0 for no limit)", value=None, min_value=1, step=1, key="row_limit", placeholder="e.g., 100")

        # 4. Preview & Run
        with st.container(border=True):
            st.markdown("#### 4. Preview & Run")

            # --- ALWAYS GENERATE INDIVIDUAL DRAFT SQL FOR REVIEW ---
            macro_tables = [t1] + [j.get("right_table") for j in st.session_state.joins if j.get("right_table")]
            macro_tables = list(dict.fromkeys(macro_tables))

            macro_select_cols = [f"{t1}.{c}" for c in cols_t1]
            for r_table, cols in cols_joins.items():
                macro_select_cols.extend([f"{r_table}.{c}" for c in cols])

            macro_joins = []
            for j in st.session_state.joins:
                if j.get("right_table") and j.get("join_type") != "CROSS JOIN":
                    l_table = j.get("left_table") if j.get("left_table") else t1
                    macro_joins.append({
                        'type': j.get("join_type"), 'table': j.get("right_table"),
                        'left': f"{l_table}.{j.get('left_col')}", 'right': f"{j.get('right_table')}.{j.get('right_col')}"
                    })

            macro_aggs = [{'func': a['func'], 'col': a['col'], 'alias': a.get('alias', '')} for a in st.session_state.aggregations if a.get('col') and a.get('func')]
            
            macro_group_by = []
            if macro_aggs or st.session_state.having_filters or st.session_state.get("manual_group_by"):
                macro_group_by = list(dict.fromkeys(macro_select_cols + st.session_state.get("manual_group_by", [])))

            macro_where = []
            for f in st.session_state.where_filters:
                if f['col']:
                    # --- NEW: Safely extract mathematical operator from parentheses ---
                    raw_op = f.get('op', '')
                    if '(' in raw_op and ')' in raw_op:
                        op_val = raw_op[raw_op.find("(")+1:raw_op.find(")")]
                    else:
                        op_val = raw_op.split(" ")[0] if " " in raw_op and "equals" in raw_op else raw_op
                        
                    if op_val == "equals": op_val = "="
                    
                    val = str(f.get('val', ''))
                    if val and not is_numeric_literal(val) and op_val not in ['IS NULL', 'IS NOT NULL']: 
                        val = f"'{val}'"
                        
                    # --- NEW: Safely extract val2 for BETWEEN clauses ---
                    val2 = str(f.get('val2', ''))
                    if val2 and not is_numeric_literal(val2) and op_val not in ['IS NULL', 'IS NOT NULL']: 
                        val2 = f"'{val2}'"
                        
                    macro_where.append({'col': f['col'], 'op': op_val, 'value': val, 'value2': val2, 'logic': f.get('logic', '')})

            macro_having = []
            for h in st.session_state.having_filters:
                if h.get('col') and h.get('func') and h.get('val'):
                    val = str(h.get('val', ''))
                    if not is_numeric_literal(val): val = f"'{val}'"
                    macro_having.append({'func': h['func'], 'col': h['col'], 'op': h['op'], 'value': val, 'logic': h.get('logic', '')})

            macro_order = [{'col': o['col'], 'dir': o['dir']} for o in st.session_state.order_filters if o.get('col')]
            macro_limit = st.session_state.get("row_limit") or 'None'

            rule_materialization = 'ephemeral' if st.session_state.active_group_mode == 'non_sequential' else 'table'

            generated_sql = f"""{{{{ config(materialized='{rule_materialization}') }}}}

{{{{ rule_engine(
    tables={macro_tables}, joins={macro_joins}, select_columns={macro_select_cols},
    aggregations={macro_aggs}, where_filters={macro_where}, group_by={macro_group_by},
    having={macro_having}, order_by={macro_order}, limit_rows={macro_limit}
) }}}}"""
            
            if st.session_state.active_group_mode == 'sequential':
                st.info("🔄 **Sequential Group.** This draft file is for review purposes. Upon full approval, a unified macro will execute the sequence.")

            live_preview_sql = build_live_rule_preview_sql(
                macro_tables,
                macro_joins,
                macro_select_cols,
                macro_aggs,
                macro_where,
                macro_group_by,
                macro_having,
                macro_order,
                macro_limit,
            )
            st.code(live_preview_sql, language="sql")

            col_btn1, col_btn2 = st.columns([2, 8])
            with col_btn1:
                if st.button("Run Query & Save to DBT", type="primary"):
                    if not r_name:
                        st.error("Please enter a Rule Name before running.")
                    else:
                        # --- NEW: SCHEMA ENFORCEMENT FOR NON-SEQUENTIAL GROUPS ---
                        passed_schema_check = True
                        
                        if st.session_state.active_group_mode == 'non_sequential':
                            # 1. Fetch the required columns for this group
                            grp_sql = f"SELECT key_column, target_column FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group WHERE group_id = {st.session_state.active_group_id}"
                            grp_res = run_query(grp_sql)
                            req_key = grp_res[0][0]
                            req_tgt = grp_res[0][1]
                            
                            # 2. Extract the final column names the user has built in the UI
                            # Strip the 'table.' prefix from selected columns
                            final_output_cols = [c.split('.')[-1] for c in macro_select_cols]
                            # Add any aliases created in aggregations
                            final_output_cols.extend([a['alias'] for a in macro_aggs if a.get('alias')])
                            
                            # 3. Validate
                            missing_cols = []
                            if req_key not in final_output_cols:
                                missing_cols.append(f"`{req_key}` (Grouping Key)")
                            if req_tgt not in final_output_cols:
                                missing_cols.append(f"`{req_tgt}` (Target Column)")
                                
                            if missing_cols:
                                passed_schema_check = False
                                st.error(f"❌ **Schema Mismatch:** This rule belongs to a non-sequential group. Your final output MUST contain: {', '.join(missing_cols)}.")
                                st.info("💡 *Tip: Ensure you selected these columns in Step 4, or use an Aggregation Alias to rename a calculated field to match the Target Column.*")
                        
                        # --- PROCEED ONLY IF SCHEMA CHECK PASSES ---
                        if passed_schema_check:
                            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            current_user = user['username']
                            try:
                                ui_state = {
                                    "primary_table": t1, "primary_columns": cols_t1, "joins": st.session_state.joins,
                                    "cols_joins": cols_joins, "aggregations": st.session_state.aggregations,
                                    "having_filters": st.session_state.having_filters, "manual_group_by": st.session_state.manual_group_by,
                                    "row_limit": st.session_state.row_limit, "where_filters": st.session_state.where_filters,
                                    "order_filters": st.session_state.order_filters, "sequence_order": sequence_order
                                }
                                
                                save_ui_state(r_name, st.session_state.active_group_name, ui_state)
                                st.session_state.schema_cache_key = st.session_state.get("schema_cache_key", 0) + 1
                                
                                # ALWAYS save individual draft file
                                dbt_filepath = save_dbt_model(r_name, st.session_state.active_group_name, generated_sql)

                                computed_sequence_order = sequence_order
                                sequence_plan = None

                                if st.session_state.active_group_mode == 'sequential':
                                    base_rules = get_group_rules_for_sequence(
                                        st.session_state.active_group_id,
                                        exclude_rule_id=current_id if action_mode == "Update Existing Rule" else None
                                    )
                                    # Use temporary rule id for planning in create mode; replaced after insert.
                                    planning_rule_id = current_id if action_mode == "Update Existing Rule" else -1
                                    sequence_plan = calculate_sequence_plan(
                                        base_rules,
                                        sequence_order,
                                        {"rule_id": planning_rule_id, "rule_name": r_name}
                                    )
                                    computed_sequence_order = sequence_plan["new_sequence_order"]

                                if action_mode == "Create New Rule":
                                    final_insert_id = get_next_rule_id() 
                                    db_query = f"INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow (rule_id, rule_name, status, created_by, created_at, updated_at, sequence_order, group_id) VALUES ({final_insert_id}, '{r_name}', 'IN_REVIEW', '{current_user}', CAST('{now}' AS TIMESTAMP), CAST('{now}' AS TIMESTAMP), {computed_sequence_order}, {st.session_state.active_group_id})"
                                else:
                                    final_insert_id = current_id
                                    db_query = f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'IN_REVIEW', updated_at = CAST('{now}' AS TIMESTAMP), sequence_order = {computed_sequence_order}, group_id = {st.session_state.active_group_id} WHERE rule_id = {final_insert_id}"
                                
                                with st.spinner("Saving workflow to database..."):
                                    if run_update(db_query):
                                        # Backend sequence handlers for A/B/C scenarios.
                                        if st.session_state.active_group_mode == 'sequential' and sequence_plan:
                                            if sequence_plan["mode"] == "single":
                                                update_rule_sequence(final_insert_id, sequence_plan["new_sequence_order"])
                                            else:
                                                batch_payload = []
                                                for row in sequence_plan["batch_updates"]:
                                                    row_rule_id = final_insert_id if row["rule_id"] == -1 else row["rule_id"]
                                                    batch_payload.append({
                                                        "rule_id": row_rule_id,
                                                        "rule_name": row["rule_name"],
                                                        "sequence_order": row["sequence_order"],
                                                        "visual_sequence": row["visual_sequence"]
                                                    })
                                                update_rule_sequence_batch(batch_payload)

                                            actor_role = str(user.get('role', '')).upper()
                                            actor_level = get_role_approval_level(actor_role)
                                            actor_user_id = user.get('user_id')

                                            for level in [1, 2, 3]:
                                                if level <= actor_level:
                                                    step_action = 'APPROVED'
                                                    step_comment = f"Auto-approved at Level {level} by {actor_role} during rule save."
                                                    step_user_id = actor_user_id
                                                elif level == actor_level + 1:
                                                    step_action = 'PENDING'
                                                    step_comment = f"Awaiting Level {level} review."
                                                    step_user_id = None
                                                else:
                                                    step_action = 'WAITING'
                                                    step_comment = 'Waiting for prior level approval.'
                                                    step_user_id = None

                                                upsert_rule_approval_step(
                                                    final_insert_id,
                                                    level,
                                                    step_action,
                                                    step_comment,
                                                    step_user_id,
                                                    now
                                                )

                                            if actor_level >= 3:
                                                run_update(
                                                    f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow "
                                                    f"SET status = 'ACTIVE', updated_at = CAST('{now}' AS TIMESTAMP) "
                                                    f"WHERE rule_id = {final_insert_id}"
                                                )

                                                can_execute_group = True
                                                if st.session_state.active_group_mode != 'non_sequential':
                                                    success_copy, copy_msg = create_final_rule_file(r_name)
                                                    if not success_copy:
                                                        st.error(f"❌ Rule approved to ACTIVE, but final file copy failed: {copy_msg}")
                                                        st.info(f"📁 Draft script written to: `{dbt_filepath}`")
                                                        can_execute_group = False

                                                if can_execute_group and check_group_fully_approved(st.session_state.active_group_id):
                                                    success_exec, exec_msg = execute_dbt_group(st.session_state.active_group_name)
                                                    if success_exec:
                                                        st.success(
                                                            f"✅ Rule '{r_name}' is ACTIVE. Group fully approved and master model executed."
                                                        )
                                                    elif can_execute_group:
                                                        st.error(
                                                            f"⚠️ Rule is ACTIVE and group is approved, but execution failed: {exec_msg}"
                                                        )
                                                elif can_execute_group:
                                                    st.success(
                                                        f"✅ Rule '{r_name}' is ACTIVE. Waiting for remaining rules in this group before group execution."
                                                    )
                                            elif actor_level == 0:
                                                if action_mode == "Create New Rule":
                                                    st.success(f"✅ Rule '{r_name}' successfully routed to the Manager!")
                                                else:
                                                    st.success(f"✅ Rule '{r_name}' updated with the same Rule ID and re-routed for review.")
                                            else:
                                                next_level = actor_level + 1
                                                if action_mode == "Create New Rule":
                                                    st.success(
                                                        f"✅ Rule '{r_name}' saved. Auto-approved through Level {actor_level}; awaiting Level {next_level}."
                                                    )
                                                else:
                                                    st.success(
                                                        f"✅ Rule '{r_name}' updated. Auto-approved through Level {actor_level}; awaiting Level {next_level}."
                                                    )

                                        st.info(f"📁 Draft script written to: `{dbt_filepath}`")
                                    else:
                                        st.warning(f"📁 Database operation failed.")
                            except Exception as e:
                                st.error(f"❌ Failed to process rule: {e}")
                            
            with col_btn2:
                if st.button("Clear All"):
                    _clear_dynamic_widget_keys()
                    st.session_state.joins = []
                    st.session_state.aggregations = []
                    st.session_state.having_filters = []
                    st.session_state.manual_group_by = []
                    if 'row_limit' in st.session_state:
                        del st.session_state['row_limit']
                    st.session_state.where_filters = [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "logic": "AND"}]
                    st.session_state.order_filters = [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}]
                    st.session_state.loaded_primary_table = None
                    st.session_state.loaded_primary_columns = None
                    st.session_state.loaded_cols_joins = None
                    st.session_state.last_selected_rule = None
                    if 't1' in st.session_state:
                        del st.session_state['t1']
                    st.rerun()