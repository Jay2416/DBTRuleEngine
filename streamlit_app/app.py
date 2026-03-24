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

from login import render_auth_page
from create_edit import render_create_edit_workspace
from admin_ui import render_admin_workspace
from approver_ui import render_approver_workspace

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
    default_role = "CREATOR"

    if check_user_exists(uname):
        return False, "⚠️ Username already exists."
    if check_user_exists(email):
        return False, "⚠️ Email already exists."

    user_id = str(uuid.uuid4())
    hashed_pw = hash_password(password)
    created_at_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
        '{default_role}',
        CAST('{created_at_str}' AS TIMESTAMP),
        NULL
    )
    """

    if run_update(sql):
        return True, "✅ Registration successful! Please log in."
    return False, "❌ Registration failed due to database error."


def verify_user(identifier, password):
    hashed_pw = hash_password(password)
    sql = f"""
    SELECT user_id, firstname, lastname, username, role
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
    WHERE (username = '{identifier}' OR email = '{identifier}')
      AND password_hash = '{hashed_pw}'
    """
    result = run_query(sql)

    if result and len(result) > 0:
        row = result[0]
        user_data = {"user_id": row[0], "firstname": row[1], "lastname": row[2], "username": row[3], "role": row[4]}

        login_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_sql = f"""
        UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
        SET last_login = CAST('{login_time}' AS TIMESTAMP)
        WHERE username = '{user_data['username']}'
        """
        run_update(update_sql)
        return user_data

    return None


def verify_user_for_reset(email, mobile):
    sql = f"""
    SELECT username FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
    WHERE email = '{email}' AND mobile = '{mobile}'
    """
    result = run_query(sql)
    return True if result and len(result) > 0 else False


def reset_password(email, new_password):
    new_hash = hash_password(new_password)
    sql = f"""
    UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE}
    SET password_hash = '{new_hash}'
    WHERE email = '{email}'
    """
    return run_update(sql)


def safe_index(options, value, default=0):
    try:
        if value is not None and value in options:
            return options.index(value)
    except (ValueError, AttributeError):
        pass
    return default


def is_numeric_literal(value):
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
    if not isinstance(value, str):
        return value
    text = value.strip()
    match = re.match(r"^\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}$", text)
    return match.group(1) if match else value


def safe_model_name(value):
    if value is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(value).lower())


def force_ephemeral_materialization(sql_text):
    if not isinstance(sql_text, str):
        return sql_text

    updated, replaced = re.subn(
        r"materialized\s*=\s*['\"][^'\"]+['\"]",
        "materialized='ephemeral'",
        sql_text,
        count=1,
        flags=re.IGNORECASE
    )
    if replaced > 0:
        return updated

    updated, injected = re.subn(
        r"\{\{\s*config\s*\(",
        "{{ config(materialized='ephemeral', ",
        sql_text,
        count=1,
        flags=re.IGNORECASE
    )
    if injected > 0:
        return updated

    return "{{ config(materialized='ephemeral') }}\n\n" + sql_text


def ensure_non_sequential_rules_ephemeral(group_id, group_name):
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

        with open(rule_path, 'r') as f:
            original = f.read()
        updated = force_ephemeral_materialization(original)
        if updated != original:
            with open(rule_path, 'w') as f:
                f.write(updated)

    if missing_files:
        return False, f"Missing rule SQL file(s): {', '.join(missing_files)}"

    return True, "All non-sequential rules are set to ephemeral."


def build_live_rule_preview_sql(macro_tables, macro_joins, macro_select_cols, macro_aggs, macro_where, macro_group_by, macro_having, macro_order, macro_limit):
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
    lines.append("  " + ",\n  ".join(select_items) if select_items else "  *")

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
        expr = f"{col} {op}" if op in ['IS NULL', 'IS NOT NULL'] else (f"{col} BETWEEN {val} AND {val2}" if op == 'BETWEEN' else f"{col} {op} {val}")
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
    if not isinstance(value, str):
        return value
    text = value.strip()
    if "." not in text:
        return text
    table_part, column_part = text.split(".", 1)
    resolved_table, _ = resolve_group_relation_name(table_part, relation_map)
    return f"{resolved_table}.{column_part}" if resolved_table else text


def _clear_dynamic_widget_keys():
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


def get_next_rule_id():
    sql = f"SELECT MAX(rule_id) FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow"
    result = run_query(sql)
    if result and result[0][0] is not None:
        return result[0][0] + 1
    return 1


def get_existing_rules(group_id=None):
    base_sql = f"SELECT rule_id, rule_name, status FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow"
    sql = f"{base_sql} WHERE group_id = {int(group_id)} ORDER BY rule_id ASC" if group_id is not None else f"{base_sql} ORDER BY rule_id ASC"
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
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    dbt_dir = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group)
    os.makedirs(dbt_dir, exist_ok=True)
    filepath = os.path.join(dbt_dir, f"{safe_name}.sql")
    with open(filepath, "w") as f:
        f.write(sql_content)
    return filepath


def save_ui_state(rule_name, group_name, state_dict):
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
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    s3_key = f"ui_states/{safe_group}/{safe_name}.json"
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=s3_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        st.error(f"Failed to load UI state from MinIO: {e}")
        return None


def load_rule_state(rule_name, group_name):
    state = load_ui_state(rule_name, group_name)
    if not state:
        return False

    _clear_dynamic_widget_keys()

    joins = state.get("joins", [])
    where_filters = state.get("where_filters", [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "logic": "AND"}])
    order_filters = state.get("order_filters", [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}])
    having_filters = state.get("having_filters", [])
    manual_group_by = state.get("manual_group_by", state.get("group_cols", []))
    row_limit = state.get("row_limit", "")

    for item in joins + where_filters + order_filters + having_filters:
        if not item.get('id'):
            item['id'] = str(uuid.uuid4())

    st.session_state.joins = joins
    st.session_state.where_filters = where_filters
    st.session_state.order_filters = order_filters
    st.session_state.having_filters = having_filters
    st.session_state.row_limit = None if row_limit in (None, "", 0, "0") else row_limit

    primary_table = state.get("primary_table")
    if primary_table and primary_table in TABLE_NAMES:
        st.session_state.t1 = primary_table

    primary_cols = state.get("primary_columns", state.get("cols_t1", []))
    valid_primary_cols = [c for c in primary_cols if c in TABLE_SCHEMAS.get(primary_table or "", [])]
    st.session_state[f"primary_cols_{primary_table}"] = valid_primary_cols

    st.session_state.manual_group_by = manual_group_by
    st.session_state.loaded_primary_table = primary_table
    st.session_state.loaded_primary_columns = primary_cols
    st.session_state.loaded_cols_joins = state.get("cols_joins", {})
    st.session_state.aggregations = state.get("aggregations", [])
    st.session_state.sequence_order = state.get("sequence_order", 1)
    st.session_state.rule_remark_text = state.get("rule_remark", "")
    return True


def get_next_approval_id():
    sql = f"SELECT MAX(approval_id) FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow"
    result = run_query(sql)
    if result and result[0][0] is not None:
        return result[0][0] + 1
    return 1


def get_manager_user_id():
    sql = f"SELECT user_id FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} WHERE role = 'MANAGER' LIMIT 1"
    result = run_query(sql)
    if result and len(result) > 0:
        return result[0][0]
    return None


def get_role_approval_level(role_name):
    level_map = {"CREATOR": 0, "MANAGER": 1, "TECHNICAL": 2, "SME": 3}
    return level_map.get(str(role_name or "").upper(), 0)


def get_rules_for_approval(level_id):
    sql = f"""
    SELECT a.approval_id, a.rule_id, r.rule_name, a.action, a.comments
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow a
    JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r ON a.rule_id = r.rule_id
    WHERE a.level_id = {level_id}
    ORDER BY a.action_date DESC
    """
    return run_query(sql)


def get_pending_groups_for_level(level_id):
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
    sql = f"""
    SELECT rule_id, rule_name, status
    FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow
    WHERE group_id = {int(group_id)}
    ORDER BY rule_id ASC
    """
    result = run_query(sql)
    return result if result else []


def upsert_rule_approval_step(rule_id, level_id, action, comments, user_id, action_ts):
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
        if exec_mode != 'non_sequential' and not os.path.exists(final_path):
            success_copy, copy_msg = create_final_rule_file(rule_name)
            if not success_copy:
                return False, f"File finalization failed for '{rule_name}': {copy_msg}"

        if not run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'ACTIVE', updated_at = CAST('{now}' AS TIMESTAMP) WHERE rule_id = {int(rule_id)}"):
            return False, f"Failed to mark rule ACTIVE: {rule_name}"

        for level in (1, 2, 3):
            if not upsert_rule_approval_step(rule_id, level, 'APPROVED', comment_text, approver_user_id, now):
                return False, f"Failed workflow upsert for rule {rule_name} at level {level}"

    success_exec, exec_msg = execute_dbt_group(group_name)
    if not success_exec:
        return False, f"Rules were approved, but group execution failed: {exec_msg}"
    return True, f"Approved {len(rules)} rule(s) in group '{group_name}' and executed the master group model."


def delete_minio_prefix(prefix):
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

    if action == 'APPROVED':
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

        run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'ACTIVE' WHERE rule_id = {rule_id}")
        grp_sql = f"SELECT g.group_id, g.group_name, g.execution_mode FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id WHERE r.rule_id = {rule_id}"
        grp_res = run_query(grp_sql)
        if not grp_res:
            return False, "Rule is ACTIVE, but group metadata lookup failed."

        g_id, g_name, exec_mode = grp_res[0]

        if str(exec_mode).lower() != 'non_sequential':
            success_copy, copy_msg = create_final_rule_file(rule_name)
            if not success_copy:
                return False, f"❌ Rule ACTIVE, but file copy failed: {copy_msg}"

        if check_group_fully_approved(g_id):
            success_gen, gen_msg = generate_group_sql(g_id, g_name)
            if not success_gen:
                return False, f"⚠️ Group approved, but SQL generation failed: {gen_msg}"

            success_exec, exec_msg = execute_dbt_group(g_name)
            return (True, f"✅ Group Fully Approved! Master group compiled and executed! {exec_msg}") if success_exec else (False, f"⚠️ Master compiled, but execution failed: {exec_msg}")

        return True, "✅ Rule ACTIVE. Waiting for other rules in the group to be approved before executing the Master group file."

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


@st.cache_data(ttl=300)
def get_rule_group_columns():
    """Return available columns in rule_group to support backward-compatible reads/writes."""
    rows = run_query(f"DESCRIBE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group")
    if not rows:
        return []
    cols = []
    for row in rows:
        col_name = str(row[0]).strip() if row and row[0] is not None else ""
        if not col_name or col_name.startswith("#"):
            continue
        cols.append(col_name)
    return cols


def get_rule_group_non_seq_config(group_id):
    """Fetch execution mode plus key/remark metadata from rule_group."""
    cols = set(get_rule_group_columns())
    if not cols:
        return None

    remark_col = "remark" if "remark" in cols else "target_column"
    select_cols = ["execution_mode", "key_column"]
    if remark_col in cols:
        select_cols.append(remark_col)

    sql = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group "
        f"WHERE group_id = {int(group_id)}"
    )
    rows = run_query(sql)
    if not rows:
        return None

    row = rows[0]
    exec_mode = row[0] if len(row) > 0 else None
    key_col = row[1] if len(row) > 1 else None
    remark_val = row[2] if len(row) > 2 else "remark"

    return {
        "execution_mode": exec_mode,
        "key_column": key_col,
        "remark_column": remark_val or "remark",
        "remark_field": remark_col,
    }

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
        
    # 2. Fetch group details including non-sequential fields
    grp_cfg = get_rule_group_non_seq_config(group_id)
    if not grp_cfg:
        return False, "Group configuration not found."

    exec_mode = grp_cfg["execution_mode"]
    key_column = grp_cfg["key_column"]
    remark_column = grp_cfg["remark_column"]

    def normalize_column_token(col_name):
        """Strip qualifiers/quotes so macro receives a raw output column name."""
        if not isinstance(col_name, str):
            return ""
        token = col_name.strip().strip('`').strip('"')
        if "." in token:
            token = token.split(".")[-1]
        return token

    key_column = normalize_column_token(key_column)
    remark_column = normalize_column_token(remark_column)
    if not key_column or not remark_column:
        return False, "Group key/remark columns are required for non-sequential execution."
    
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
    remark_column='{remark_column}'
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

            macro_having = []
            for h in state.get("having_filters", []):
                if h.get('col') and h.get('func') and h.get('val'):
                    val = str(h.get('val', ''))
                    if not is_numeric_literal(val):
                        val = f"'{val}'"
                    macro_having.append({
                        'func': h['func'],
                        'col': rewrite_column_reference(h['col'], relation_map),
                        'op': h['op'],
                        'value': val,
                        'logic': h.get('logic', '')
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
    render_auth_page(
        register_user,
        verify_user,
        verify_user_for_reset,
        reset_password,
        is_valid_password,
        is_valid_phone,
    )

# --- LOGGED IN DASHBOARD (NEW UI) ---
else:
    user = st.session_state.user_info

    # --- SIDEBAR NAV ---
    with st.sidebar:
        st.markdown("### 🔀 DBT Rule Engine")
        st.caption("Query Builder Environment")
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
        render_admin_workspace(
            user=user,
            ctx={
                "AUTH_CATALOG": AUTH_CATALOG,
                "AUTH_SCHEMA": AUTH_SCHEMA,
                "AUTH_TABLE": AUTH_TABLE,
                "run_query": run_query,
                "run_update": run_update,
                "check_user_exists": check_user_exists,
                "hash_password": hash_password,
                "get_next_approval_id": get_next_approval_id,
                "create_final_rule_file": create_final_rule_file,
                "check_group_fully_approved": check_group_fully_approved,
                "execute_dbt_group": execute_dbt_group,
                "get_group_name_for_rule": get_group_name_for_rule,
                "get_s3_client": get_s3_client,
                "MINIO_BUCKET": MINIO_BUCKET,
                "get_group_overview_rows": get_group_overview_rows,
                "approve_group_rules": approve_group_rules,
                "delete_group_with_assets": delete_group_with_assets,
            },
        )
    
    elif role in ["MANAGER", "TECHNICAL", "SME"] and not st.session_state.get("mgr_show_rule_builder", False):
        render_approver_workspace(
            role=role,
            user=user,
            ctx={
                "get_pending_groups_for_level": get_pending_groups_for_level,
                "approve_group_rules": approve_group_rules,
                "get_rules_for_approval": get_rules_for_approval,
                "read_dbt_sql": read_dbt_sql,
                "process_approval": process_approval,
            },
        )
        
    elif role == "CREATOR" or (role in ["MANAGER", "TECHNICAL", "SME"] and st.session_state.get("mgr_show_rule_builder", False)):
        render_create_edit_workspace(
            role=role,
            user=user,
            table_schemas=TABLE_SCHEMAS,
            table_names=TABLE_NAMES,
            ctx={
                "AUTH_CATALOG": AUTH_CATALOG,
                "AUTH_SCHEMA": AUTH_SCHEMA,
                "run_query": run_query,
                "run_update": run_update,
                "get_all_groups": get_all_groups,
                "get_next_group_id": get_next_group_id,
                "setup_new_group_environment": setup_new_group_environment,
                "get_next_rule_id": get_next_rule_id,
                "get_rules_by_group": get_rules_by_group,
                "get_rule_visual_position": get_rule_visual_position,
                "load_rule_state": load_rule_state,
                "get_group_rules_for_sequence": get_group_rules_for_sequence,
                "map_rules_to_visual_sequence": map_rules_to_visual_sequence,
                "get_existing_rules": get_existing_rules,
                "safe_model_name": safe_model_name,
                "safe_index": safe_index,
                "is_numeric_literal": is_numeric_literal,
                "build_live_rule_preview_sql": build_live_rule_preview_sql,
                "save_ui_state": save_ui_state,
                "save_dbt_model": save_dbt_model,
                "calculate_sequence_plan": calculate_sequence_plan,
                "update_rule_sequence": update_rule_sequence,
                "update_rule_sequence_batch": update_rule_sequence_batch,
                "get_role_approval_level": get_role_approval_level,
                "upsert_rule_approval_step": upsert_rule_approval_step,
                "create_final_rule_file": create_final_rule_file,
                "check_group_fully_approved": check_group_fully_approved,
                "execute_dbt_group": execute_dbt_group,
                "_clear_dynamic_widget_keys": _clear_dynamic_widget_keys,
                "get_rule_group_columns": get_rule_group_columns,
                "get_rule_group_non_seq_config": get_rule_group_non_seq_config,
            },
        )
        st.stop()