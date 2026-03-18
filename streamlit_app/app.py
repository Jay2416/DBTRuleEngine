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

@st.cache_data(ttl=300) # Caches results for 5 minutes
def get_iceberg_schemas():
    s3_client = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
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

def run_update(query):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
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

def get_existing_rules():
    """Fetches existing ACTIVE rules from the database to use as data sources."""
    # Added WHERE status = 'ACTIVE' so only approved rules can be selected as sources
    sql = f"SELECT rule_id, rule_name FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE status = 'ACTIVE' ORDER BY rule_id ASC"
    
    result = run_query(sql)
    return result if result else []

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
    
    s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
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
    
    s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
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
                
                if exec_mode == 'non_sequential':
                    success_exec, exec_msg = execute_dbt_rule(rule_name)
                    return (True, f"✅ Rule ACTIVE and executed! {exec_msg}") if success_exec else (False, f"⚠️ File created, but execution failed: {exec_msg}")
                else:
                    if check_group_fully_approved(g_id):
                        generate_group_sql(g_id, g_name)
                        success_exec, exec_msg = execute_dbt_group(g_name)
                        return (True, f"✅ Group Fully Approved! Master CTE compiled and executed! {exec_msg}") if success_exec else (False, f"⚠️ Master compiled, but execution failed: {exec_msg}")
                    else:
                        return True, "✅ Rule ACTIVE. Waiting for other rules in the group to be approved before executing the Master sequence."
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
    final_rules_str = f"\n      final_{safe_group}:\n        +schema: final_{safe_group}\n"
    
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
            
        # Inject under final_rules
        if line.strip() == "final_rules:":
            in_final_rules = True
        elif in_final_rules and line.strip() == "+schema: final_rules":
            new_lines.append(final_rules_str)
            in_final_rules = False
            
    # Write the updated lines back to the file
    with open(yml_path, 'w') as f:
        f.writelines(new_lines)
        
    return True, "Folders and YAML updated successfully."

def generate_group_sql(group_id, group_name):
    """
    Universally fetches all rules for ANY sequential group, builds the steps array, 
    and generates a single hierarchical SQL file using the sequential macro.
    """
    # 1. Fetch rules in the correct order (1000, 2000, 3000)
    rules = get_rules_by_group(group_id)
    if not rules:
        return False, "No rules found in group."
        
    group_steps = []

    # Build canonical CTE aliases and resolution map once for the entire group.
    rule_cte_map = {r[1]: safe_model_name(r[1]) for r in rules}
    relation_map = {}
    for rule_name, cte_name in rule_cte_map.items():
        safe_name = safe_model_name(rule_name)
        relation_map[rule_name] = cte_name
        relation_map[safe_name] = cte_name
        relation_map[f"final_{safe_name}"] = cte_name
    
    # 2. Loop through rules and format the logic for the universal macro
    for row in rules:
        rule_id, rule_name, seq_order = row
        state = load_ui_state(rule_name, group_name)
        
        if not state:
            continue
            
        t1_raw = state.get("primary_table")
        t1, is_primary_cte = resolve_group_relation_name(t1_raw, relation_map)
        if not t1:
            continue
        
        # Use resolved table token (either in-group CTE or external model).
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

        # Format Output Columns
        select_cols = [f"{t1}.{c}" for c in state.get("primary_columns", [])]
        for r_table, cols in state.get("cols_joins", {}).items():
            resolved_r_table, _ = resolve_group_relation_name(r_table, relation_map)
            if not resolved_r_table:
                continue
            select_cols.extend([f"{resolved_r_table}.{c}" for c in cols])

        # Format Group By
        macro_group_by = []
        aggs = state.get("aggregations", [])
        manual_gb = [rewrite_column_reference(c, relation_map) for c in state.get("manual_group_by", [])]
        having = state.get("having_filters", [])
        if aggs or having or manual_gb:
            macro_group_by = list(dict.fromkeys(select_cols + manual_gb))

        # Format Where Filters
        macro_where = []
        for f in state.get("where_filters", []):
            if f['col']:
                op_val = f['op'].split(" ")[0] if " " in f['op'] and "equals" in f['op'] else f['op']
                if op_val == "equals": op_val = "="
                val = str(f.get('val', ''))
                if val and not is_numeric_literal(val) and op_val not in ['IS NULL', 'IS NOT NULL']:
                    val = f"'{val}'"
                macro_where.append({
                    'col': rewrite_column_reference(f['col'], relation_map),
                    'op': op_val,
                    'value': val,
                    'logic': f.get('logic', '')
                })

        macro_having = []
        for h in having:
            if h.get('col') and h.get('func') and h.get('val'):
                h_val = str(h.get('val', ''))
                if not is_numeric_literal(h_val):
                    h_val = f"'{h_val}'"
                macro_having.append({
                    'func': h['func'],
                    'col': rewrite_column_reference(h['col'], relation_map),
                    'op': h.get('op', '>'),
                    'value': h_val,
                    'logic': h.get('logic', '')
                })

        macro_order_by = []
        for o in state.get("order_filters", []):
            if o.get('col'):
                macro_order_by.append({
                    'col': rewrite_column_reference(o['col'], relation_map),
                    'dir': o['dir']
                })

        # Build the final step dictionary for this specific rule
        # Build the final step dictionary for this specific rule
        step_dict = {
            "rule_name": rule_cte_map.get(rule_name, safe_model_name(rule_name)),
            "sequence_order": seq_order,
            "primary_table": from_table,
            "is_primary_cte": is_primary_cte, # Pass the flag
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

    # 3. Build the final Jinja string wrapping the universal macro
    safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
    
    generated_sql = f"""{{{{ config(materialized='table') }}}}

{{{{ sequential_rule_engine(
    steps={group_steps}
) }}}}"""

    # --- FIXED: Save to the final_rules folder instead of draft ---
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
    
    command = f'docker exec dbt bash -c "cd formula1 && dbt run --select {dbt_target}"'
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return True, f"✅ Group Executed Successfully!\n\nLogs:\n{result.stdout}"
        else:
            error_output = result.stderr if result.stderr else result.stdout
            return False, f"Group execution failed. Logs:\n{error_output}"
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
    saved_rules_list = get_existing_rules()
    if saved_rules_list:
        for r in saved_rules_list:
            rule_nm = r[1]
            # Format the name exactly how dbt will save it in the final_rules folder
            target_model_name = f"final_{re.sub(r'[^a-zA-Z0-9_]', '_', rule_nm.lower())}"
            
            # If Iceberg didn't find it (because it's ephemeral), build its schema from JSON!
            if target_model_name not in TABLE_SCHEMAS:
                
                # --- FIXED: Fetch the group name dynamically before loading UI state ---
                rule_group_name = get_group_name_for_rule(rule_nm)
                state = load_ui_state(rule_nm, rule_group_name)
                # ---------------------------------------------------------------------
                
                if state:
                    # Reconstruct the expected output columns
                    simulated_cols = state.get("primary_columns", [])
                    for j_cols in state.get("cols_joins", {}).values():
                        simulated_cols.extend(j_cols)
                    for agg in state.get("aggregations", []):
                        if agg.get("alias"):
                            simulated_cols.append(agg["alias"])
                    
                    if simulated_cols:
                        # Add it to our schemas dictionary so Streamlit can use it!
                        TABLE_SCHEMAS[target_model_name] = list(set(simulated_cols))

    TABLE_NAMES = list(TABLE_SCHEMAS.keys())
    if not TABLE_NAMES:
        st.error("No tables available to build query.")
        st.stop()

    # Initialize dynamic session states for UI forms
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
            st.session_state.logged_in = False
            st.session_state.user_info = None
            st.rerun()

    role = user["role"]

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

                                    if exec_mode == 'non_sequential':
                                        success_exec, exec_msg = execute_dbt_rule(sel_rule_name)
                                        if success_exec: st.success(f"✅ Rule forced to ACTIVE and executed!")
                                        else: st.error(f"⚠️ DBT execution failed:\n{exec_msg}")
                                    else:
                                        if check_group_fully_approved(g_id):
                                            generate_group_sql(g_id, g_name)
                                            success_exec, exec_msg = execute_dbt_group(g_name)
                                            if success_exec: st.success("✅ Sequence Complete! Master group file generated and executed.")
                                            else: st.error(f"⚠️ Group compiled but failed to run:\n{exec_msg}")
                                        else:
                                            st.success("✅ Rule forced to ACTIVE. Execution paused until all rules in group are approved.")
                                else:
                                    st.error(f"❌ Forced to ACTIVE, but file issue: {copy_msg}")
                                
                            time.sleep(3.5)
                            st.rerun()
                            
                with col2:
                    if st.button("❌ Force Reject", use_container_width=True):
                        if run_update(f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'REJECTED' WHERE rule_id = {sel_rule_id}"):
                            st.warning(f"Rule ID {sel_rule_id} forced to REJECTED.")
                            time.sleep(1)
                            st.rerun()
                            
                with col3:
                    if st.button("🗑️ Delete Rule", type="primary", use_container_width=True):
                        # 1. Delete associated approval records first (Foreign Key constraint logic)
                        run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow WHERE rule_id = {sel_rule_id}")
                        
                        # 2. Delete the rule from the main table
                        if run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE rule_id = {sel_rule_id}"):
                            
                            # 3. Physically delete the generated files
                            group_name = get_group_name_for_rule(sel_rule_name)
                            safe_group = re.sub(r'[^a-zA-Z0-9_]', '_', group_name.lower())
                            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', sel_rule_name.lower())
                            
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
                                s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
                                s3_client.delete_object(Bucket=MINIO_BUCKET, Key=f"ui_states/{safe_group}/{safe_name}.json")
                            except Exception as e:
                                st.warning(f"Note: Could not delete UI state from MinIO: {e}")
                                
                            st.error(f"Rule ID {sel_rule_id} and all associated files (Draft & Final) have been permanently deleted.")
                            time.sleep(1.5)
                            st.rerun()
            else:
                st.info("No rules currently exist in the database.")
    
    elif role in ["MANAGER", "TECHNICAL", "SME"]:
        st.title(f"✅ {role.capitalize()} Approval Workspace")
        
        # Map role to numeric level
        level_map = {"MANAGER": 1, "TECHNICAL": 2, "SME": 3}
        my_level = level_map[role]
        
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
        
    elif role == "CREATOR":
        # --- MAIN QUERY BUILDER AREA ---
        st.title("Rule Registry: Manage Business Logic")
        
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
            
            with col2:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                if not st.session_state.create_new_group:
                    if st.button("➕ New Group", use_container_width=True):
                        st.session_state.create_new_group = True
                        st.rerun()
                else:
                    if st.button("💾 Save Group", type="primary", use_container_width=True):
                        if new_g_name:
                            new_id = get_next_group_id()
                            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # Grab the username from the active session
                            current_user = user['username']
                            
                            # UPDATED SQL: Removed 'status', added 'created_by' to match your schema
                            sql = f"""
                            INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group 
                            (group_id, group_name, execution_mode, created_by, created_at) 
                            VALUES 
                            ({new_id}, '{new_g_name}', '{new_g_mode}', '{current_user}', CAST('{now}' AS TIMESTAMP))
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
                        
                        # Load state cleanly
                        if st.session_state.get("last_selected_rule") != selected_rule_name:
                            st.session_state.last_selected_rule = selected_rule_name
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
                        loaded_seq = st.session_state.get("sequence_order", 1)
                        sequence_order = st.number_input("Sequence Order (e.g., 1, 2, 3)", min_value=1, step=1, value=int(loaded_seq))
                    else:
                        st.info("ℹ️ Group is non-sequential. All rules run in parallel.")
                        sequence_order = 0
            # ---------------------------------------------------------
        # ---------------------------------------------------------
        # 3 & 4. DATA SOURCES, JOINS & OUTPUT COLUMNS (50-50 Split)
        # ---------------------------------------------------------
        
        # --- NEW: Helper to visually hide 'final_' from the UI ---
        clean_name = lambda x: x.replace("final_", "") if isinstance(x, str) and x.startswith("final_") else x
        
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
                    op_val = f['op'].split(" ")[0] if " " in f['op'] and "equals" in f['op'] else f['op']
                    if op_val == "equals": op_val = "="
                    val = str(f.get('val', ''))
                    if val and not is_numeric_literal(val) and op_val not in ['IS NULL', 'IS NOT NULL']: val = f"'{val}'"
                    macro_where.append({'col': f['col'], 'op': op_val, 'value': val, 'logic': f.get('logic', '')})

            macro_having = []
            for h in st.session_state.having_filters:
                if h.get('col') and h.get('func') and h.get('val'):
                    val = str(h.get('val', ''))
                    if not is_numeric_literal(val): val = f"'{val}'"
                    macro_having.append({'func': h['func'], 'col': h['col'], 'op': h['op'], 'value': val, 'logic': h.get('logic', '')})

            macro_order = [{'col': o['col'], 'dir': o['dir']} for o in st.session_state.order_filters if o.get('col')]
            macro_limit = st.session_state.get("row_limit") or 'None'

            generated_sql = f"""{{{{ config(materialized='table') }}}}

{{{{ rule_engine(
    tables={macro_tables}, joins={macro_joins}, select_columns={macro_select_cols},
    aggregations={macro_aggs}, where_filters={macro_where}, group_by={macro_group_by},
    having={macro_having}, order_by={macro_order}, limit_rows={macro_limit}
) }}}}"""
            
            if st.session_state.active_group_mode == 'sequential':
                st.info("🔄 **Sequential Group.** This draft file is for review purposes. Upon full approval, a unified macro will execute the sequence.")
            st.code(generated_sql, language="sql")

            col_btn1, col_btn2 = st.columns([2, 8])
            with col_btn1:
                if st.button("Run Query & Save to DBT", type="primary"):
                    if not r_name:
                        st.error("Please enter a Rule Name before running.")
                    else:
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
                            
                            # ALWAYS save individual draft file
                            dbt_filepath = save_dbt_model(r_name, st.session_state.active_group_name, generated_sql)

                            if action_mode == "Create New Rule":
                                final_insert_id = get_next_rule_id() 
                                db_query = f"INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow (rule_id, rule_name, status, created_by, created_at, updated_at, sequence_order, group_id) VALUES ({final_insert_id}, '{r_name}', 'IN_REVIEW', '{current_user}', CAST('{now}' AS TIMESTAMP), CAST('{now}' AS TIMESTAMP), {sequence_order}, {st.session_state.active_group_id})"
                            else:
                                final_insert_id = current_id
                                db_query = f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow SET status = 'IN_REVIEW', updated_at = CAST('{now}' AS TIMESTAMP), sequence_order = {sequence_order}, group_id = {st.session_state.active_group_id} WHERE rule_id = {final_insert_id}"
                            
                            with st.spinner("Saving workflow to database..."):
                                if run_update(db_query):
                                    manager_id = get_manager_user_id()
                                    manager_id_sql = f"'{manager_id}'" if manager_id else "NULL"
                                    approval_id = get_next_approval_id()
                                    run_update(f"INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow (approval_id, rule_id, user_id, level_id, action, comments, action_date) VALUES ({approval_id}, {final_insert_id}, {manager_id_sql}, 1, 'PENDING', 'Rule submitted and awaiting Level 1 (Manager) review.', CAST('{now}' AS TIMESTAMP))")
                                    
                                    st.success(f"✅ Rule '{r_name}' successfully routed to the Manager!")
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