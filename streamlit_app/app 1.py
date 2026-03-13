import streamlit as st
import pandas as pd
from pyhive import hive
import uuid
import datetime
import re
import time
import boto3
import json
import os

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
    prefix = "iceberg_data/f1_staging/"
    
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
    """Fetches existing rules from the database."""
    sql = f"SELECT rule_id, rule_name FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow ORDER BY rule_id ASC"
    result = run_query(sql)
    return result if result else []

def save_dbt_model(rule_name, sql_content, ui_state):
    """Saves the SQL query and a JSON state file directly to the shared volume."""
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    dbt_dir = os.path.join("/", "dbt", "formula1", "models", "rules")
    os.makedirs(dbt_dir, exist_ok=True)
    
    # 1. Save the .sql file
    sql_filepath = os.path.join(dbt_dir, f"{safe_name}.sql")
    with open(sql_filepath, "w") as f:
        f.write(sql_content)
        
    # 2. Save the .json state file
    json_filepath = os.path.join(dbt_dir, f"{safe_name}.json")
    with open(json_filepath, "w") as f:
        json.dump(ui_state, f, indent=4)
        
    return sql_filepath

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

def load_rule_state(rule_name):
    """Reads the JSON file and pre-populates ALL widget keys in session state."""
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rule_name.lower())
    json_filepath = os.path.join("/", "dbt", "formula1", "models", "rules", f"{safe_name}.json")
    
    if os.path.exists(json_filepath):
        with open(json_filepath, "r") as f:
            state = json.load(f)
        
        # Step 1: Clear ALL old dynamic widget keys
        _clear_dynamic_widget_keys()
        
        # Step 2: Load structural data state
        joins = state.get("joins", [])
        where_filters = state.get("where_filters", [])
        order_filters = state.get("order_filters", [])
        having_filters = state.get("having_filters", [])
        manual_group_by = state.get("manual_group_by", [])
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
        
        # Step 4: Set primary columns multiselect key
        primary_cols = state.get("primary_columns", [])
        valid_primary_cols = [c for c in primary_cols if c in TABLE_SCHEMAS.get(primary_table or "", [])]
        st.session_state[f"primary_cols_{primary_table}"] = valid_primary_cols
        
        # Step 5: Set join widget keys directly
        join_types_list = ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN"]
        for join_data in joins:
            uid = join_data.get("id")
            if not uid:
                continue
            # Right table
            rt = join_data.get("right_table", "")
            if rt and rt in TABLE_NAMES:
                st.session_state[f"t_right_{uid}"] = rt
            # Join type
            jt = join_data.get("join_type", "")
            if jt in join_types_list:
                st.session_state[f"j_type_{uid}"] = jt
            # Left table
            lt = join_data.get("left_table", "")
            if lt and lt in TABLE_NAMES:
                st.session_state[f"l_tbl_{uid}"] = lt
            # Left column
            lc = join_data.get("left_col", "")
            lt_for_cols = lt if lt else (primary_table or "")
            if lc and lc in TABLE_SCHEMAS.get(lt_for_cols, []):
                st.session_state[f"l_col_{uid}"] = lc
            # Right column
            rc = join_data.get("right_col", "")
            if rc and rc in TABLE_SCHEMAS.get(rt, []):
                st.session_state[f"r_col_{uid}"] = rc
        
        # Step 6: Set join output column multiselect keys
        cols_joins_data = state.get("cols_joins", {})
        for table, cols in cols_joins_data.items():
            valid_cols = [c for c in cols if c in TABLE_SCHEMAS.get(table, [])]
            st.session_state[f"out_col_{table}"] = valid_cols
        
        # Step 7: Set WHERE filter widget keys
        op_options_list = ["equals (=)", "greater than (>)", "less than (<)", "not equals (!=)",
                          "greater than equal (>=)", "less than equal (<=)", "not equal to (!=)",
                          "LIKE", "NOT LIKE", "IN", "NOT IN", "IS", "IS NOT", "IS NULL", "IS NOT NULL", "BETWEEN"]
        logic_opts = ["AND", "OR"]
        
        # Pre-calculate available_columns for validation
        loaded_available_columns = [f"{primary_table}.{c}" for c in TABLE_SCHEMAS.get(primary_table or "", [])]
        for jd in joins:
            rt = jd.get("right_table")
            if rt:
                loaded_available_columns.extend([f"{rt}.{c}" for c in TABLE_SCHEMAS.get(rt, [])])
        loaded_available_columns.insert(0, "")
        st.session_state.manual_group_by = [c for c in manual_group_by if c in loaded_available_columns]
        
        for fd in where_filters:
            uid = fd.get("id")
            if not uid:
                continue
            # Operator
            if fd.get("op") in op_options_list:
                st.session_state[f"w_op_{uid}"] = fd["op"]
            # Logic
            if fd.get("logic") in logic_opts:
                st.session_state[f"logic_{uid}"] = fd["logic"]
            # Column - NOW we set it directly if it's valid
            col_val = fd.get("col", "")
            if col_val in loaded_available_columns:
                st.session_state[f"w_col_{uid}"] = col_val
            # Value text inputs
            if fd.get('val'):
                st.session_state[f"w_val_{uid}"] = fd["val"]
                st.session_state[f"w_val1_{uid}"] = fd["val"]
            if fd.get('val2'):
                st.session_state[f"w_val2_{uid}"] = fd["val2"]
        
        # Step 8: Set ORDER BY widget keys
        dir_opts = ["ASC", "DESC"]
        for sd in order_filters:
            uid = sd.get("id")
            if not uid:
                continue
            # Direction
            if sd.get("dir") in dir_opts:
                st.session_state[f"o_dir_{uid}"] = sd["dir"]
            # Column - NOW we set it directly if it's valid
            col_val = sd.get("col", "")
            if col_val in loaded_available_columns:
                st.session_state[f"o_col_{uid}"] = col_val

        # Step 8.5: Set HAVING filter widget keys
        having_logic_opts = ["AND", "OR"]
        having_func_opts = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
        having_op_opts = ["=", ">", "<", ">=", "<=", "!="]
        for hd in having_filters:
            uid = hd.get("id")
            if not uid:
                continue
            if hd.get("logic") in having_logic_opts:
                st.session_state[f"h_logic_{uid}"] = hd["logic"]
            if hd.get("func") in having_func_opts:
                st.session_state[f"h_func_{uid}"] = hd["func"]
            col_val = hd.get("col", "")
            if col_val in loaded_available_columns:
                st.session_state[f"h_col_{uid}"] = col_val
            if hd.get("op") in having_op_opts:
                st.session_state[f"h_op_{uid}"] = hd["op"]
            if hd.get("val"):
                st.session_state[f"h_val_{uid}"] = hd["val"]
        
        # Step 9: Keep loaded state for reference
        st.session_state.loaded_primary_table = primary_table
        st.session_state.loaded_primary_columns = primary_cols
        st.session_state.loaded_cols_joins = cols_joins_data
        
        # Step 10: Load aggregations
        aggregations = state.get("aggregations", [])
        # Ensure each aggregation has an 'id'
        for agg in aggregations:
            if not agg.get('id'):
                agg['id'] = str(uuid.uuid4())
        st.session_state.aggregations = aggregations
        
        # Set aggregation widget keys
        agg_func_options = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
        for agg_data in aggregations:
            uid = agg_data.get("id")
            if not uid:
                continue
            # Column
            col_val = agg_data.get("col", "")
            if col_val in loaded_available_columns:
                st.session_state[f"agg_col_{uid}"] = col_val
            # Function
            func_val = agg_data.get("func", "")
            if func_val in agg_func_options:
                st.session_state[f"agg_func_{uid}"] = func_val
            # Alias
            if agg_data.get("alias"):
                st.session_state[f"agg_alias_{uid}"] = agg_data["alias"]
        
        return True
    return False


# ------------------------------------------------------------------------------
# 5. MAIN APPLICATION UI
# ------------------------------------------------------------------------------

# --- MOCK USER SETUP ---
# Creates a fake user so the sidebar and database inserts don't crash
user = {
    "firstname": "Test",
    "lastname": "User",
    "username": "test_user",
    "role": "CREATOR"
}

# Fetch DB F1 Schemas dynamically from MinIO Iceberg Metadata
try:
    TABLE_SCHEMAS = get_iceberg_schemas()
    if not TABLE_SCHEMAS:
        st.warning("No schemas found in MinIO. Please check your connection and paths.")
except Exception as e:
    st.error(f"Failed to fetch schemas from MinIO: {e}")
    TABLE_SCHEMAS = {}
    
TABLE_NAMES = list(TABLE_SCHEMAS.keys())

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

# --- MAIN QUERY BUILDER AREA ---
st.title("Build Your Rule Data Flow")

# 1. Source & Selection
with st.container(border=True):
    st.markdown("#### 1. Source & Selection")
    
    action_mode = st.radio("Action:", ["Create New Rule", "Update Existing Rule"], horizontal=True)
    
    if action_mode == "Create New Rule":
        current_id = get_next_rule_id()
        st.info(f"🆕 Creating Rule ID: **{current_id}**")
        r_name = st.text_input("Rule Name (Compulsory)", placeholder="e.g., Driver Standings 2023 Analysis")
    else:
        existing_rules = get_existing_rules()
        if existing_rules:
            # Create a dictionary mapping rule_name -> rule_id
            rule_dict = {row[1]: row[0] for row in existing_rules}
            selected_rule = st.selectbox("Select Rule to Update:", list(rule_dict.keys()), key="rule_selector")
            current_id = rule_dict[selected_rule]
            st.info(f"🔄 Updating Rule ID: **{current_id}**")
            r_name = st.text_input("Rule Name", value=selected_rule, disabled=True)

            # Load state only when the selected rule changes
            if st.session_state.get("last_selected_rule") != selected_rule:
                st.session_state.last_selected_rule = selected_rule
                if load_rule_state(selected_rule):
                    st.rerun()
        else:
            st.warning("No existing rules found.")
            current_id = None
            r_name = ""
    
    col1, col2 = st.columns([1, 1])
    with col1:
        # Determine the index of the loaded table (if it exists) to set it as default
        default_t1_idx = 0
        loaded_t1 = st.session_state.get("loaded_primary_table")
        if loaded_t1 and loaded_t1 in TABLE_NAMES:
            default_t1_idx = TABLE_NAMES.index(loaded_t1)
            
        # Primary Table Dropdown
        t1 = st.selectbox("Select Primary Table:", TABLE_NAMES, index=default_t1_idx, key="t1")
        
        # --- MULTIPLE JOINS LOGIC ---
        st.markdown("**Joins Configuration**")
        joins_to_remove = []
        
        # Keep track of tables already in the query to use as "Left Table" options
        available_left_tables = [t1] + [j.get("right_table") for j in st.session_state.joins if j.get("right_table")]

        for i, join_data in enumerate(st.session_state.joins):
            uid = join_data["id"]
            st.markdown(f"**Join {i+1}**")
            
            # 1. Select the table to join and the type
            # Only calculate index if the key is NOT already in session_state
            t_right_key = f"t_right_{uid}"
            if t_right_key in st.session_state:
                join_data["right_table"] = st.selectbox("Table to Join:", TABLE_NAMES, key=t_right_key)
            else:
                right_table_idx = safe_index(TABLE_NAMES, join_data.get("right_table"))
                join_data["right_table"] = st.selectbox("Table to Join:", TABLE_NAMES, index=right_table_idx, key=t_right_key)
            
            join_types = ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN"]
            j_type_key = f"j_type_{uid}"
            if j_type_key in st.session_state:
                join_data["join_type"] = st.selectbox("Join Type:", join_types, key=j_type_key)
            else:
                join_type_idx = safe_index(join_types, join_data.get("join_type"))
                join_data["join_type"] = st.selectbox("Join Type:", join_types, index=join_type_idx, key=j_type_key)
            
            # 2. Select the ON condition columns (if not CROSS JOIN)
            if join_data["join_type"] != "CROSS JOIN":
                sub_col1, sub_col2 = st.columns(2)
                with sub_col1:
                    left_tbl_opts = available_left_tables[:i+1]
                    l_tbl_key = f"l_tbl_{uid}"
                    if l_tbl_key in st.session_state:
                        join_data["left_table"] = st.selectbox("Join to Table:", left_tbl_opts, key=l_tbl_key)
                    else:
                        left_tbl_idx = safe_index(left_tbl_opts, join_data.get("left_table"))
                        join_data["left_table"] = st.selectbox("Join to Table:", left_tbl_opts, index=left_tbl_idx, key=l_tbl_key)
                    
                    l_table_actual = join_data["left_table"] if join_data["left_table"] else t1
                    l_cols = TABLE_SCHEMAS.get(l_table_actual, [])
                    l_col_key = f"l_col_{uid}"
                    if l_col_key in st.session_state:
                        join_data["left_col"] = st.selectbox(f"Column in {l_table_actual}:", l_cols, key=l_col_key)
                    else:
                        left_col_idx = safe_index(l_cols, join_data.get("left_col"))
                        join_data["left_col"] = st.selectbox(f"Column in {l_table_actual}:", l_cols, index=left_col_idx, key=l_col_key)
                with sub_col2:
                    r_table_actual = join_data["right_table"]
                    r_cols = TABLE_SCHEMAS.get(r_table_actual, [])
                    r_col_key = f"r_col_{uid}"
                    if r_col_key in st.session_state:
                        join_data["right_col"] = st.selectbox(f"Column in {r_table_actual}:", r_cols, key=r_col_key)
                    else:
                        right_col_idx = safe_index(r_cols, join_data.get("right_col"))
                        join_data["right_col"] = st.selectbox(f"Column in {r_table_actual}:", r_cols, index=right_col_idx, key=r_col_key)
            
            if st.button("➖ Remove Join", key=f"rm_j_{uid}"):
                joins_to_remove.append(i)
            st.divider()
                
        # Process removals
        for index in sorted(joins_to_remove, reverse=True):
            st.session_state.joins.pop(index)
            st.rerun()

        if st.button("➕ Add Join"):
            st.session_state.joins.append({
                "id": str(uuid.uuid4()), "right_table": "", "join_type": "LEFT JOIN", 
                "left_table": "", "left_col": "", "right_col": ""
            })
            st.rerun()

    with col2:
            # --- DYNAMIC OUTPUT COLUMNS ---
            primary_cols_key = f"primary_cols_{t1}"
            if primary_cols_key not in st.session_state:
                st.session_state[primary_cols_key] = TABLE_SCHEMAS.get(t1, [])[:3]
            
            cols_t1 = st.multiselect(f"Output Columns for {t1}:", TABLE_SCHEMAS.get(t1, []), key=primary_cols_key)
            
            cols_joins = {}
            
            for join_data in st.session_state.joins:
                r_table = join_data.get("right_table")
                if r_table and r_table not in cols_joins:
                    out_col_key = f"out_col_{r_table}"
                    if out_col_key not in st.session_state:
                        st.session_state[out_col_key] = TABLE_SCHEMAS.get(r_table, [])[:3]
                    
                    cols_joins[r_table] = st.multiselect(f"Output Columns for {r_table}:", TABLE_SCHEMAS.get(r_table, []), key=out_col_key)

# Pre-calculate available columns for WHERE and ORDER BY
available_columns = [f"{t1}.{c}" for c in TABLE_SCHEMAS.get(t1, [])]
for r_table in cols_joins.keys():
    available_columns.extend([f"{r_table}.{c}" for c in TABLE_SCHEMAS.get(r_table, [])])
available_columns.insert(0, "") # Add empty default 

# Build selected output-column strings and derive aggregation-eligible columns
selected_output_columns = [f"{t1}.{c}" for c in cols_t1]
for r_table, cols in cols_joins.items():
    selected_output_columns.extend([f"{r_table}.{c}" for c in cols])

available_agg_columns = [col for col in available_columns if col not in selected_output_columns]

# 1.5 Aggregate Functions (NEW SECTION)
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
    
    # Remove flagged aggregations
    for index in sorted(aggs_to_remove, reverse=True):
        st.session_state.aggregations.pop(index)
        st.rerun()
    
    if st.button("➕ Add Aggregation"):
        st.session_state.aggregations.append({
            "id": str(uuid.uuid4()),
            "col": "",
            "func": "SUM",
            "alias": ""
        })
        st.rerun()

# 1.75 Manual Group BY (Optional)
with st.container(border=True):
    st.markdown("#### 1.75 Manual Group BY (Optional)")

    has_active_aggregations = any(agg.get('col') and agg.get('func') for agg in st.session_state.aggregations)
    has_active_having = any(h.get('col') and h.get('func') for h in st.session_state.having_filters)

    if has_active_aggregations or has_active_having:
        st.info("Group By is automatically managed because aggregations or HAVING filters are active.")
    else:
        st.multiselect(
            "Select columns to Group By:",
            available_columns,
            key="manual_group_by"
        )

# 2. Filtering Rules (WHERE Clause)
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
            # 1. Hide ALL boxes for IS NULL / IS NOT NULL
            if filter_data['op'] in ["IS NULL", "IS NOT NULL"]:
                st.write("") # Keeps the layout perfectly aligned
                filter_data['val'] = ""
                filter_data['val2'] = ""
                
            # 2. Show TWO boxes ("From" and "To") ONLY for BETWEEN
            elif filter_data['op'] == "BETWEEN":
                sub_col1, sub_col2 = st.columns(2)
                with sub_col1:
                    filter_data['val'] = st.text_input("From", value=filter_data.get('val', ''), key=f"w_val1_{uid}")
                with sub_col2:
                    filter_data['val2'] = st.text_input("To", value=filter_data.get('val2', ''), key=f"w_val2_{uid}")
                    
            # 3. Show ONE box ("Value") for everything else
            else:
                filter_data['val'] = st.text_input("Value", value=filter_data.get('val', ''), key=f"w_val_{uid}")
                filter_data['val2'] = "" # Safety wipe just in case they switched away from BETWEEN

        with row_col5:
            # Add margin to align button with inputs
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            if st.button("🗑️", key=f"w_del_{uid}"):
                filters_to_remove.append(i)
    
    # Remove flagged filters
    for index in sorted(filters_to_remove, reverse=True):
        st.session_state.where_filters.pop(index)
        st.rerun()

    if st.button("➕ Add Condition"):
        st.session_state.where_filters.append({"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "","val2": "", "logic": "AND"})
        st.rerun()

# 2.5 (HAVING Clause)
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
        st.session_state.having_filters.append({
            "id": str(uuid.uuid4()),
            "logic": "AND",
            "func": "COUNT",
            "col": "",
            "op": ">",
            "val": ""
        })
        st.rerun()

# 3. Sorting (ORDER BY)
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

# 3.5 Row Limit (Optional)
with st.container(border=True):
    st.markdown("#### 3.5 Row Limit (Optional)")
    st.number_input(
        "Maximum rows to return (leave blank or 0 for no limit)",
        value=None,
        min_value=1,
        step=1,
        key="row_limit",
        placeholder="e.g., 100"
    )

# 4. Preview & Run
with st.container(border=True):
    st.markdown("#### 4. Preview & Run")
    
    # Build SELECT clause
    # --- NEW DYNAMIC CTE BUILDER ---
    # Get all unique tables used in the query
    all_tables_in_query = [t1] + [j.get("right_table") for j in st.session_state.joins if j.get("right_table")]
    unique_tables = list(dict.fromkeys(all_tables_in_query)) 
    
    cte_clauses = [f"{tbl} AS (\n    SELECT * FROM {{{{ ref('{tbl}') }}}}\n)" for tbl in unique_tables]
    cte_block = "WITH " + ",\n".join(cte_clauses) if cte_clauses else ""
    
    # --- DYNAMIC SELECT CLAUSE (WITH AGGREGATIONS) ---
    # Collect non-aggregated columns
    non_agg_cols = [f"{t1}.{c}" for c in cols_t1]
    for r_table, cols in cols_joins.items():
        non_agg_cols.extend([f"{r_table}.{c}" for c in cols])
    
    # Collect aggregation expressions
    agg_expressions = []
    has_valid_aggregations = False
    for agg in st.session_state.aggregations:
        if agg.get('col') and agg.get('func'):
            has_valid_aggregations = True
            agg_expr = f"{agg['func']}({agg['col']})"
            if agg.get('alias'):
                agg_expr += f" AS {agg['alias']}"
            agg_expressions.append(agg_expr)
    
    # Combine columns: non-aggregated + aggregated
    all_select_cols = non_agg_cols + agg_expressions
    select_clause = "SELECT\n    " + (",\n    ".join(all_select_cols) if all_select_cols else "*")
    
    # --- DYNAMIC FROM & JOIN CLAUSES ---
    from_clause = f"FROM {t1}"
    
    for join_data in st.session_state.joins:
        r_table = join_data.get("right_table")
        if not r_table: 
            continue
            
        j_type = join_data["join_type"]
        
        if j_type == "CROSS JOIN":
            from_clause += f"\nCROSS JOIN {r_table}"
        else:
            l_table = join_data["left_table"] if join_data["left_table"] else t1
            l_col = join_data["left_col"]
            r_col = join_data["right_col"]
            from_clause += f"\n{j_type} {r_table}\n  ON {l_table}.{l_col} = {r_table}.{r_col}"

    # Build WHERE clause
    where_clauses = []
    for i, f in enumerate(st.session_state.where_filters):
        if f['col']:
            logic = f"{f['logic']} " if i > 0 else ""
            
            op_map = {
                "equals (=)": "=", "greater than (>)": ">", "less than (<)": "<", 
                "not equals (!=)": "!=", "greater than equal (>=)": ">=", 
                "less than equal (<=)": "<=", "LIKE": "LIKE", "NOT LIKE": "NOT LIKE", 
                "IN": "IN", "NOT IN": "NOT IN", "BETWEEN": "BETWEEN",
                "IS": "IS", "IS NOT": "IS NOT", "IS NULL": "IS NULL", "IS NOT NULL": "IS NOT NULL"
            }
            op_sql = op_map.get(f['op'], "=")
            
            # Handle IS NULL / IS NOT NULL (No values needed)
            if f['op'] in ["IS NULL", "IS NOT NULL"]:
                where_clauses.append(f"{logic}{f['col']} {op_sql}")
                
            # Handle BETWEEN (Needs val and val2)
            elif f['op'] == "BETWEEN" and f.get('val') and f.get('val2'):
                v1 = f['val'] if is_numeric_literal(f['val']) else f"'{f['val']}'"
                v2 = f['val2'] if is_numeric_literal(f['val2']) else f"'{f['val2']}'"
                where_clauses.append(f"{logic}{f['col']} BETWEEN {v1} AND {v2}")
                
            # Handle normal operators (Requires val)
            elif f.get('val') and f['op'] not in ["BETWEEN", "IS NULL", "IS NOT NULL"]:
                # Bypass quotes for IN, NOT IN, and special IS values
                is_unquoted = f['op'] in ["IN", "NOT IN", "IS", "IS NOT"] or f['val'].upper() in ("NULL", "TRUE", "FALSE")
                val = f['val'] if is_unquoted or is_numeric_literal(f['val']) else f"'{f['val']}'"
                
                where_clauses.append(f"{logic}{f['col']} {op_sql} {val}")

    where_clause = ("\nWHERE " + "\n  ".join(where_clauses)) if where_clauses else ""

    # --- BUILD GROUP BY CLAUSE (ONLY IF AGGREGATIONS EXIST) ---
    having_clauses = []
    for i, h in enumerate(st.session_state.having_filters):
        if h.get('func') and h.get('col') and h.get('op') and h.get('val'):
            logic = f"{h.get('logic', 'AND')} " if i > 0 else ""
            h_value = str(h['val']).strip()
            h_sql_val = h_value if is_numeric_literal(h_value) else f"'{h_value}'"
            having_clauses.append(f"{logic}{h['func']}({h['col']}) {h['op']} {h_sql_val}")

    having_clause = ("\nHAVING " + "\n  ".join(having_clauses)) if having_clauses else ""

    group_by_clause = ""
    if (has_valid_aggregations or having_clauses) and non_agg_cols:
        group_by_clause = "\nGROUP BY " + ", ".join(non_agg_cols)
    elif not has_valid_aggregations and not having_clauses and st.session_state.manual_group_by:
        group_by_clause = "\nGROUP BY " + ", ".join(st.session_state.manual_group_by)

    # Build ORDER BY clause
    order_clauses = []
    for f in st.session_state.order_filters:
        if f['col']:
            order_clauses.append(f"{f['col']} {f['dir']}")

    order_clause = ("\nORDER BY " + ", ".join(order_clauses)) if order_clauses else ""

    # Build LIMIT clause
    limit_clause = ""
    row_limit_value = st.session_state.get("row_limit", None)
    if isinstance(row_limit_value, (int, float)) and int(row_limit_value) > 0:
        limit_clause = f"\nLIMIT {int(row_limit_value)}"

    # Combine SQL (HAVING inserted after GROUP BY and before ORDER BY)
    generated_sql = f"{cte_block}\n\n{select_clause}\n{from_clause}{where_clause}{group_by_clause}{having_clause}{order_clause}{limit_clause}"
    
    st.code(generated_sql, language="sql")

    # Action Buttons
    col_btn1, col_btn2 = st.columns([2, 8])
    # Action Buttons
    col_btn1, col_btn2 = st.columns([2, 8])
    with col_btn1:
        if st.button("Run Query & Save to DBT", type="primary"):
            if not r_name:
                st.error("Please enter a Rule Name before running.")
            else:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                current_user = user['username']
                safe_sql = generated_sql.replace("'", "''")
                
                try:
                    # 1. Bundle up the current UI state
                    ui_state = {
                        "primary_table": t1,
                        "primary_columns": cols_t1,
                        "joins": st.session_state.joins,
                        "cols_joins": cols_joins,
                        "aggregations": st.session_state.aggregations,
                        "having_filters": st.session_state.having_filters,
                        "manual_group_by": st.session_state.manual_group_by,
                        "row_limit": st.session_state.row_limit,
                        "where_filters": st.session_state.where_filters,
                        "order_filters": st.session_state.order_filters
                    }
                    
                    # 2. Write the DBT Script (.sql file) AND JSON file locally
                    dbt_filepath = save_dbt_model(r_name, generated_sql, ui_state)
                    
                    # 3. Database Entry
                    if action_mode == "Create New Rule":
                        final_insert_id = get_next_rule_id() 
                        
                        db_query = f"""
                        INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow
                        (rule_id, rule_name, rule_sql, created_by, created_at, updated_at)
                        VALUES (
                            {final_insert_id}, '{r_name}', '{safe_sql}', '{current_user}', 
                            CAST('{now}' AS TIMESTAMP), CAST('{now}' AS TIMESTAMP)
                        )
                        """
                    else:
                        final_insert_id = current_id # Use the locked ID from the dropdown
                        db_query = f"""
                        UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow
                        SET rule_sql = '{safe_sql}',
                            updated_at = CAST('{now}' AS TIMESTAMP)
                        WHERE rule_id = {final_insert_id}
                        """
                    
                    with st.spinner("Saving workflow to database..."):
                        if run_update(db_query):
                            st.success(f"✅ Rule '{r_name}' (ID: {final_insert_id}) successfully {'submitted' if action_mode == 'Create New Rule' else 'updated'}!")
                            st.info(f"📁 DBT script written to: `{dbt_filepath}`")
                        else:
                            st.warning(f"📁 DBT script written to `{dbt_filepath}`, but Database operation failed.")
                            
                except Exception as e:
                    st.error(f"❌ Failed to process rule: {e}")
                    
    with col_btn2:
        if st.button("Clear All"):
            # Clear all dynamic widget keys first
            _clear_dynamic_widget_keys()
            # Reset data states
            st.session_state.joins = []
            st.session_state.aggregations = []
            st.session_state.having_filters = []
            st.session_state.manual_group_by = []
            st.session_state.row_limit = None
            st.session_state.where_filters = [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "logic": "AND"}]
            st.session_state.order_filters = [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}]
            st.session_state.loaded_primary_table = None
            st.session_state.loaded_primary_columns = None
            st.session_state.loaded_cols_joins = None
            st.session_state.last_selected_rule = None
            # Reset primary table selectbox
            if 't1' in st.session_state:
                del st.session_state['t1']
            st.rerun()