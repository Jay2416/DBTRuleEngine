import datetime
import os
import re
import time
import uuid

import pandas as pd
import streamlit as st


def render_create_edit_workspace(role, user, table_schemas, table_names, ctx):
    AUTH_CATALOG = ctx["AUTH_CATALOG"]
    AUTH_SCHEMA = ctx["AUTH_SCHEMA"]
    run_query = ctx["run_query"]
    run_update = ctx["run_update"]
    get_all_groups = ctx["get_all_groups"]
    get_next_group_id = ctx["get_next_group_id"]
    setup_new_group_environment = ctx["setup_new_group_environment"]
    get_next_rule_id = ctx["get_next_rule_id"]
    get_rules_by_group = ctx["get_rules_by_group"]
    get_rule_visual_position = ctx["get_rule_visual_position"]
    load_rule_state = ctx["load_rule_state"]
    get_group_rules_for_sequence = ctx["get_group_rules_for_sequence"]
    map_rules_to_visual_sequence = ctx["map_rules_to_visual_sequence"]
    get_existing_rules = ctx["get_existing_rules"]
    safe_model_name = ctx["safe_model_name"]
    safe_index = ctx["safe_index"]
    is_numeric_literal = ctx["is_numeric_literal"]
    build_live_rule_preview_sql = ctx["build_live_rule_preview_sql"]
    save_ui_state = ctx["save_ui_state"]
    save_dbt_model = ctx["save_dbt_model"]
    calculate_sequence_plan = ctx["calculate_sequence_plan"]
    update_rule_sequence = ctx["update_rule_sequence"]
    update_rule_sequence_batch = ctx["update_rule_sequence_batch"]
    get_role_approval_level = ctx["get_role_approval_level"]
    upsert_rule_approval_step = ctx["upsert_rule_approval_step"]
    create_final_rule_file = ctx["create_final_rule_file"]
    check_group_fully_approved = ctx["check_group_fully_approved"]
    execute_dbt_group = ctx["execute_dbt_group"]
    _clear_dynamic_widget_keys = ctx["_clear_dynamic_widget_keys"]

    TABLE_SCHEMAS = dict(table_schemas)
    TABLE_NAMES = list(table_names)

    def normalize_column_token(col_name):
        if not isinstance(col_name, str):
            return ""
        return re.sub(r"[^a-zA-Z0-9]", "", col_name).lower()

    def resolve_key_for_table(table_name, group_key_column):
        key_norm = normalize_column_token(group_key_column)
        for col in TABLE_SCHEMAS.get(table_name, []):
            if normalize_column_token(col) == key_norm:
                return col
        return None

    def load_explanation_id_columns():
        candidate_paths = [
            os.path.join("/", "dbt", "formula1", "Explanation.md"),
            os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "dbt", "formula1", "Explanation.md")),
        ]
        for path in candidate_paths:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                tokens = re.findall(r"`([A-Za-z_][A-Za-z0-9_]*)`", content)
                id_tokens = [t for t in tokens if t.lower().endswith("id")]
                if id_tokens:
                    deduped = []
                    for tok in id_tokens:
                        if tok not in deduped:
                            deduped.append(tok)
                    return deduped
            except Exception:
                continue
        return []

    def get_key_column_options_from_explanation():
        doc_ids = load_explanation_id_columns()
        schema_cols = {
            col
            for cols in TABLE_SCHEMAS.values()
            for col in cols
            if normalize_column_token(col)
        }
        if doc_ids:
            doc_norm = {normalize_column_token(c) for c in doc_ids}
            matched = sorted([c for c in schema_cols if normalize_column_token(c) in doc_norm])
            if matched:
                return matched

        fallback = sorted([c for c in schema_cols if normalize_column_token(c).endswith("id")])
        return fallback

    get_rule_group_non_seq_config = ctx.get("get_rule_group_non_seq_config")
    get_rule_group_columns = ctx.get("get_rule_group_columns")
    group_columns = set(get_rule_group_columns() or []) if callable(get_rule_group_columns) else set()
    group_remark_field = "remark" if "remark" in group_columns else "target_column"

    st.title("Rule Registry: Manage Business Logic")

    if role in ["MANAGER", "TECHNICAL", "SME"]:
        st.caption(f"Signed in as {role}. Rules saved here are auto-approved up to your level.")
        if st.button("Return to Approval Workspace", key="back_to_mgr_workspace"):
            st.session_state.mgr_show_rule_builder = False
            st.rerun()

    clean_name = lambda x: str(x) if x else ""

    if "create_new_group" not in st.session_state:
        st.session_state.create_new_group = False
    if "active_group_id" not in st.session_state:
        st.session_state.active_group_id = None

    with st.container(border=True):
        st.markdown("#### 1. Rule Context & Grouping")

        col1, col2 = st.columns([8, 2])

        with col1:
            if not st.session_state.create_new_group:
                groups = get_all_groups()
                if groups:
                    group_dict = {f"{g[1]} ({g[2]})": g for g in groups}
                    selected_group_label = st.selectbox("Select Rule Group:", list(group_dict.keys()))
                    active_group = group_dict[selected_group_label]

                    st.session_state.active_group_id = active_group[0]
                    st.session_state.active_group_name = active_group[1]
                    st.session_state.active_group_mode = active_group[2]
                else:
                    st.info("No groups found. Please create a new group.")
                    st.session_state.active_group_id = None
            else:
                st.info("🆕 Creating a New Rule Group")
                new_g_name = st.text_input("New Group Name:", placeholder="e.g., Fraud_Risk_Scoring")
                new_g_mode = st.radio("Execution Mode:", ["non_sequential", "sequential"], horizontal=True)

                key_col = ""

                if new_g_mode == "non_sequential":
                    with st.container(border=True):
                        st.markdown("##### ⚙️ Non-Sequential Configuration")
                        st.caption("Select the grouping key. Rule remark text will be set per rule.")

                        key_options = get_key_column_options_from_explanation()
                        if not key_options:
                            st.error("No ID-style key columns were found from schemas.")
                        else:
                            key_col = st.selectbox(
                                "Grouping Key (ID column):",
                                key_options,
                                key="new_grp_key",
                                help="Only tables containing this key will be available while authoring rules in this group.",
                            )

                        st.text_input(
                            "Output Remark Column:",
                            value="remark",
                            disabled=True,
                            help="Fixed output column for non-sequential aggregation.",
                        )

        with col2:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            if not st.session_state.create_new_group:
                if st.button("➕ New Group", use_container_width=True):
                    st.session_state.create_new_group = True
                    st.rerun()
            else:
                if st.button("💾 Save Group", type="primary", use_container_width=True):
                    if new_g_name:
                        if new_g_mode == "non_sequential" and not key_col:
                            st.error("⚠️ Please select a Grouping Key for non-sequential execution.")
                        else:
                            new_id = get_next_group_id()
                            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            current_user = user["username"]

                            insert_cols = [
                                "group_id",
                                "group_name",
                                "execution_mode",
                                "created_by",
                                "created_at",
                                "key_column",
                            ]
                            insert_vals = [
                                str(new_id),
                                f"'{new_g_name}'",
                                f"'{new_g_mode}'",
                                f"'{current_user}'",
                                f"CAST('{now}' AS TIMESTAMP)",
                                f"'{key_col}'" if key_col else "NULL",
                            ]

                            if new_g_mode == "non_sequential":
                                insert_cols.append(group_remark_field)
                                insert_vals.append("'remark'")

                            sql = f"""
                                INSERT INTO {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group
                                ({', '.join(insert_cols)})
                                VALUES
                                ({', '.join(insert_vals)})
                                """

                            if run_update(sql):
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

    if st.session_state.active_group_id and not st.session_state.create_new_group:
        with st.container(border=True):
            st.markdown(f"#### 2. Rule Definitions for: {st.session_state.active_group_name}")

            previous_action_mode = st.session_state.get("prev_action_mode", "Create New Rule")
            action_mode = st.radio("Action:", ["Create New Rule", "Update Existing Rule"], horizontal=True)

            # --- NEW FIX: WIPE THE CANVAS WHEN SWITCHING TO 'CREATE NEW RULE' ---
            if action_mode != previous_action_mode:
                st.session_state.prev_action_mode = action_mode
                if action_mode == "Create New Rule":
                    _clear_dynamic_widget_keys()
                    st.session_state.joins = []
                    st.session_state.aggregations = []
                    st.session_state.having_filters = []
                    st.session_state.manual_group_by = []
                    if "row_limit" in st.session_state:
                        del st.session_state["row_limit"]
                    st.session_state.where_filters = [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "val2": "", "logic": "AND"}]
                    st.session_state.order_filters = [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}]
                    st.session_state.loaded_primary_table = None
                    st.session_state.loaded_primary_columns = None
                    st.session_state.loaded_cols_joins = None
                    st.session_state.last_selected_rule = None
                    if "rule_remark_text" in st.session_state:
                        del st.session_state["rule_remark_text"]
                    if "t1" in st.session_state:
                        del st.session_state["t1"]
                    st.rerun() # Force immediate UI refresh with clean state

            if action_mode == "Create New Rule":
                st.session_state.currently_loaded_rule = None
                current_id = get_next_rule_id()
                st.info(f"🆕 Creating Rule ID: **{current_id}**")
                r_name = st.text_input("Rule Name (Compulsory)", placeholder="e.g., fraud_check_amount")

            else:
                group_rules = get_rules_by_group(st.session_state.active_group_id)
                if group_rules:
                    rule_dict = {f"ID: {row[0]} - {row[1]}": row[0] for row in group_rules}
                    selected_rule_label = st.selectbox("Select Rule to Update:", list(rule_dict.keys()), key="rule_selector")
                    current_id = rule_dict[selected_rule_label]
                    st.info(f"🔄 Updating Rule ID: **{current_id}**")

                    selected_rule_name = [r[1] for r in group_rules if r[0] == current_id][0]
                    r_name = st.text_input("Rule Name", value=selected_rule_name, disabled=True)

                    selected_rule_visual_seq = get_rule_visual_position(st.session_state.active_group_id, current_id)

                    if st.session_state.get("last_selected_rule") != selected_rule_name:
                        st.session_state.last_selected_rule = selected_rule_name
                        st.session_state.visual_sequence_input = int(selected_rule_visual_seq)
                        if load_rule_state(selected_rule_name, st.session_state.active_group_name):
                            st.rerun()
                else:
                    st.warning(f"No existing rules found in '{st.session_state.active_group_name}'.")
                    current_id = None
                    r_name = ""

            rule_remark_text = st.session_state.get("rule_remark_text", "")
            if r_name or action_mode == "Create New Rule":
                if st.session_state.active_group_mode == "non_sequential":
                    rule_remark_text = st.text_input(
                        "Rule Remark (Compulsory)",
                        key="rule_remark_text",
                        placeholder="e.g., Q3 Contender",
                        help="This remark is appended for matching keys when this rule is triggered.",
                    )

                st.divider()

                if st.session_state.active_group_mode == "sequential":
                    st.markdown("**Sequential Execution Setup**")

                    default_visual = (
                        get_rule_visual_position(st.session_state.active_group_id, current_id)
                        if action_mode == "Update Existing Rule"
                        else (len(get_group_rules_for_sequence(st.session_state.active_group_id)) + 1)
                    )
                    visual_sequence_input = st.session_state.get("visual_sequence_input", int(default_visual))

                    seq_left, seq_right = st.columns([1, 1], gap="large")
                    with seq_left:
                        with st.container(border=True):
                            sequence_visual_order = st.number_input(
                                "Sequence Position",
                                min_value=1,
                                step=1,
                                value=int(visual_sequence_input),
                                key="visual_sequence_input",
                            )

                    with seq_right:
                        with st.container(border=True):
                            rules_for_map = get_group_rules_for_sequence(st.session_state.active_group_id)
                            mapped_rules = map_rules_to_visual_sequence(rules_for_map)
                            if mapped_rules:
                                df_seq = pd.DataFrame(
                                    [
                                        {"Sequence Order": row["visual_sequence"], "Rule Name": row["rule_name"]}
                                        for row in mapped_rules
                                    ]
                                )
                                st.dataframe(df_seq, hide_index=True, use_container_width=True)
                            else:
                                st.info("No existing rules in this group yet.")

                    sequence_order = int(sequence_visual_order)
                else:
                    st.info("ℹ️ Group is non-sequential. All rules run in parallel.")
                    sequence_order = 0

    active_group_key_column = ""
    active_group_remark_column = "remark"
    if st.session_state.active_group_id and st.session_state.get("active_group_mode") == "non_sequential":
        if callable(get_rule_group_non_seq_config):
            cfg = get_rule_group_non_seq_config(st.session_state.active_group_id)
            if cfg:
                active_group_key_column = cfg.get("key_column", "")
                active_group_remark_column = cfg.get("remark_column", "remark")

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

    TABLE_NAMES = [t for t in TABLE_SCHEMAS.keys() if (t not in all_rule_models) or (t in group_rule_models)]
    JOIN_TABLE_NAMES = list(TABLE_NAMES)

    if st.session_state.get("active_group_mode") == "non_sequential" and active_group_key_column:
        key_norm = normalize_column_token(active_group_key_column)
        TABLE_NAMES = [
            t
            for t in TABLE_NAMES
            if any(normalize_column_token(c) == key_norm for c in TABLE_SCHEMAS.get(t, []))
        ]

    if st.session_state.get("t1") and st.session_state["t1"] not in TABLE_NAMES:
        del st.session_state["t1"]

    if not TABLE_NAMES:
        st.error("No tables are available for this group key column. Please check group key configuration.")
        return

    clean_name = lambda x: x

    col_src1, col_src2 = st.columns(2)

    with col_src1:
        with st.container(border=True):
            st.markdown("#### 3. Data Sources & Joins")

            join_table_options = JOIN_TABLE_NAMES if st.session_state.get("active_group_mode") == "non_sequential" else TABLE_NAMES

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
                    join_data["right_table"] = st.selectbox("Table to Join:", join_table_options, key=t_right_key, format_func=clean_name)
                else:
                    right_table_idx = safe_index(join_table_options, join_data.get("right_table"))
                    join_data["right_table"] = st.selectbox(
                        "Table to Join:", join_table_options, index=right_table_idx, key=t_right_key, format_func=clean_name
                    )

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
                        left_tbl_opts = available_left_tables[: i + 1]
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
                            join_data["left_col"] = st.selectbox(
                                f"Column in {clean_name(l_table_actual)}:", l_cols, index=left_col_idx, key=l_col_key
                            )
                    with sub_col2:
                        r_table_actual = join_data["right_table"]
                        r_cols = TABLE_SCHEMAS.get(r_table_actual, [])
                        r_col_key = f"r_col_{uid}"
                        if r_col_key in st.session_state:
                            join_data["right_col"] = st.selectbox(f"Column in {clean_name(r_table_actual)}:", r_cols, key=r_col_key)
                        else:
                            right_col_idx = safe_index(r_cols, join_data.get("right_col"))
                            join_data["right_col"] = st.selectbox(
                                f"Column in {clean_name(r_table_actual)}:", r_cols, index=right_col_idx, key=r_col_key
                            )

                if st.button("➖ Remove Join", key=f"rm_j_{uid}"):
                    joins_to_remove.append(i)
                st.divider()

            for index in sorted(joins_to_remove, reverse=True):
                st.session_state.joins.pop(index)
                st.rerun()

            if st.button("➕ Add Join"):
                st.session_state.joins.append(
                    {
                        "id": str(uuid.uuid4()),
                        "right_table": "",
                        "join_type": "LEFT JOIN",
                        "left_table": "",
                        "left_col": "",
                        "right_col": "",
                    }
                )
                st.rerun()

    with col_src2:
        with st.container(border=True):
            st.markdown("#### 4. Output Columns Selection")
            st.caption("Select the columns you want to pass through to the final rule output.")

            primary_cols_key = f"primary_cols_{t1}"
            cols_t1 = []

            if st.session_state.active_group_mode == "non_sequential" and active_group_key_column:
                resolved_key = resolve_key_for_table(t1, active_group_key_column)
                if not resolved_key:
                    st.error(
                        f"Primary table '{clean_name(t1)}' does not contain configured key column '{active_group_key_column}'."
                    )
                else:
                    st.session_state[primary_cols_key] = [resolved_key]
                    cols_t1 = st.multiselect(
                        f"Output Column for {clean_name(t1)}:",
                        [resolved_key],
                        default=[resolved_key],
                        max_selections=1,
                        key=primary_cols_key,
                        disabled=True,
                        help="Non-sequential mode uses a single fixed key output column.",
                    )
            else:
                if primary_cols_key not in st.session_state:
                    st.session_state[primary_cols_key] = TABLE_SCHEMAS.get(t1, [])[:3]
                cols_t1 = st.multiselect(
                    f"Output Columns for {clean_name(t1)}:",
                    TABLE_SCHEMAS.get(t1, []),
                    key=primary_cols_key,
                )

            cols_joins = {}
            for join_data in st.session_state.joins:
                r_table = join_data.get("right_table")
                if r_table and r_table not in cols_joins:
                    if st.session_state.active_group_mode == "non_sequential":
                        st.caption(f"Output for {clean_name(r_table)} is disabled in non-sequential mode.")
                        cols_joins[r_table] = []
                    else:
                        out_col_key = f"out_col_{r_table}"
                        if out_col_key not in st.session_state:
                            st.session_state[out_col_key] = TABLE_SCHEMAS.get(r_table, [])[:3]

                        cols_joins[r_table] = st.multiselect(
                            f"Output Columns for {clean_name(r_table)}:", TABLE_SCHEMAS.get(r_table, []), key=out_col_key
                        )

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
                    agg_data["col"] = st.selectbox(
                        "Column", available_agg_columns, key=agg_col_key, label_visibility="collapsed" if i > 0 else "visible"
                    )
                else:
                    col_idx = safe_index(available_agg_columns, agg_data.get("col"))
                    agg_data["col"] = st.selectbox(
                        "Column",
                        available_agg_columns,
                        index=col_idx,
                        key=agg_col_key,
                        label_visibility="collapsed" if i > 0 else "visible",
                    )

            with agg_col2:
                agg_func_key = f"agg_func_{uid}"
                if agg_func_key in st.session_state:
                    agg_data["func"] = st.selectbox(
                        "Function", agg_func_options, key=agg_func_key, label_visibility="collapsed" if i > 0 else "visible"
                    )
                else:
                    func_idx = safe_index(agg_func_options, agg_data.get("func"))
                    agg_data["func"] = st.selectbox(
                        "Function",
                        agg_func_options,
                        index=func_idx,
                        key=agg_func_key,
                        label_visibility="collapsed" if i > 0 else "visible",
                    )

            with agg_col3:
                agg_alias_key = f"agg_alias_{uid}"
                agg_data["alias"] = st.text_input(
                    "Alias (AS)",
                    value=agg_data.get("alias", ""),
                    key=agg_alias_key,
                    label_visibility="collapsed" if i > 0 else "visible",
                    placeholder="e.g., total_points",
                )

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

        auto_grouped_cols = selected_output_columns
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
                        filter_data["logic"] = st.selectbox("AND/OR", logic_opts, key=logic_key, label_visibility="collapsed")
                    else:
                        logic_idx = safe_index(logic_opts, filter_data.get("logic"))
                        filter_data["logic"] = st.selectbox("AND/OR", logic_opts, index=logic_idx, key=logic_key, label_visibility="collapsed")
                else:
                    st.write("WHERE")
            with row_col2:
                w_col_key = f"w_col_{uid}"
                if w_col_key in st.session_state:
                    filter_data["col"] = st.selectbox("Column", available_columns, key=w_col_key)
                else:
                    col_idx = safe_index(available_columns, filter_data.get("col"))
                    filter_data["col"] = st.selectbox("Column", available_columns, index=col_idx, key=w_col_key)
            with row_col3:
                op_options = [
                    "equals (=)",
                    "greater than (>)",
                    "less than (<)",
                    "not equals (!=)",
                    "greater than equal (>=)",
                    "less than equal (<=)",
                    "not equal to (!=)",
                    "LIKE",
                    "NOT LIKE",
                    "IN",
                    "NOT IN",
                    "IS",
                    "IS NOT",
                    "IS NULL",
                    "IS NOT NULL",
                    "BETWEEN",
                ]
                w_op_key = f"w_op_{uid}"
                if w_op_key in st.session_state:
                    filter_data["op"] = st.selectbox("Operator", op_options, key=w_op_key)
                else:
                    op_idx = safe_index(op_options, filter_data.get("op"))
                    filter_data["op"] = st.selectbox("Operator", op_options, index=op_idx, key=w_op_key)
            with row_col4:
                if filter_data["op"] in ["IS NULL", "IS NOT NULL"]:
                    st.write("")
                    filter_data["val"] = ""
                    filter_data["val2"] = ""
                elif filter_data["op"] == "BETWEEN":
                    sub_col1, sub_col2 = st.columns(2)
                    with sub_col1:
                        w_val1_key = f"w_val1_{uid}"
                        if w_val1_key not in st.session_state:
                            st.session_state[w_val1_key] = filter_data.get("val", "")
                        filter_data["val"] = st.text_input("From", key=w_val1_key)
                    with sub_col2:
                        w_val2_key = f"w_val2_{uid}"
                        if w_val2_key not in st.session_state:
                            st.session_state[w_val2_key] = filter_data.get("val2", "")
                        filter_data["val2"] = st.text_input("To", key=w_val2_key)
                else:
                    w_val_key = f"w_val_{uid}"
                    if w_val_key not in st.session_state:
                        st.session_state[w_val_key] = filter_data.get("val", "")
                    filter_data["val"] = st.text_input("Value", key=w_val_key)
                    filter_data["val2"] = ""

            with row_col5:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️", key=f"w_del_{uid}"):
                    filters_to_remove.append(i)

        for index in sorted(filters_to_remove, reverse=True):
            st.session_state.where_filters.pop(index)
            st.rerun()

        if st.button("➕ Add Condition"):
            st.session_state.where_filters.append(
                {"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "val2": "", "logic": "AND"}
            )
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
                        having_data["logic"] = st.selectbox("AND/OR", having_logic_opts, key=h_logic_key, label_visibility="collapsed")
                    else:
                        h_logic_idx = safe_index(having_logic_opts, having_data.get("logic"))
                        having_data["logic"] = st.selectbox(
                            "AND/OR", having_logic_opts, index=h_logic_idx, key=h_logic_key, label_visibility="collapsed"
                        )
                else:
                    st.write("HAVING")

            with h_col2:
                h_func_key = f"h_func_{uid}"
                if h_func_key in st.session_state:
                    having_data["func"] = st.selectbox("Function", having_func_opts, key=h_func_key)
                else:
                    h_func_idx = safe_index(having_func_opts, having_data.get("func"))
                    having_data["func"] = st.selectbox("Function", having_func_opts, index=h_func_idx, key=h_func_key)

            with h_col3:
                h_col_key = f"h_col_{uid}"
                if h_col_key in st.session_state:
                    having_data["col"] = st.selectbox("Column", available_columns, key=h_col_key)
                else:
                    h_col_idx = safe_index(available_columns, having_data.get("col"))
                    having_data["col"] = st.selectbox("Column", available_columns, index=h_col_idx, key=h_col_key)

            with h_col4:
                h_op_key = f"h_op_{uid}"
                if h_op_key in st.session_state:
                    having_data["op"] = st.selectbox("Operator", having_op_opts, key=h_op_key)
                else:
                    h_op_idx = safe_index(having_op_opts, having_data.get("op"))
                    having_data["op"] = st.selectbox("Operator", having_op_opts, index=h_op_idx, key=h_op_key)

            with h_col5:
                having_data["val"] = st.text_input("Value", value=having_data.get("val", ""), key=f"h_val_{uid}")

            with h_col6:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️", key=f"h_del_{uid}"):
                    having_to_remove.append(i)

        for index in sorted(having_to_remove, reverse=True):
            st.session_state.having_filters.pop(index)
            st.rerun()

        if st.button("➕ Add Having Condition"):
            st.session_state.having_filters.append(
                {"id": str(uuid.uuid4()), "logic": "AND", "func": "COUNT", "col": "", "op": ">", "val": ""}
            )
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
                    sort_data["col"] = st.selectbox("Sort Column", available_columns, key=o_col_key)
                else:
                    sort_col_idx = safe_index(available_columns, sort_data.get("col"))
                    sort_data["col"] = st.selectbox("Sort Column", available_columns, index=sort_col_idx, key=o_col_key)
            with row_col2:
                dir_opts = ["ASC", "DESC"]
                o_dir_key = f"o_dir_{uid}"
                if o_dir_key in st.session_state:
                    sort_data["dir"] = st.selectbox("Direction", dir_opts, key=o_dir_key)
                else:
                    dir_idx = safe_index(dir_opts, sort_data.get("dir"))
                    sort_data["dir"] = st.selectbox("Direction", dir_opts, index=dir_idx, key=o_dir_key)
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
        st.number_input(
            "Maximum rows to return (leave blank or 0 for no limit)",
            value=None,
            min_value=1,
            step=1,
            key="row_limit",
            placeholder="e.g., 100",
        )

    with st.container(border=True):
        st.markdown("#### 4. Preview & Run")

        macro_tables = [t1] + [j.get("right_table") for j in st.session_state.joins if j.get("right_table")]
        macro_tables = list(dict.fromkeys(macro_tables))

        macro_select_cols = [f"{t1}.{c}" for c in cols_t1]
        for r_table, cols in cols_joins.items():
            macro_select_cols.extend([f"{r_table}.{c}" for c in cols])

        macro_joins = []
        for j in st.session_state.joins:
            if j.get("right_table") and j.get("join_type") != "CROSS JOIN":
                l_table = j.get("left_table") if j.get("left_table") else t1
                macro_joins.append(
                    {
                        "type": j.get("join_type"),
                        "table": j.get("right_table"),
                        "left": f"{l_table}.{j.get('left_col')}",
                        "right": f"{j.get('right_table')}.{j.get('right_col')}",
                    }
                )

        macro_aggs = [
            {"func": a["func"], "col": a["col"], "alias": a.get("alias", "")}
            for a in st.session_state.aggregations
            if a.get("col") and a.get("func")
        ]

        macro_group_by = []
        if macro_aggs or st.session_state.having_filters or st.session_state.get("manual_group_by"):
            macro_group_by = list(dict.fromkeys(macro_select_cols + st.session_state.get("manual_group_by", [])))

        macro_where = []
        for f in st.session_state.where_filters:
            if f["col"]:
                raw_op = f.get("op", "")
                if "(" in raw_op and ")" in raw_op:
                    op_val = raw_op[raw_op.find("(") + 1 : raw_op.find(")")]
                else:
                    op_val = raw_op.split(" ")[0] if " " in raw_op and "equals" in raw_op else raw_op

                if op_val == "equals":
                    op_val = "="

                val = str(f.get("val", ""))
                if val and not is_numeric_literal(val) and op_val not in ["IS NULL", "IS NOT NULL"]:
                    val = f"'{val}'"

                val2 = str(f.get("val2", ""))
                if val2 and not is_numeric_literal(val2) and op_val not in ["IS NULL", "IS NOT NULL"]:
                    val2 = f"'{val2}'"

                macro_where.append({"col": f["col"], "op": op_val, "value": val, "value2": val2, "logic": f.get("logic", "")})

        macro_having = []
        for h in st.session_state.having_filters:
            if h.get("col") and h.get("func") and h.get("val"):
                val = str(h.get("val", ""))
                if not is_numeric_literal(val):
                    val = f"'{val}'"
                macro_having.append(
                    {"func": h["func"], "col": h["col"], "op": h["op"], "value": val, "logic": h.get("logic", "")}
                )

        macro_order = [{"col": o["col"], "dir": o["dir"]} for o in st.session_state.order_filters if o.get("col")]
        macro_limit = st.session_state.get("row_limit") or "None"

        if st.session_state.active_group_mode == "non_sequential":
            resolved_key = resolve_key_for_table(t1, active_group_key_column)
            escaped_remark = str(rule_remark_text or "").replace("'", "''")
            if resolved_key:
                macro_select_cols = [
                    f"{t1}.{resolved_key}",
                    f"'{escaped_remark}' AS {active_group_remark_column}",
                ]
            else:
                macro_select_cols = [f"'{escaped_remark}' AS {active_group_remark_column}"]
            macro_group_by = []
            macro_having = []
            macro_aggs = []

        rule_materialization = "ephemeral" if st.session_state.active_group_mode == "non_sequential" else "table"

        generated_sql = f"""{{{{ config(materialized='{rule_materialization}') }}}}

{{{{ rule_engine(
    tables={macro_tables}, joins={macro_joins}, select_columns={macro_select_cols},
    aggregations={macro_aggs}, where_filters={macro_where}, group_by={macro_group_by},
    having={macro_having}, order_by={macro_order}, limit_rows={macro_limit}
) }}}}"""

        if st.session_state.active_group_mode == "sequential":
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
                elif st.session_state.active_group_mode == "non_sequential" and not str(rule_remark_text or "").strip():
                    st.error("Please enter Rule Remark for this non-sequential rule.")
                else:
                    passed_schema_check = True

                    if st.session_state.active_group_mode == "non_sequential":
                        req_key = active_group_key_column
                        req_tgt = active_group_remark_column

                        final_output_cols = []
                        for c in macro_select_cols:
                            alias_match = re.search(r"\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", c, flags=re.IGNORECASE)
                            if alias_match:
                                final_output_cols.append(alias_match.group(1))
                            else:
                                final_output_cols.append(c.split(".")[-1].strip())
                        final_output_cols.extend([a["alias"] for a in macro_aggs if a.get("alias")])

                        missing_cols = []
                        if req_key not in final_output_cols:
                            missing_cols.append(f"`{req_key}` (Grouping Key)")
                        if req_tgt not in final_output_cols:
                            missing_cols.append(f"`{req_tgt}` (Remark Column)")

                        if missing_cols:
                            passed_schema_check = False
                            st.error(
                                f"❌ **Schema Mismatch:** This rule belongs to a non-sequential group. Your final output MUST contain: {', '.join(missing_cols)}."
                            )
                            st.info(
                                "💡 *Tip: Non-sequential mode requires fixed key output and the rule remark output column.*"
                            )

                    if passed_schema_check:
                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        current_user = user["username"]
                        try:
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
                                "order_filters": st.session_state.order_filters,
                                "sequence_order": sequence_order,
                                "rule_remark": rule_remark_text,
                            }

                            save_ui_state(r_name, st.session_state.active_group_name, ui_state)
                            st.session_state.schema_cache_key = st.session_state.get("schema_cache_key", 0) + 1

                            dbt_filepath = save_dbt_model(r_name, st.session_state.active_group_name, generated_sql)

                            computed_sequence_order = sequence_order
                            sequence_plan = None

                            if st.session_state.active_group_mode == "sequential":
                                base_rules = get_group_rules_for_sequence(
                                    st.session_state.active_group_id,
                                    exclude_rule_id=current_id if action_mode == "Update Existing Rule" else None,
                                )
                                planning_rule_id = current_id if action_mode == "Update Existing Rule" else -1
                                sequence_plan = calculate_sequence_plan(
                                    base_rules,
                                    sequence_order,
                                    {"rule_id": planning_rule_id, "rule_name": r_name},
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
                                    if st.session_state.active_group_mode == "sequential" and sequence_plan:
                                        if sequence_plan["mode"] == "single":
                                            update_rule_sequence(final_insert_id, sequence_plan["new_sequence_order"])
                                        else:
                                            batch_payload = []
                                            for row in sequence_plan["batch_updates"]:
                                                row_rule_id = final_insert_id if row["rule_id"] == -1 else row["rule_id"]
                                                batch_payload.append(
                                                    {
                                                        "rule_id": row_rule_id,
                                                        "rule_name": row["rule_name"],
                                                        "sequence_order": row["sequence_order"],
                                                        "visual_sequence": row["visual_sequence"],
                                                    }
                                                )
                                            update_rule_sequence_batch(batch_payload)

                                    actor_role = str(user.get("role", "")).upper()
                                    actor_level = get_role_approval_level(actor_role)
                                    actor_user_id = user.get("user_id")

                                    for level in [1, 2, 3]:
                                        if level <= actor_level:
                                            step_action = "APPROVED"
                                            step_comment = f"Auto-approved at Level {level} by {actor_role} during rule save."
                                            step_user_id = actor_user_id
                                        elif level == actor_level + 1:
                                            step_action = "PENDING"
                                            step_comment = f"Awaiting Level {level} review."
                                            step_user_id = None
                                        else:
                                            step_action = "WAITING"
                                            step_comment = "Waiting for prior level approval."
                                            step_user_id = None

                                        upsert_rule_approval_step(
                                            final_insert_id,
                                            level,
                                            step_action,
                                            step_comment,
                                            step_user_id,
                                            now,
                                        )

                                    if actor_level >= 3:
                                        run_update(
                                            f"UPDATE {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow "
                                            f"SET status = 'ACTIVE', updated_at = CAST('{now}' AS TIMESTAMP) "
                                            f"WHERE rule_id = {final_insert_id}"
                                        )

                                        can_execute_group = True
                                        if st.session_state.active_group_mode != "non_sequential":
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
                                    st.warning("📁 Database operation failed.")
                        except Exception as e:
                            st.error(f"❌ Failed to process rule: {e}")

        with col_btn2:
            if st.button("Clear All"):
                _clear_dynamic_widget_keys()
                st.session_state.joins = []
                st.session_state.aggregations = []
                st.session_state.having_filters = []
                st.session_state.manual_group_by = []
                if "row_limit" in st.session_state:
                    del st.session_state["row_limit"]
                st.session_state.where_filters = [{"id": str(uuid.uuid4()), "col": "", "op": "equals (=)", "val": "", "logic": "AND"}]
                st.session_state.order_filters = [{"id": str(uuid.uuid4()), "col": "", "dir": "ASC"}]
                st.session_state.loaded_primary_table = None
                st.session_state.loaded_primary_columns = None
                st.session_state.loaded_cols_joins = None
                st.session_state.last_selected_rule = None
                if "rule_remark_text" in st.session_state:
                    del st.session_state["rule_remark_text"]
                if "t1" in st.session_state:
                    del st.session_state["t1"]
                st.rerun()
