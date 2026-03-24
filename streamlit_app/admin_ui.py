import datetime
import os
import re
import shutil
import time
import uuid

import pandas as pd
import streamlit as st


def render_admin_workspace(user, ctx):
    AUTH_CATALOG = ctx["AUTH_CATALOG"]
    AUTH_SCHEMA = ctx["AUTH_SCHEMA"]
    AUTH_TABLE = ctx["AUTH_TABLE"]

    run_query = ctx["run_query"]
    run_update = ctx["run_update"]
    check_user_exists = ctx["check_user_exists"]
    hash_password = ctx["hash_password"]
    get_next_approval_id = ctx["get_next_approval_id"]
    create_final_rule_file = ctx["create_final_rule_file"]
    check_group_fully_approved = ctx["check_group_fully_approved"]
    execute_dbt_group = ctx["execute_dbt_group"]
    get_group_name_for_rule = ctx["get_group_name_for_rule"]
    get_s3_client = ctx["get_s3_client"]
    MINIO_BUCKET = ctx["MINIO_BUCKET"]
    get_group_overview_rows = ctx["get_group_overview_rows"]
    approve_group_rules = ctx["approve_group_rules"]
    delete_group_with_assets = ctx["delete_group_with_assets"]

    st.title("🛡️ Admin Control Panel")

    tab1, tab2 = st.tabs(["👥 User Management", "🗂️ Global Rule Oversight"])

    with tab1:
        st.subheader("Manage Users & Roles")

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
                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

        st.caption("Edit the 'System Role' column to change permissions. Set a user to 'SUSPENDED' to deactivate their access.")

        users_sql = f"SELECT user_id, username, email, role, CAST(last_login AS STRING) as last_login FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.{AUTH_TABLE} ORDER BY created_at DESC"
        users_data = run_query(users_sql)

        if users_data:
            df_users = pd.DataFrame(users_data, columns=["user_id", "username", "email", "role", "last_login"])

            edited_df = st.data_editor(
                df_users,
                column_config={
                    "role": st.column_config.SelectboxColumn(
                        "System Role",
                        options=["CREATOR", "MANAGER", "TECHNICAL", "SME", "ADMIN", "SUSPENDED"],
                        required=True,
                    ),
                    "user_id": None,
                    "username": "Username",
                    "email": "Email Address",
                    "last_login": "Last Login",
                },
                disabled=["username", "email", "last_login"],
                hide_index=True,
                key="user_editor",
                use_container_width=True,
            )

            if st.button("💾 Save Role Changes", type="primary"):
                changes_made = False
                with st.spinner("Updating user roles..."):
                    for index, row in edited_df.iterrows():
                        orig_role = df_users.loc[index, "role"]
                        new_role = row["role"]
                        if orig_role != new_role:
                            uid = row["user_id"]
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

                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        admin_user_id = user["user_id"]
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

                        with st.spinner("Finalizing file and checking group status..."):
                            success_copy, copy_msg = create_final_rule_file(sel_rule_name)

                            if success_copy:
                                grp_sql = f"SELECT g.group_id, g.group_name, g.execution_mode FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow r JOIN {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_group g ON r.group_id = g.group_id WHERE r.rule_id = {sel_rule_id}"
                                grp_res = run_query(grp_sql)
                                g_id, g_name, _ = grp_res[0]

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
                    safe_group = re.sub(r"[^a-zA-Z0-9_]", "_", group_name.lower()) if group_name else "ungrouped"
                    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", sel_rule_name.lower())

                    run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.approval_workflow WHERE rule_id = {sel_rule_id}")
                    if run_update(f"DELETE FROM {AUTH_CATALOG}.{AUTH_SCHEMA}.rule_workflow WHERE rule_id = {sel_rule_id}"):
                        sql_path = os.path.join("/", "dbt", "formula1", "models", "rules", safe_group, f"{safe_name}.sql")
                        if os.path.exists(sql_path):
                            os.remove(sql_path)

                        final_sql_path = os.path.join(
                            "/", "dbt", "formula1", "models", "final_rules", f"final_{safe_group}", f"final_{safe_name}.sql"
                        )
                        if os.path.exists(final_sql_path):
                            os.remove(final_sql_path)

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
                columns=["Group ID", "Group Name", "Mode", "Total Rules", "Active", "In Review", "Rejected"],
            )
            st.dataframe(group_df, use_container_width=True, hide_index=True)

            group_dict = {f"Group ID: {row[0]} | {row[1]} ({row[2]}) | Rules: {row[3]}": row for row in group_rows}
            selected_group_label = st.selectbox("Select Group to Modify:", list(group_dict.keys()), key="god_mode_group_selector")
            selected_group_row = group_dict[selected_group_label]
            selected_group_id = selected_group_row[0]
            selected_group_name = selected_group_row[1]

            group_comments = st.text_area(
                "Group action comments (optional):",
                key="god_mode_group_comments",
                placeholder="Reason for force-approving or deleting this group...",
            )

            g_col1, g_col2 = st.columns(2)
            with g_col1:
                if st.button("✅ Force Approve Whole Group", use_container_width=True, key="god_force_approve_group"):
                    with st.spinner("Approving all rules in group and running master model..."):
                        success, msg = approve_group_rules(
                            selected_group_id,
                            user["user_id"],
                            user["role"],
                            comments=group_comments,
                            force_admin=True,
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
                    key="confirm_group_delete",
                )
                if st.button("🗑️ Delete Group (Admin Only)", type="primary", use_container_width=True, key="god_delete_group"):
                    if not confirm_delete_group:
                        st.error("Please confirm group deletion first.")
                    else:
                        with st.spinner("Deleting group, related rules, SQL assets, and MinIO states..."):
                            success, msg = delete_group_with_assets(selected_group_id, user["role"])
                            if success:
                                st.success(msg)
                                time.sleep(1.5)
                                st.rerun()
                            else:
                                st.error(msg)
        else:
            st.info("No groups currently exist in the database.")
