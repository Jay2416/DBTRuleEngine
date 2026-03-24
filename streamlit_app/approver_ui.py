import time

import streamlit as st


def render_approver_workspace(role, user, ctx):
    get_pending_groups_for_level = ctx["get_pending_groups_for_level"]
    approve_group_rules = ctx["approve_group_rules"]
    get_rules_for_approval = ctx["get_rules_for_approval"]
    read_dbt_sql = ctx["read_dbt_sql"]
    process_approval = ctx["process_approval"]

    st.title(f"✅ {role.capitalize()} Approval Workspace")

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
                        user["user_id"],
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
                    success, msg = process_approval(app_id, r_id, r_name, my_level, user["user_id"], "APPROVED", review_comments)
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
                        success, msg = process_approval(app_id, r_id, r_name, my_level, user["user_id"], "REJECTED", review_comments)
                        if success:
                            st.warning(msg)
                            time.sleep(1.5)
                            st.rerun()
