import time

import streamlit as st


def render_auth_page(
    register_user,
    verify_user,
    verify_user_for_reset,
    reset_password,
    is_valid_password,
    is_valid_phone,
):
    st.title("🛡️ DBT Rule Engine Access")

    tab1, tab2 = st.tabs(["🔑 Login", "📝 Register"])

    with tab1:
        st.subheader("Login to your account")

        login_id = st.text_input("Username or Email", key="login_id")
        login_pw = st.text_input("Password", type="password", key="login_pw")

        with st.popover("Forgot Password?"):
            st.markdown("### 🔐 Reset Password")
            st.caption("Verify your identity to set a new password.")

            fp_email = st.text_input("Registered Email", key="fp_email")
            fp_mobile = st.text_input("Registered Mobile", key="fp_mobile")

            st.divider()

            fp_new_pw = st.text_input("New Password", type="password", key="fp_new")
            fp_conf_pw = st.text_input("Confirm New Password", type="password", key="fp_conf")

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

        st.write("")
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
                    success, msg = register_user(r_fname, r_lname, r_uname, r_email, r_phone, r_pw)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
