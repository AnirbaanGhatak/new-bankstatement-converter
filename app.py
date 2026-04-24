import streamlit as st
import os
import pandas as pd
from io import BytesIO

# Import your master class
from core_parser import BankStatementParser
# Import the new ML workers we designed
from trainmodel import train_custom_model, apply_categorization, get_sheet_names

# --- Configuration ---
st.set_page_config(
    page_title="PDF Processor",
    page_icon="📄",
    layout="wide"
)

# --- The Dashboard App ---
st.title("🤖 Bank Statement Converter")

# --- Bank Selection Map ---
bank_options = {
    "Axis Bank": "AXIS",
    "Bank of Baroda": "BOB",
    "Bank of Maharashtra": "BOM",
    "Canara Bank": "CANARA",
    "Greater Bombay Co-op Bank": "GBM",
    "HDFC Bank": "HDFC",
    "ICICI Bank": "ICICI",
    "Indian Bank": "INDIAN",
    "Kotak Mahindra Bank": "KOTAK",
    "Saraswat Bank": "SARASWAT",
    "Standard Chartered": "SCB",
    "Union Bank of India": "UNION",
    "Yes Bank": "YES"
}

# --- State Management ---
if 'uploaded_file_details' not in st.session_state:
    st.session_state.uploaded_file_details = None
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'selected_bank' not in st.session_state:
    st.session_state.selected_bank = None
if 'active_model' not in st.session_state:
    st.session_state.active_model = None

def convert_to_excel(df):
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Transactions")
    return excel_buffer.getvalue()

# ==========================================
# UI: INPUT SECTION
# ==========================================
st.subheader("Step 1: Target Statement (Required)")
col1, col2 = st.columns([1, 2])

with col1:
    selected_bank_display = st.selectbox("Select Bank Format", list(bank_options.keys()))
    selected_bank_code = bank_options[selected_bank_display]

with col2:
    uploaded_file = st.file_uploader(
        "Upload PDF :red-badge[⚠️ ONLY FILES WITHOUT PASSWORDS]", 
        type="pdf",
        accept_multiple_files=False
    )

st.info("If encountered any error or a bank is to be added please inform Anirbaan(Tojo)")


st.divider()

st.subheader("Step 2: AI Categorization (Optional) (Trials Ongoing)")
st.info("Upload previously categorized Excels to teach the AI how to assign Account Heads.")
historical_files = st.file_uploader("Upload Historical Data", type=["xlsx", "xls"], accept_multiple_files=True)

# Dynamic Sheet Selectors
file_sheet_selections = []
if historical_files:
    st.markdown("**Select the correct sheet for each uploaded file:**")
    # Display side-by-side columns for the dropdowns to keep the UI clean
    cols = st.columns(len(historical_files)) 
    for i, file in enumerate(historical_files):
        with cols[i]:
            sheet_names = get_sheet_names(file)
            selected_sheet = st.selectbox(f"Sheet for: {file.name}", options=sheet_names, key=file.name)
            file_sheet_selections.append((file, selected_sheet))

st.divider()

# ==========================================
# EXECUTION ENGINE (Triggered by Button)
# ==========================================
if st.button("🚀 Process Bank Statement", type="primary", use_container_width=True):
    
    if uploaded_file is None:
        st.error("⚠️ Please upload a PDF statement to begin.")
        st.stop()
        
    try:
        # --- Phase 1: PDF Parsing (With Memory Check) ---
        is_new_file = (st.session_state.uploaded_file_details is None or uploaded_file.name != st.session_state.uploaded_file_details['name'])
        is_new_bank = (selected_bank_code != st.session_state.selected_bank)
        
        if is_new_file or is_new_bank:
            with st.spinner(f"Parsing {selected_bank_display} statement... Wait for it..."):
                file_content = uploaded_file.getvalue()
                temp_pdf_path = "temp_statement.pdf"
                
                with open(temp_pdf_path, "wb") as f:
                    f.write(file_content)
                
                # Use YOUR parser
                parser = BankStatementParser(temp_pdf_path, selected_bank_code)
                raw_df = parser.process()
                
                if os.path.exists(temp_pdf_path):
                    os.remove(temp_pdf_path)
                
                if isinstance(raw_df, pd.DataFrame) and not raw_df.empty:
                    # Save to memory!
                    st.session_state.uploaded_file_details = {'name': uploaded_file.name}
                    st.session_state.selected_bank = selected_bank_code
                    st.session_state.processed_df = raw_df
                    st.toast(f"{selected_bank_display} PDF parsed successfully!", icon="📄")
                else:
                    st.error("⚠️ Processing complete, but no data could be extracted. The PDF might be a scan.")
                    st.stop()

        # Grab a fresh copy of the parsed data from memory
        working_df = st.session_state.processed_df.copy()

        # --- Phase 2: AI Categorization ---
        if historical_files:
            with st.spinner("Training custom AI model on historical data..."):
                active_model, row_count = train_custom_model(file_sheet_selections)
                st.toast(f"AI Trained on {row_count} transactions!", icon="🧠")
                
            with st.spinner("Applying AI Categorization..."):
                working_df = apply_categorization(working_df, active_model)
                st.success("✅ Workflow Complete: PDF Parsed & Categorized!")
        else:
            st.success("✅ Workflow Complete: PDF Parsed (No AI Categorization Applied).")

        # --- Phase 3: Output ---
        st.subheader("Data Preview")
        st.dataframe(working_df, use_container_width=True)
        
        # Download Button
        excel_data = convert_to_excel(working_df)
        foil_name = f"{os.path.splitext(uploaded_file.name)[0]}_processed.xlsx"

        st.download_button(
            label="📥 Download Final Excel File",
            data=excel_data, 
            file_name=foil_name, 
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
    except Exception as e:
        st.error(f"❌ An unexpected error occurred: {str(e)}")