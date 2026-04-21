import streamlit as st
import os
import pandas as pd
from io import BytesIO

# Import the master class we just built in core_parser.py
from core_parser import BankStatementParser

# --- Configuration ---
st.set_page_config(
    page_title="PDF Processor",
    page_icon="📄",
    layout="wide" # Set to wide to make the dataframe easier to read
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
if 'processing_status' not in st.session_state:
    st.session_state.processing_status = "idle"
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'processor_message' not in st.session_state:
    st.session_state.processor_message = None
if 'selected_bank' not in st.session_state:
    st.session_state.selected_bank = None
    
def convert_to_excel(df):
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Transactions")
    return excel_buffer.getvalue()

# --- UI: Inputs ---
col1, col2 = st.columns([1, 2])

with col1:
    selected_bank_display = st.selectbox("1. Select Bank Format", list(bank_options.keys()))
    selected_bank_code = bank_options[selected_bank_display]

with col2:
    uploaded_file = st.file_uploader(
        "2. Upload PDF :red-badge[⚠️ ONLY FILES WITHOUT PASSWORDS]", 
        type="pdf",
        accept_multiple_files=False, 
        key="file_uploader_widget"
    )

# --- Trigger Logic ---
# We reset the status if the user uploads a NEW file or changes the BANK format
if uploaded_file is not None:
    is_new_file = (
        st.session_state.uploaded_file_details is None or 
        uploaded_file.name != st.session_state.uploaded_file_details['name']
    )
    is_new_bank = (selected_bank_code != st.session_state.selected_bank)
    
    if is_new_file or is_new_bank:
        st.session_state.uploaded_file_details = {
            'name': uploaded_file.name,
            'content': uploaded_file.getvalue()
        }
        st.session_state.selected_bank = selected_bank_code
        st.session_state.processed_df = None
        st.session_state.processing_status = "processing"
        st.session_state.processor_message = None

# --- Processing Execution ---
if st.session_state.processing_status == "processing" and st.session_state.uploaded_file_details:
    file_details = st.session_state.uploaded_file_details
    temp_pdf_path = "temp_statement.pdf"
    
    # Save the file to disk temporarily for Camelot to read
    with open(temp_pdf_path, "wb") as f:
        f.write(file_details['content'])
    
    with st.spinner(f"Parsing {selected_bank_display} statement... Wait for it..."):
        try:    
            # Initialize the parser with the temp file and the selected bank code
            parser = BankStatementParser(temp_pdf_path, st.session_state.selected_bank)
            processed_df = parser.process()
        
            if isinstance(processed_df, pd.DataFrame):
                if not processed_df.empty:
                    st.session_state.processed_df = processed_df
                    st.session_state.processing_status = "completed"
                    st.session_state.processor_message = f"✅ {selected_bank_display} PDF processed successfully!"
                else:
                    st.session_state.processing_status = "failed"
                    st.session_state.processor_message = "⚠️ Processing complete, but no transaction data could be extracted. The PDF might be an image/scan or an unsupported layout."
            else:
                st.session_state.processing_status = "failed"
                st.session_state.processor_message = "**Processing Failed.** The engine did not return a valid data table."
                
        except Exception as e:
            st.session_state.processing_status = "failed"
            st.session_state.processor_message = f"❌ An unexpected error occurred during processing: {e}"
            
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)

# --- UI: Outputs ---
if st.session_state.uploaded_file_details is None:
    st.info("Please select a bank and upload a Bank Statement PDF to begin processing.")
    
elif st.session_state.processing_status == "completed":
    st.success(st.session_state.processor_message)
    
    # Display a glimpse of the data
    st.subheader("Data Preview")
    st.dataframe(st.session_state.processed_df, use_container_width=True)
    
    # Download Button
    excel_data = convert_to_excel(st.session_state.processed_df)
    foil_name = f"{os.path.splitext(st.session_state.uploaded_file_details['name'])[0]}_converted.xlsx"

    st.download_button(
        label="📥 Download Converted Excel",
        data=excel_data, 
        file_name=foil_name, 
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
        key="download_button_widget"
    )
    
elif st.session_state.processing_status == "failed":
    st.error(st.session_state.processor_message)