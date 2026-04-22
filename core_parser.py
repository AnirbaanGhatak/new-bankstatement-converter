import camelot
import pandas as pd
import numpy as np
import re
import pdfplumber

# ==========================================
# 1. UNIVERSAL HELPER FUNCTIONS
# ==========================================

def clean_currency(value):
    """Strips commas, Cr/Dr suffixes, and converts to float."""
    if pd.isna(value) or value is None: 
        return np.nan
    cleaned_value = re.sub(r'[^\d.-]', '', str(value))
    return float(cleaned_value) if cleaned_value else np.nan

def get_tables(file_path, flav, pg="all"):
    """Generic wrapper to extract tables across all pages using Camelot."""
    tables = camelot.read_pdf(filepath=file_path, pages=pg, flavor=flav)
    df = pd.DataFrame()
    for table in tables:
        df = pd.concat([df, table.df], ignore_index=True)
    return df

# ==========================================
# 2. THE MASTER DISPATCHER CLASS
# ==========================================

class BankStatementParser:
    def __init__(self, file_path, bank_name):
        self.file_path = file_path
        self.bank_name = bank_name  # Now provided directly by the user via UI

    def process(self):
        """Routes the file to the correct parsing logic based on the explicit bank name."""
        df = pd.DataFrame()
        
        if self.bank_name == "AXIS":
            df = self._parse_axis_bank()
        elif self.bank_name == "BOB":
            df = self._parse_bob()
        elif self.bank_name == "BOM":
            df = self._parse_bom()
        elif self.bank_name == "GBM":
            df = self._parse_gbm()
        elif self.bank_name == "INDIAN":
            df = self._parse_indian()
        elif self.bank_name == "SCB":
            df = self._parse_scb()
        elif self.bank_name == "UNION":
            df = self._parse_union()
        elif self.bank_name == "YES":
            df = self._parse_yes()
            
        # --- AUTO-DETECTION ROUTING FOR MULTI-FORMAT BANKS ---
        elif self.bank_name == "CANARA":
            # Canara routing based on your earlier setup
            df = self._parse_canara() 
            
        elif self.bank_name == "KOTAK":
            # Kotak routing based on your earlier setup
            df = self._parse_kotak()
            
        elif self.bank_name == "SARASWAT":
            # Saraswat unified parser handles both formats automatically
            df = self._parse_saraswat()
            
        elif self.bank_name == "HDFC":
            df = self._parse_hdfc()
                
        elif self.bank_name == "ICICI":
            # ICICI Format Auto-Detection (Cascade)
            try:
                df = self._parse_icici_v1()
                if df.empty: raise ValueError
            except:
                try:
                    df = self._parse_icici_v2()
                    if df.empty: raise ValueError
                except:
                    # Use Wealth Management/Privilege format as the ultimate fallback
                    df = self._parse_icici_wm()
        else:
            raise ValueError(f"No parser available for format: {self.bank_name}")

        # Final Standardization Check: Ensure every output has exact standard columns
        standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
        
        if df.empty:
            return pd.DataFrame(columns=standard_cols)
            
        for col in standard_cols:
            if col not in df.columns:
                df[col] = np.nan
        
        return df[standard_cols]

    # =========================================================================
    # 3. INDIVIDUAL BANK PARSERS (THE SPECIALIST CLERKS)
    # =========================================================================
    #
    # >>>>>>>>>>> PASTE ALL 16 _parse_... METHODS WE WROTE HERE <<<<<<<<<<<
    #
    # =========================================================================


    def _parse_axis_bank(self):
            """
            Parses Axis Bank statements using Camelot's lattice method.
            Standardizes columns and removes non-transaction rows.
            """
            # 1. Extract tables using lattice (grid-based)
            df = get_tables(self.file_path, flav='lattice')

            if df.empty:
                return pd.DataFrame() 

            # 2. Map the columns based on the Axis format in the images
            # Axis format: Tran Date | Chq No | Particulars | Debit | Credit | Balance | Init. Br
            df.columns = ["Date", "Chq No", "Particulars", "Debit", "Credit", "Balance", "Init_Br"]

            # 3. Clean up empty cells
            df.replace('', np.nan, inplace=True)

            # 4. Filter out Header Rows (removes rows where 'Date' column literally says 'Tran Date')
            df = df[~df['Date'].astype(str).str.contains('Tran Date|Date', case=False, na=False)]

            # 5. Filter out non-transaction rows (Opening/Closing balances and Totals)
            # We look at the 'Particulars' column for these keywords
            unwanted_keywords = 'TRANSACTION TOTAL|CLOSING BALANCE|OPENING BALANCE'
            df = df[~df['Particulars'].astype(str).str.contains(unwanted_keywords, case=False, na=False)]

            # 6. Drop rows that are completely empty (just a safety net)
            df.dropna(how='all', inplace=True)

            # 7. Reset index for a clean dataframe
            df.reset_index(drop=True, inplace=True)

            # We return only the standard columns required by the Dispatcher, dropping 'Init_Br'
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return df[standard_cols]

    
    def _parse_bob(self):
            """
            Parses Bank of Baroda statements using the 'stream' method.
            Uses balance-differential logic to accurately sort Debits and Credits.
            """
            # 1. Extract tables using stream (text-based guessing)
            df_main = get_tables(self.file_path, flav="stream")

            if df_main.empty:
                return pd.DataFrame()

            # 2. Filter out the massive amount of header/footer junk BOB adds to every page
            unwanted_patterns = [
                'Transaction Details Page', '-----------------', 'Page Total', 
                'Grand Total:', 'Note: Cheques received', 'returning on the basis',
                'Unless the constituent', 'BANK OF BARODA', 'PALI ROAD', 
                'ADDRESS:', 'HELPLINE NO.', 'BRANCH PHONE NO.', 'MICR CODE:', 
                'A/C Number', 'Statement of account', 'DATE PARTICULARS',
                'We are committed', 'Please contact your branch', 'ABBREVIATIONS USED',
                'Pending penal charges', '\*\*\*\*END OF STATEMENT\*\*\*\*', 'As On',
                'A/C Name', 'City', 'CKYC Number', 'Tel No.', 'Nomination Flag',
                'Scheme Description', 'Joint Holders'
            ]
            
            regex_pattern = '|'.join(unwanted_patterns)
            
            # Keep only rows that DO NOT contain the unwanted patterns
            # We assume Camelot dumped the main text block into column 0
            cleaned_df = df_main[~df_main[0].astype(str).str.contains(regex_pattern, na=False, case=False)]
            cleaned_df.reset_index(drop=True, inplace=True)

            processed_rows = []
            previous_balance_numeric = None
            i = 0

            # 3. Iterate through the text lines to reconstruct the transactions
            while i < len(cleaned_df):
                # Clean up any leading numbers/spaces Camelot might have injected
                current_line = re.sub(r'^\d+\s+', '', str(cleaned_df[0].iloc[i]).strip())

                # Check if the line starts with a Date (DD-MM-YY format for BOB)
                if re.match(r'^\d{2}-\d{2}-\d{2}', current_line):
                    full_transaction_line = current_line
                    
                    # --- Multi-line Description Merger ---
                    # Check the next line to see if it's the continuation of a UPI description
                    if (i + 1) < len(cleaned_df):
                        next_line = re.sub(r'^\d+\s+', '', str(cleaned_df[0].iloc[i + 1]).strip())
                        # If the next line DOES NOT start with a date, it belongs to the current transaction
                        if not re.match(r'^\d{2}-\d{2}-\d{2}', next_line) and next_line != "":
                            parts = current_line.split()
                            # Inject the next line into the middle of the current line's particulars
                            full_transaction_line = f"{' '.join(parts[:-2])} {next_line} {' '.join(parts[-2:])}"
                            i += 1 # Skip the next line in the main loop since we absorbed it

                    # --- Parsing the reconstructed line ---
                    parts = full_transaction_line.split()
                    if len(parts) < 3:
                        i += 1
                        continue

                    date = parts[0]
                    balance_str = parts[-1]
                    
                    debit = np.nan
                    credit = np.nan
                    chq_no = ""
                    
                    # Handle the "Brought Forward" (B/F) starting row
                    if "B/F" in parts:
                        description = 'Brought Forward'
                        previous_balance_numeric = clean_currency(balance_str)
                    else:
                        amount_str = parts[-2]
                        # Everything between the Date and the Amount is the Description/Chq No
                        description = ' '.join(parts[1:-2])
                        
                        amount_numeric = clean_currency(amount_str)
                        current_balance_numeric = clean_currency(balance_str)

                        # --- Your Brilliant Balance-Differential Logic ---
                        if previous_balance_numeric is not None and current_balance_numeric is not None:
                            # If current balance is lower, money left the account (Debit)
                            if current_balance_numeric < previous_balance_numeric:
                                debit = amount_numeric
                            else:
                                credit = amount_numeric
                        
                        previous_balance_numeric = current_balance_numeric

                    processed_rows.append({
                        'Date': date,
                        'Particulars': description,
                        'Chq No': chq_no, # BOB often merges this into particulars, so we leave blank to keep columns aligned
                        'Debit': debit,
                        'Credit': credit,
                        'Balance': balance_str
                    })
                i += 1

            result_df = pd.DataFrame(processed_rows)
            
            # 4. Standardize output columns for the Dispatcher
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            for col in standard_cols:
                if col not in result_df.columns:
                    result_df[col] = np.nan
                    
            return result_df[standard_cols]

    
    def _parse_bom(self):
            """
            Parses Bank of Maharashtra statements using lattice.
            Specifically filters out header/footer tables by checking column counts,
            and cleans multi-line text blocks within cells.
            """
            # 1. We bypass the general helper here so we can inspect each table individually
            tables = camelot.read_pdf(self.file_path, pages='all', flavor='lattice')
            
            df_list = []
            for table in tables:
                # The actual transaction table has exactly 8 columns.
                # Header tables (Account details) and Footer tables (Summary) have fewer/more.
                if table.df.shape[1] == 8:
                    df_list.append(table.df)
            
            if not df_list:
                return pd.DataFrame()
                
            # Merge only the valid transaction tables
            df = pd.concat(df_list, ignore_index=True)
            
            # 2. Map the 8 columns
            df.columns = ["Sr_No", "Date", "Particulars", "Chq No", "Debit", "Credit", "Balance", "Channel"]
            
            # 3. Clean up the rows
            # Remove the header row that repeats on every page
            df = df[~df['Date'].astype(str).str.contains('Date', case=False, na=False)]
            
            # BOM uses hyphens '-' instead of blank spaces for zero amounts. We need to clear those out.
            df['Debit'] = df['Debit'].astype(str).replace('-', '', regex=False)
            df['Credit'] = df['Credit'].astype(str).replace('-', '', regex=False)
            
            # BOM Particulars wrap text onto multiple lines inside the same cell. 
            # Camelot reads these as newline characters (\n). We replace them with a space.
            df['Particulars'] = df['Particulars'].astype(str).str.replace('\n', ' ', regex=False)
            
            # Drop any rows where Date is completely empty (safeguard)
            df.replace('', np.nan, inplace=True)
            df.dropna(subset=['Date'], inplace=True)
            
            # Reset index
            df.reset_index(drop=True, inplace=True)

            # 4. Standardize output columns for the Dispatcher
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return df[standard_cols]

    
    def _parse_canara(self):
            """Routes to the correct Canara Bank parser based on the PDF format."""
            with pdfplumber.open(self.file_path) as pdf:
                first_page_text = pdf.pages[0].extract_text()
                
            # Format 1 uses "Value Date" and "Branch Code" in its headers
            if "Value Date" in first_page_text and "Branch Code" in first_page_text:
                return self._parse_canara_v1()
            else:
                # Format 2 uses "Deposits" and "Withdrawals"
                return self._parse_canara_v2()

    def _parse_canara_v1(self):
        """Canara Format 1: The Blue Grid (Lattice)"""
        df = get_tables(self.file_path, flav="lattice")
        if df.empty: return pd.DataFrame()
        
        # Format 1 Columns: Txn Date | Value Date | Cheque No. | Description | Branch Code | Debit | Credit | Balance
        # Sometimes Camelot might misread column counts if headers span, so we ensure we have 8 cols.
        if df.shape[1] == 8:
            df.columns = ["Date", "Value Date", "Chq No", "Particulars", "Branch Code", "Debit", "Credit", "Balance"]
        else:
            # Fallback if columns merge
            df.columns = [str(i) for i in range(df.shape[1])]

        # Clean headers and empty rows
        df = df[~df['Date'].astype(str).str.contains('Txn Date|Date', case=False, na=False)]
        df.replace('', np.nan, inplace=True)
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)

        standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
        return df[standard_cols]

    def _parse_canara_v2(self):
        """Canara Format 2: The Multi-Line Text (Stream)"""
        df_main = get_tables(self.file_path, flav="stream")
        if df_main.empty: return pd.DataFrame()
        
        # Clean up common header/footer junk
        unwanted = ['Date', 'Particulars', 'Deposits', 'Withdrawals', 'Balance', 'Opening Balance', 'Closing Balance', 'page', 'DISCLAIMER', 'UNLESS THE CONSTITUENT', 'BEWARE OF PHISHING']
        pattern = '|'.join(unwanted)
        df_main = df_main[~df_main[0].astype(str).str.contains(pattern, case=False, na=False)]
        df_main = df_main[~df_main[1].astype(str).str.contains(pattern, case=False, na=False)]
        df_main.replace('', np.nan, inplace=True)
        df_main.reset_index(drop=True, inplace=True)

        processed_rows = []
        temp_date = None
        temp_particulars_parts = []
        temp_deposit = np.nan
        temp_withdrawal = np.nan
        temp_balance = np.nan

        # Format 2 Columns assumed by Stream: 
        # 0: Date, 1: Particulars, 2: Deposits (Credit), 3: Withdrawals (Debit), 4: Balance
        for index, row in df_main.iterrows():
            # 1. Collect data for a transaction block
            if pd.notna(row[0]) and str(row[0]).strip() != "":
                temp_date = str(row[0]).strip()
            if pd.notna(row[3]) and str(row[3]).strip() != "":
                temp_withdrawal = clean_currency(row[3])
            if pd.notna(row[2]) and str(row[2]).strip() != "":
                temp_deposit = clean_currency(row[2])
            if pd.notna(row[4]) and str(row[4]).strip() != "":
                temp_balance = str(row[4]).strip()

            particulars_cell = str(row[1]) if pd.notna(row[1]) else ""
            
            if particulars_cell:
                # 2. Check if the line marks the end of a transaction
                if particulars_cell.strip().startswith('Chq:'):
                    cheque_no = particulars_cell.replace('Chq:', '').strip()
                    full_particulars = ' '.join(temp_particulars_parts)
                    
                    processed_rows.append({
                        'Date': temp_date,
                        'Particulars': full_particulars,
                        'Chq No': cheque_no,
                        'Debit': temp_withdrawal,   # Withdrawals column
                        'Credit': temp_deposit,     # Deposits column
                        'Balance': temp_balance
                    })
                    
                    # 3. Reset temporary variables for the next transaction block
                    temp_date = None
                    temp_particulars_parts = []
                    temp_withdrawal = np.nan
                    temp_deposit = np.nan
                    temp_balance = np.nan
                else:
                    # If not a 'Chq:' line, append text to the running description
                    temp_particulars_parts.append(particulars_cell.strip())

        result_df = pd.DataFrame(processed_rows)
        
        standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
        for col in standard_cols:
            if col not in result_df.columns:
                result_df[col] = np.nan
                
        return result_df[standard_cols]


    def _parse_hdfc(self):
        """
        Intelligently routes to HDFC V1 or V2 based on column header text.
        V1 uses "Withdrawal Amount". V2 uses "Withdrawal Amt."
        """
        try:
            with pdfplumber.open(self.file_path) as pdf:
                first_page_text = pdf.pages[0].extract_text() or ""
        except:
            first_page_text = ""

        # Check for the specific spelling used in Format 1
        if "Withdrawal Amount" in first_page_text or "Closing Balance*" in first_page_text:
            return self._parse_hdfc_v1()
        
        # Check for the specific spelling used in Format 2
        elif "Withdrawal Amt" in first_page_text or "Value Dt" in first_page_text:
            return self._parse_hdfc_v2()
            
        else:
            # Fallback if it's a weird scan: try V2 (Stream) first as it handles messy data better
            try:
                df = self._parse_hdfc_v2()
                if not df.empty and len(df) > 1: return df
            except:
                pass
            return self._parse_hdfc_v1()
    
    def _parse_hdfc_v1(self):
            """
            Parses the cleaner HDFC format using lattice.
            Columns: Date | Narration | Chq./Ref.No. | Value Date | Withdrawal | Deposit | Closing Balance*
            """
            df = get_tables(self.file_path, flav="lattice")
            if df.empty: return pd.DataFrame()

            # 1. Map to standard 7 columns
            if df.shape[1] == 7:
                df.columns = ["Date", "Particulars", "Chq No", "Value Date", "Debit", "Credit", "Balance"]
            else:
                # Fallback if Camelot misreads
                df.columns = [str(i) for i in range(df.shape[1])]

            # 2. Clean out the header row
            df = df[~df['Date'].astype(str).str.contains('Date|STATEMENT', case=False, na=False)]

            # 3. Clean multi-line text (Camelot reads wrapped text as \n)
            if 'Particulars' in df.columns:
                df['Particulars'] = df['Particulars'].astype(str).str.replace('\n', ' ', regex=False)

            # 4. Remove empty rows and resets
            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(subset=['Date'], inplace=True)
            df.reset_index(drop=True, inplace=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return df[standard_cols]

    def _parse_hdfc_v2(self):
            """
            Parses the messy, multi-line HDFC format using stream and cumsum block logic.
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            # 1. Force the dataframe to 7 columns (Stream sometimes misses empty columns)
            while df.shape[1] < 7:
                df[df.shape[1]] = np.nan
            df = df.iloc[:, :7]
            df.columns = ["Date", "Particulars", "Chq No", "Value Date", "Debit", "Credit", "Balance"]

            # 2. The Great Purge: Filter out all the repeating page headers
            unwanted_junk = [
                'Page No', 'We understand your world', 'Account Branch', 'Address', 
                'City', 'State', 'Phone', 'OD Limit', 'Currency', 'Email', 'Cust ID', 
                'Account No', 'A/C Open', 'Account Status', 'RTGS/NEFT', 'Branch Code', 
                'Account Type', 'Nomination', 'Statement of account', 'Date', 
                'STATEMENT SUMMARY', 'Opening Balance', 'HDFC BANK LIMITED', 'Generated', 
                'Closing balance', 'Contents of this', 'State account', 'Registered Office'
            ]
            pattern = '|'.join(unwanted_junk)

            # Drop rows where Date or Particulars contain the junk
            df = df[~df['Date'].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df['Particulars'].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df = df.dropna(subset=['Date', 'Particulars'], how='all')

            # 3. Establish Anchors (Rows that actually start with a Date: DD/MM/YY)
            # HDFC Format 2 uses DD/MM/YY (e.g., 02/04/24)
            date_pattern = r'^\d{2}/\d{2}/\d{2}$'
            df['is_anchor'] = df['Date'].astype(str).str.match(date_pattern)
            
            # If a row is NOT an anchor, blank out its date so cumsum can group it
            df.loc[~df['is_anchor'], 'Date'] = np.nan

            # 4. Apply your Block ID logic
            df['block_id'] = df['Date'].notna().cumsum()
            
            # Drop any floating text that appeared before the first real transaction
            df = df[df['block_id'] > 0]

            # 5. Consolidate the blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                # Join all the fragmented Particulars together
                desc_parts = group['Particulars'].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()

                return pd.Series({
                    'Date': anchor['Date'],
                    'Particulars': full_desc,
                    'Chq No': anchor['Chq No'] if pd.notna(anchor['Chq No']) else "",
                    'Debit': anchor['Debit'],
                    'Credit': anchor['Credit'],
                    'Balance': anchor['Balance']
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    
    def _parse_icici_wm(self):
            """
            Parses ICICI Wealth Management statements using stream.
            Skips summary pages, handles missing columns, and groups multi-line descriptions.
            Original Columns: DATE | MODE** | PARTICULARS | DEPOSITS | WITHDRAWALS | BALANCE
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            # 1. Stream can sometimes read varying column counts. Ensure we have at least 6.
            while df.shape[1] < 6:
                df[df.shape[1]] = np.nan
                
            # 2. The Great Purge: Clean out headers, footers, and all of Page 1
            unwanted_junk = [
                'STATEMENT SUMMARY', 'RELATIONSHIP', 'Savings Account Balance', 
                'Total Savings', 'TOTAL DEPOSITS', 'ACCOUNT DETAILS', 'ACCOUNT TYPE', 
                'Statement of Transactions', 'DATE', 'MODE\*\*', 'PARTICULARS', 
                'TOTAL', 'Account Related', 'ACCOUNT NUMBER', 'Nominee name', 
                'Sincerely', 'Team ICICI', 'system-generated', 'Page ', 'MS.'
            ]
            pattern = '|'.join(unwanted_junk)
            
            # Filter rows where any of the first three columns contain the junk keywords
            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[2].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Handle the "Stream Shift" by merging Mode (Col 1) and Particulars (Col 2)
            df['Merged_Particulars'] = df[1].fillna('').astype(str) + " " + df[2].fillna('').astype(str)
            df['Merged_Particulars'] = df['Merged_Particulars'].str.strip()

            # 4. Set up Anchor Logic (Looking for DD-MM-YYYY format in Col 0)
            date_pattern = r'^\d{2}-\d{2}-\d{4}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            # Blank out non-anchors and group blocks
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            # Drop any floating text that appeared before the first real transaction
            df = df[df['block_id'] > 0] 

            # 5. Consolidate the blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                # Join all text in our new merged particulars column
                desc_parts = group['Merged_Particulars'].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                # Map the columns: Col 3 = Deposits, Col 4 = Withdrawals, Col 5 = Balance
                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': "", # Intentionally blanked out as it doesn't exist here
                    'Debit': anchor[4],
                    'Credit': anchor[3],
                    'Balance': anchor[5]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            # 6. Final standardization
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    def _parse_icici_v1(self):
            """
            Parses the standard ICICI Bank statement (Red Headers) using lattice.
            Original Columns: Date | Particulars | Chq.No. | Withdrawals | Deposits | Autosweep | Reverse Sweep | Balance
            """
            df = get_tables(self.file_path, flav="lattice")
            if df.empty: return pd.DataFrame()

            # 1. Check if lattice perfectly grabbed all 8 columns
            if df.shape[1] >= 8:
                # Keep only the columns we care about: 0, 1, 2, 3, 4, 7
                df = df.iloc[:, [0, 1, 2, 3, 4, 7]]
                df.columns = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            else:
                # Fallback if Camelot merged columns
                df.columns = [str(i) for i in range(df.shape[1])]

            # 2. Clean out the junk rows
            unwanted_keywords = ['Date', 'Page Total', 'Legends', 'VAT/MAT/NFS', 'B/F']
            pattern = '|'.join(unwanted_keywords)
            df = df[~df['Date'].astype(str).str.contains(pattern, case=False, na=False)]
            
            # 3. Clean multi-line particulars (just in case)
            if 'Particulars' in df.columns:
                df['Particulars'] = df['Particulars'].astype(str).str.replace('\n', ' ', regex=False)

            # 4. Final cleanup
            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(subset=['Date'], inplace=True)
            df.reset_index(drop=True, inplace=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return df[standard_cols]

    def _parse_icici_v2(self):
            """
            Parses the ICICI 'Detailed Statement' format using lattice.
            Original Columns: S No. | Value Date | Transaction Date | Cheque Number | Transaction Remarks | Withdrawal | Deposit | Balance
            """
            df = get_tables(self.file_path, flav="lattice")
            if df.empty: return pd.DataFrame()

            # 1. Map columns using specific indices to match standard format
            if df.shape[1] >= 8:
                # We want: Transaction Date (2), Remarks (4), Cheque (3), Withdrawal (5), Deposit (6), Balance (7)
                df = df.iloc[:, [2, 4, 3, 5, 6, 7]]
                df.columns = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            else:
                df.columns = [str(i) for i in range(df.shape[1])]

            # 2. Clean out headers and footers
            # Drop rows where 'Date' contains the column header word
            df = df[~df['Date'].astype(str).str.contains('Transaction Date|Date', case=False, na=False)]
            
            # The legends are usually outside the grid, but if they get caught, we drop them
            df = df[~df['Particulars'].astype(str).str.contains('Legends Used|Bharat Bill', case=False, na=False)]

            # 3. Handle multi-line wrapped text (crucial for this format)
            if 'Particulars' in df.columns:
                df['Particulars'] = df['Particulars'].astype(str).str.replace('\n', ' ', regex=False)

            # 4. Final cleanup
            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(subset=['Date'], inplace=True)
            df.reset_index(drop=True, inplace=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return df[standard_cols]

    def _parse_icici_privilege(self):
            """
            Parses ICICI Privilege Banking statements using stream.
            Structurally identical to Wealth Management, but includes a 
            TDS/Interest summary table at the end that must be skipped.
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            while df.shape[1] < 6:
                df[df.shape[1]] = np.nan

            # 1. The Cutoff Switch: Stop parsing at the TDS Summary Table
            cutoff_idx = df[df.apply(lambda row: row.astype(str).str.contains('Summary of TDS', case=False).any(), axis=1)].index
            if not cutoff_idx.empty:
                df = df.loc[:cutoff_idx[0] - 1]

            # 2. Purge Headers and Account Summaries
            unwanted_junk = [
                'Summary of Accounts', 'ACCOUNT DETAILS', 'FIXED DEPOSITS', 
                'Statement of Transactions', 'DATE', 'MODE\*\*', 'PARTICULARS', 
                'DEPOSIT NO', 'TOTAL', 'Page ', 'iCICI Bank', 'khayaal aapka',
                'PRIVILEGE BANKING', 'MS.', 'MR.'
            ]
            pattern = '|'.join(unwanted_junk)

            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[2].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Merge Mode and Particulars to fix Stream shifting
            df['Merged_Particulars'] = df[1].fillna('').astype(str) + " " + df[2].fillna('').astype(str)
            df['Merged_Particulars'] = df['Merged_Particulars'].str.strip()

            # 4. Anchor Logic
            date_pattern = r'^\d{2}-\d{2}-\d{4}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            df = df[df['block_id'] > 0]

            # 5. Consolidate Blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                desc_parts = group['Merged_Particulars'].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': "", 
                    'Debit': anchor[4],
                    'Credit': anchor[3],
                    'Balance': anchor[5]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    def _parse_icici_pb(self):
            """
            Parses ICICI Private Banking statements using stream.
            Identical column structure to Wealth Management, but requires aggressive 
            filtering of the heavy Fixed Deposit tables at the top and bottom.
            """
            df = get_tables(self.file_path, flav="stream",pg="2-end")
            if df.empty: return pd.DataFrame()

            # 1. Force 6 columns
            while df.shape[1] < 6:
                df[df.shape[1]] = np.nan

            # 2. The Cutoff Switch: Stop parsing when we hit the FD tables at the end
            # This prevents FD details from bleeding into the final transaction's particulars
            cutoff_idx = df[df.apply(lambda row: row.astype(str).str.contains('Statement of Linked Fixed', case=False).any(), axis=1)].index
            if not cutoff_idx.empty:
                df = df.loc[:cutoff_idx[0] - 1]

            # 3. Purge remaining Headers, Footers, and Page 1 FD Junk
            unwanted_junk = [
                'ACCOUNT DETAILS', 'FIXED DEPOSITS', 'Statement of Transactions',
                'PRINCIPAL DEP', 'DATE', 'MODE', 'PARTICULARS', 'DEPOSIT NO',
                'Total:', 'GRAND TOTAL', 'SUB TOTAL', 'Page ', 'iCICI Bank'
            ]
            pattern = '|'.join(unwanted_junk)

            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[2].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 4. Handle the "Stream Shift" by merging Mode (Col 1) and Particulars (Col 2)
            df['Merged_Particulars'] = df[1].fillna('').astype(str) + " " + df[2].fillna('').astype(str)
            df['Merged_Particulars'] = df['Merged_Particulars'].str.strip()

            # 5. Set up Anchor Logic (Looking for DD-MM-YYYY format in Col 0)
            date_pattern = r'^\d{2}-\d{2}-\d{4}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            df = df[df['block_id'] > 0]

            # 6. Consolidate the blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                desc_parts = group['Merged_Particulars'].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': "", 
                    'Debit': anchor[4],
                    'Credit': anchor[3],
                    'Balance': anchor[5]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    
    def _parse_indian(self):
            """
            Parses Indian Bank statements using stream.
            Aggressively filters massive page headers/footers and floating balance rows,
            then groups multi-line 'Details' text blocks.
            Original Columns: Post Date | Value Date | Details | Chq.No. | Debit | Credit | Balance
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            # 1. Force 7 columns
            while df.shape[1] < 7:
                df[df.shape[1]] = np.nan
            df = df.iloc[:, :7]

            # 2. The Great Purge: Indian Bank has a LOT of junk text
            unwanted_junk = [
                'STATEMENT OF ACCOUNT', 'INDIAN BANK', 'Branch Code', 'Phone No', 'Email ID',
                'IFSC Code', 'Statement Date', 'Statement From', 'Statement Time', 'Page No',
                'Post Date', 'Value Date', 'Details', 'Chq.No.', 'Debit', 'Credit', 'Balance',
                '====', '----', 'Statement Summary', 'Dr. Count', 'Cr. Count', 
                'In Case Your Account', 'Brought Forward', 'Carried Forward', 'CLOSING BALANCE',
                'MUMBAI', 'Account No', 'Product:', 
                'Currency:', 'Int Rate', 'Limit :', 'Drawing Power:', 'Cleared Balance', 
                'Uncleared Amount', 'Nominee name', 'Ckyc ID'
            ]
            pattern = '|'.join(unwanted_junk)

            # Filter Col 0 (Dates/Headers) and Col 2 (Details)
            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[2].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Anchor Logic: Indian Bank uses DD/MM/YY (e.g., 03/04/25)
            date_pattern = r'^\d{2}/\d{2}/\d{2}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            # Drop any floating text before the first date anchor
            df = df[df['block_id'] > 0]

            # 4. Consolidate Blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                
                # The "Details" are in column 2. Join all fragments in this block.
                desc_parts = group[2].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                # Note: Indian Bank statements often leave Chq No (Col 3) blank,
                # which can sometimes cause Stream to shift Debits left. We rely on 
                # standard column placement here, but clean_currency handles the math later.
                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': anchor[3] if pd.notna(anchor[3]) else "", 
                    'Debit': anchor[4],
                    'Credit': anchor[5],
                    'Balance': anchor[6]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            # 5. Standardize Output
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    
    def _parse_kotak(self):
            """Routes to the correct Kotak Bank parser based on column headers."""
            with pdfplumber.open(self.file_path) as pdf:
                first_page_text = pdf.pages[0].extract_text()
                
            # Format 1 combines Debit/Credit into a single column
            if "DEBIT/CREDIT" in first_page_text.upper():
                return self._parse_kotak_v1()
            else:
                return self._parse_kotak_v2()

    def _parse_kotak_v1(self):
            """
            Parses Kotak Format 1 (Grey/White).
            Key logic: Splits the combined DEBIT/CREDIT column based on +/- signs.
            Original Columns: # | TRANSACTION DATE | VALUE DATE | TRANSACTION DETAILS | CHQ / REF NO. | DEBIT/CREDIT(₹) | BALANCE(₹)
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            while df.shape[1] < 7:
                df[df.shape[1]] = np.nan

            # 1. Purge Headers and Footers
            unwanted_junk = [
                'Account Statement', 'Branch', 'CRN', 'Joint holder', 'IFSC', 'MICR', 
                'TRANSACTION DATE', 'BALANCE', 'Statement generated', 'Page '
            ]
            pattern = '|'.join(unwanted_junk)

            # Look in the first few columns where this text usually lands
            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 2. Anchor Logic: Date is in Col 1 (Col 0 is the Serial Number)
            # Format: DD Mmm YYYY (e.g., 02 Apr 2024)
            date_pattern = r'^\d{2}\s[A-Za-z]{3}\s\d{4}'
            df['is_anchor'] = df[1].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 1] = np.nan
            df['block_id'] = df[1].notna().cumsum()
            
            df = df[df['block_id'] > 0]

            # 3. Consolidate Blocks & Split Debit/Credit
            def consolidate_block(group):
                anchor = group.iloc[0]
                desc_parts = group[3].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                # Grab the amount string (e.g., "-7,500.00" or "+2,017.00")
                amt_str = str(anchor[5]).strip() if pd.notna(anchor[5]) else ""
                debit, credit = np.nan, np.nan
                
                if amt_str.startswith('-'):
                    debit = amt_str.replace('-', '')
                elif amt_str.startswith('+'):
                    credit = amt_str.replace('+', '')
                else:
                    # Fallback if the sign is missing
                    debit = amt_str
                    
                return pd.Series({
                    'Date': str(anchor[1])[:11], # Slices off the "05:01 AM" time stamp below the date
                    'Particulars': full_desc,
                    'Chq No': anchor[4] if pd.notna(anchor[4]) else "",
                    'Debit': debit,
                    'Credit': credit,
                    'Balance': anchor[6]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    def _parse_kotak_v2(self):
            """
            Parses Kotak Format 2 (Red/Yellow).
            Key logic: Triggers a kill-switch the moment it sees Summary or FD tables.
            Original Columns: Date | Narration | Chq/Ref No. | Withdrawal (Dr) | Deposit (Cr) | Balance
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            while df.shape[1] < 6:
                df[df.shape[1]] = np.nan

            # 1. The Kill-Switch: Sever the DataFrame before the summary tables hit
            cutoff_keywords = 'Statement Summary|Summary Statement of Standalone|Detailed Statement'
            cutoff_idx = df[df.apply(lambda row: row.astype(str).str.contains(cutoff_keywords, case=False).any(), axis=1)].index
            if not cutoff_idx.empty:
                df = df.loc[:cutoff_idx[0] - 1]

            # 2. Purge standard headers
            unwanted_junk = [
                'Period', 'Currency', 'Home Branch', 'MICR Code', 'IFSC Code', 
                'Nominee', 'Statement of Banking Account', 'Date', 'Narration', 
                'Withdrawal', 'Deposit', 'Balance', 'Contd.', 'AP-Autopay', 
                'Kotak Mahindra Bank', 'OPENING BALANCE'
            ]
            pattern = '|'.join(unwanted_junk)

            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Anchor Logic: DD-Mmm-YY (e.g., 01-Apr-24)
            date_pattern = r'^\d{2}-[A-Za-z]{3}-\d{2}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            df = df[df['block_id'] > 0]

            # 4. Consolidate Blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                desc_parts = group[1].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': anchor[2] if pd.notna(anchor[2]) else "",
                    'Debit': anchor[3],
                    'Credit': anchor[4],
                    'Balance': anchor[5]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    
    def _parse_saraswat(self):
            """
            Parses both modern and legacy Saraswat Bank formats using stream.
            Utilizes a massive filter to destroy the heavy headers/footers, 
            and groups multi-line descriptions using block logic.
            Original Columns: Date | Particulars | Instruments | Dr Amount | Cr Amount | Total Amount
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            # 1. Force 6 columns
            while df.shape[1] < 6:
                df[df.shape[1]] = np.nan
            df = df.iloc[:, :6]

            # 2. The Great Purge: Destroys headers from both Format 1 and Format 2
            unwanted_junk = [
                'STATEMENT OF ACCOUNTS', 'BILL OF SUPPLY', 'Branch', 'Address', 'CITY', 
                'PIN CODE', 'PHONE', 'GST', 'RUN DATE', 'Account No', 'Name', 'Customer ID', 
                'Purpose Code', 'Joint Holder', 'From Date', 'To Date', 'Opening Balance', 
                'CurrentROI%', 'Expiry Date', 'Limit Effective Date', 'Total Sanction', 
                'Branch MICR', 'IFSC', 'Nomination', '15 Digit Account', 'Particulars', 
                'Instruments', 'Dr Amount', 'Cr Amount', 'Total Amount', 'PAGE', 
                'Totals / Balance', 'Closing Balance', 'END OF STATEMENT', 
                'Saraswat Co-operative', 'Period :', 'Date', 'SAC CODE', 'FAX'
            ]
            pattern = '|'.join(unwanted_junk)

            # Apply filter to Column 0 and Column 1
            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Anchor Logic: Date format DD-MM-YYYY (e.g., 01-04-2024)
            date_pattern = r'^\d{2}-\d{2}-\d{4}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            # Drop floating text before the first date
            df = df[df['block_id'] > 0]

            # 4. Consolidate Blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                
                # Join all multi-line particulars together
                desc_parts = group[1].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                # Clean the "CR" and "DR" tags off the balance so it's a pure number
                balance_str = str(anchor[5]).replace('CR', '').replace('DR', '').strip() if pd.notna(anchor[5]) else np.nan

                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': anchor[2] if pd.notna(anchor[2]) else "",
                    'Debit': anchor[3],
                    'Credit': anchor[4],
                    'Balance': balance_str
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            # 5. Standardize Output
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    
    def _parse_scb(self):
            """
            Parses Standard Chartered Bank statements using stream.
            Handles the unique 'Mmm DD' date format and flipped Deposit/Withdrawal columns.
            Original Columns: Date | Value Date | Description | Cheque | Deposit | Withdrawal | Balance
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            # 1. Force 7 columns
            while df.shape[1] < 7:
                df[df.shape[1]] = np.nan
            df = df.iloc[:, :7]

            # 2. The Great Purge: Clean out headers, footers, and page noise
            unwanted_junk = [
                'ACCOUNT STATEMENT', 'Standard Chartered', 'BRANCH', 'STATEMENT DATE', 
                'CURRENCY', 'ACCOUNT TYPE', 'ACCOUNT NO', 'NOMINEE', 'BRANCH ADDRESS', 
                'IFSC', 'MICR CODE', 'Phone No', 'Date', 'Value Date', 'Description', 
                'Cheque', 'Deposit', 'Withdrawal', 'Balance', 'Page ', 'Bank deposits are covered', 
                'Please register', 'Report irregularities', 'Total', 'M/S', 'FLAT NO', 
                'MAHARASHTRA', 'INDIA', 'Balance Brought Forward'
            ]
            pattern = '|'.join(unwanted_junk)

            # Filter the first few columns where this text usually lands
            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[2].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Anchor Logic: SCB uses Mmm DD (e.g., 'Apr 01', 'Mar 13')
            date_pattern = r'^[A-Z][a-z]{2}\s\d{2}$'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)
            
            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()
            
            # Drop any floating text before the first date anchor
            df = df[df['block_id'] > 0]

            # 4. Consolidate Blocks
            def consolidate_block(group):
                anchor = group.iloc[0]
                
                # The Description is in column 2. Join all fragments.
                desc_parts = group[2].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()
                
                return pd.Series({
                    'Date': anchor[0],
                    'Particulars': full_desc,
                    'Chq No': anchor[3] if pd.notna(anchor[3]) else "", 
                    'Debit': anchor[5],  # SCB puts Withdrawal in Column 5
                    'Credit': anchor[4], # SCB puts Deposit in Column 4
                    'Balance': anchor[6]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            # 5. Standardize Output
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]

    
    def _parse_union(self):
            """
            Parses Union Bank statements using stream.
            Handles 8 columns, merges multiple reference ID columns, and cleans time-stamped dates.
            Original Columns: Date | Remarks | Tran Id-1 | UTR Number | Instr. ID | Withdrawals | Deposits | Balance
            """
            df = get_tables(self.file_path, flav="stream")
            if df.empty: return pd.DataFrame()

            # 1. Force 8 columns
            while df.shape[1] < 8:
                df[df.shape[1]] = np.nan
            df = df.iloc[:, :8]

            # 2. The Great Purge: Clean out headers, footers, and page noise
            unwanted_junk = [
                'Statement of Account', 'Union Bank', 'Branch', 'Customer Id',
                'Account No', 'Account Currency', 'Account Type', 'MICR Code',
                'IFSC Code', 'CKYC Number', 'Statement Date', 'Statement Period',
                'Records from', 'No more records', 'Date', 'Remarks', 'Tran Id',
                'UTR Number', 'Instr. ID', 'Withdrawals', 'Deposits', 'Balance',
                'Page No', 'For any queries', 'system generated', 'TO AVAIL OUR LOAN'
            ]
            pattern = '|'.join(unwanted_junk)

            # Filter the first couple of columns
            df = df[~df[0].astype(str).str.contains(pattern, case=False, na=False)]
            df = df[~df[1].astype(str).str.contains(pattern, case=False, na=False)]

            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)

            # 3. Anchor Logic: DD-MM-YYYY (e.g., 02-04-2025)
            date_pattern = r'^\d{2}-\d{2}-\d{4}'
            df['is_anchor'] = df[0].astype(str).str.match(date_pattern)

            df.loc[~df['is_anchor'], 0] = np.nan
            df['block_id'] = df[0].notna().cumsum()

            df = df[df['block_id'] > 0]

            # 4. Consolidate Blocks
            def consolidate_block(group):
                anchor = group.iloc[0]

                # The Remarks are in column 1. Join all fragments.
                desc_parts = group[1].dropna().astype(str).tolist()
                full_desc = ' '.join(desc_parts).replace('\n', ' ').strip()

                # Union Bank has Tran Id (2), UTR (3), and Instr ID (4).
                # Combine them into the Chq No field if they exist, ignoring hyphens.
                chq_parts = []
                for col in [2, 3, 4]:
                    val = str(anchor[col]).replace('-', '').strip() if pd.notna(anchor[col]) else ""
                    if val: chq_parts.append(val)
                chq_no = ' / '.join(chq_parts)

                return pd.Series({
                    'Date': str(anchor[0])[:10], # Slices off the HH:MM:SS time stamp
                    'Particulars': full_desc,
                    'Chq No': chq_no,
                    'Debit': anchor[5],
                    'Credit': anchor[6],
                    'Balance': anchor[7]
                })

            result_df = df.groupby('block_id').apply(consolidate_block).reset_index(drop=True)

            # 5. Standardize Output
            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return result_df[standard_cols]


    def _parse_yes(self):
            """
            Parses Yes Bank statements using lattice.
            Filters out the summary tables at the end by strictly requiring 7 columns.
            Original Columns: Transaction Date | Value Date | Cheque No/ Reference No | Description | Withdrawals | Deposits | Running Balance
            """
            # We bypass the generic get_tables helper so we can filter by column count
            tables = camelot.read_pdf(self.file_path, pages='all', flavor='lattice')
            
            df_list = []
            for table in tables:
                # The transaction table has exactly 7 columns. 
                # The summary boxes at the end have 4 or fewer.
                if table.df.shape[1] == 7:
                    df_list.append(table.df)
            
            if not df_list:
                return pd.DataFrame()
                
            df = pd.concat(df_list, ignore_index=True)

            # 1. Map columns
            df.columns = ["Date", "Value Date", "Chq No", "Particulars", "Debit", "Credit", "Balance"]

            # 2. Clean out repeating header rows
            df = df[~df['Date'].astype(str).str.contains('Transaction Date|Date', case=False, na=False)]

            # 3. Clean multi-line descriptions (Camelot reads wrapped text as \n)
            df['Particulars'] = df['Particulars'].astype(str).str.replace('\n', ' ', regex=False)
            
            # 4. Remove empty rows and resets
            df.replace('', np.nan, inplace=True)
            df.replace('nan', np.nan, inplace=True)
            df.dropna(subset=['Date'], inplace=True)
            df.reset_index(drop=True, inplace=True)

            standard_cols = ["Date", "Particulars", "Chq No", "Debit", "Credit", "Balance"]
            return df[standard_cols]
