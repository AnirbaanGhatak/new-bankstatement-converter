# ml_engine.py
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.pipeline import make_pipeline

def get_sheet_names(excel_file):
    """Reads an uploaded Excel file just to extract the sheet names."""
    xls = pd.ExcelFile(excel_file)
    excel_file.seek(0) # Reset the file pointer so it can be read again later!
    return xls.sheet_names

def _clean_and_standardize(df):
    """Internal function to normalize column names."""
    df.columns = df.columns.astype(str).str.lower().str.strip()
    mapping = {
        'particulars': 'description',
        'narration': 'description',
        'transaction details': 'description',
        'details': 'description',
        'account head': 'category',
        'ledger head': 'category',
        'group': 'category'
    }
    return df.rename(columns=mapping)

def train_custom_model(file_sheet_pairs):
    """
    Takes a list of tuples: [(file_object, "SheetName"), ...]
    Cleans the targeted sheets and returns a trained ML model.
    """
    all_data = []
    
    for file, sheet_name in file_sheet_pairs:
        # We now read ONLY the specific sheet the user selected
        df = pd.read_excel(file, sheet_name=sheet_name)
        df = _clean_and_standardize(df)
        
        if 'description' in df.columns and 'category' in df.columns:
            df = df.dropna(subset=['description', 'category'])
            all_data.append(df[['description', 'category']])
            
    if not all_data:
        raise ValueError("Could not find valid 'Description' and 'Category' columns in the selected sheets.")
        
    master_train_df = pd.concat(all_data, ignore_index=True)
    
    model = make_pipeline(TfidfVectorizer(ngram_range=(1, 2)), LinearSVC())
    model.fit(master_train_df['description'], master_train_df['category'])
    
    return model, len(master_train_df)

def apply_categorization(target_df, trained_model):
    """Applies the AI predictions to the raw PDF data."""
    target_df['predicted_category'] = trained_model.predict(target_df['description'])
    
    target_df = target_df.rename(columns={
        'date': 'Date',
        'description': 'Description',
        'amount': 'Amount',
        'predicted_category': 'Account Head'
    })
    
    return target_df