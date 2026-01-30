import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import re

def clean_column_name(col):
    col = col.strip().lower()
    col = re.sub(r'[^a-z0-9_]', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    return col

def build_brick():
    print("Reading dataset...")
    # Try reading as csv first, if fails or single column, try tab
    try:
        df = pd.read_csv("download/dataset.csv")
        if len(df.columns) < 2:
            print("CSV read resulted in single column, trying tab delimiter...")
            df = pd.read_csv("download/dataset.csv", sep='\t')
    except Exception as e:
        print(f"Error reading as CSV: {e}")
        print("Trying tab delimiter...")
        df = pd.read_csv("download/dataset.csv", sep='\t')
    
    # Rename critical columns first if they exist
    # Based on paper/dataset structure
    if 'Solubility' in df.columns:
        df = df.rename(columns={'Solubility': 'solubility_log_s'})
    
    # Clean all column names
    df.columns = [clean_column_name(c) for c in df.columns]
    
    print("Columns:", df.columns.tolist())
    
    # Ensure smiles is present
    if 'smiles' not in df.columns:
        raise ValueError("No SMILES column found")
        
    # Convert types
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype('float32')
        elif df[col].dtype == 'int64':
            df[col] = df[col].astype('int32')
            
    # Convert to parquet
    os.makedirs("brick", exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "brick/aqsoldb.parquet")
    print(f"Brick built: brick/aqsoldb.parquet with {len(df)} records")

if __name__ == "__main__":
    build_brick()
