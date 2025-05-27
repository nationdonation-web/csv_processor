#!/usr/bin/env python3
from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import json
from datetime import date, datetime
import time
from tqdm import tqdm
import supabase
from supabase import create_client, Client
import io
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "CSV Processor Service is Running!"

@app.route('/process-csv', methods=['POST'])
def process_csv():
    try:
        # Get CSV data from n8n
        csv_data = request.get_data()
        csv_string = csv_data.decode('utf-8')
        csv_buffer = io.StringIO(csv_string)
        
        # ONLY CHANGE: Replace pd.read_csv('Daily.csv') with this line
        df = pd.read_csv(csv_buffer)
        
        # YOUR EXACT EXISTING CODE STARTS HERE - UNCHANGED
        df_filtered = df.copy()
        
        columns_to_clean = ['Amount', 'TotalAmount', 'Surcharge']
        df_filtered[columns_to_clean] = (df_filtered[columns_to_clean]
                                        .replace(r'[\$,]', '', regex=True)
                                        .astype(float))
        
        # Find rows where Amount, Surcharge, and TotalAmount are null/NaN
        blank_rows = df_filtered[
            df_filtered['Amount'].isna() & 
            df_filtered['Surcharge'].isna() & 
            df_filtered['TotalAmount'].isna()
        ]
        
        print(f"Found {len(blank_rows)} rows with null Amount, Surcharge, and TotalAmount:")
        print(blank_rows)
        
        sum_df = df_filtered['TotalAmount'].sum()
        print(f"Sum of Amount in df_filtered: {sum_df}")
        
        print(df_filtered.head())
        
        # Step 1: First check the current format of TransactionDatetime
        print("Current format of TransactionDatetime column:")
        print(df_filtered['TransactionDatetime'].head(5))
        print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
        
        # Step 2: Convert to datetime format
        print("\nConverting TransactionDatetime to datetime format...")
        try:
            # Try standard conversion
            df_filtered['TransactionDatetime'] = pd.to_datetime(df_filtered['TransactionDatetime'])
        except Exception as e:
            print(f"Error in standard conversion: {str(e)}")
            
            # Try with error handling
            print("Attempting conversion with errors='coerce'...")
            df_filtered['TransactionDatetime'] = pd.to_datetime(df_filtered['TransactionDatetime'], errors='coerce')
            
            # Check for NaT values created during conversion
            nat_count = df_filtered['TransactionDatetime'].isna().sum()
            if nat_count > 0:
                print(f"Warning: {nat_count} values couldn't be converted to datetime and were set to NaT")
                
                # Show some examples of values that couldn't be converted
                original_values = df_filtered['TransactionDatetime'].copy()
                problem_indices = df_filtered[df_filtered['TransactionDatetime'].isna()].index
                if len(problem_indices) > 0:
                    print("\nSample of problematic values:")
                    for idx in problem_indices[:5]:  # Show up to 5 examples
                        print(f"Index {idx}: '{original_values[idx]}'")
        
        # Step 3: Check the conversion results
        print("\nAfter datetime conversion:")
        print(df_filtered['TransactionDatetime'].head(5))
        print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
        print(f"NaT values: {df_filtered['TransactionDatetime'].isna().sum()} out of {len(df_filtered)}")
        
        # Step 4: Now you can use .dt accessor to convert to date
        if pd.api.types.is_datetime64_dtype(df_filtered['TransactionDatetime']):
            print("\nConverting datetime to date...")
            df_filtered['TransactionDatetime'] = df_filtered['TransactionDatetime'].dt.date
            
            # Step 5: Check the final date format
            print("\nFinal date format:")
            print(df_filtered['TransactionDatetime'].head(5))
            print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
        else:
            print("\nWARNING: TransactionDatetime is still not a datetime type after conversion attempts.")
            print("Current data type:", df_filtered['TransactionDatetime'].dtype)
            print("Sample values:", df_filtered['TransactionDatetime'].head(5).tolist())
        
        url = "https://thxvfnachnpgmeottlem.supabase.co"
        key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRoeHZmbmFjaG5wZ21lb3R0bGVtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Njg2ODQ1OSwiZXhwIjoyMDYyNDQ0NDU5fQ.TBgZdtH3INLZtpnraa4dfPbZ0hZHLdCoY1VKhqEv8FA"
        
        supabase = create_client(url, key,)
        
        # Replace NaN values with None
        df_filtered = df_filtered.replace({np.nan: None})
        
        # Custom JSON encoder to handle NaN values
        class NanHandlingEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, float) and np.isnan(obj):
                    return None
                if isinstance(obj, (datetime, date, pd.Timestamp)):
                    return obj.isoformat()
                return super().default(obj)
        
        def upload_dataframe_in_chunks(df, table_name, chunk_size=20000):
            total_rows = len(df)
            chunks = range(0, total_rows, chunk_size)
            successful_rows = 0
            failed_chunks = []
        
            print(f"Uploading {total_rows} rows to {table_name} in chunks of {chunk_size}")
        
            with tqdm(total=total_rows) as pbar:
                for i in chunks:
                    end_idx = min(i + chunk_size, total_rows)
                    chunk = df.iloc[i:end_idx]
        
                    try:
                        # Convert chunk to JSON records
                        chunk_records = json.loads(json.dumps(chunk.to_dict('records'), cls=NanHandlingEncoder))
                        response = supabase.table(table_name).insert(chunk_records).execute()
        
                        # Count successful rows
                        successful_rows += len(response.data if hasattr(response, 'data') else chunk_records)
                        print(f"Successfully uploaded chunk {i} to {end_idx-1}")
        
                    except Exception as e:
                        print(f"Error uploading chunk {i} to {end_idx-1}: {e}")
                        failed_chunks.append((i, end_idx))
                    pbar.update(len(chunk))
        
            return {"total_rows": total_rows, "successful_rows": successful_rows, "failed_chunks": failed_chunks}
        
        # Execute the upload
        table_name = "N8N_Test"
        result = upload_dataframe_in_chunks(df_filtered, table_name, chunk_size=20000)
        
        # Retry failed chunks with smaller chunk size and collect permanently failed data
        permanently_failed_data = []
        
        if result['failed_chunks']:
            print("Retrying failed chunks with smaller chunk size...")
            for start_idx, end_idx in result['failed_chunks']:
                failed_chunk = df_filtered.iloc[start_idx:end_idx]
                retry_result = upload_dataframe_in_chunks(failed_chunk, table_name, chunk_size=10000)
                print(f"Retry result: {retry_result['successful_rows']} of {retry_result['total_rows']} rows uploaded successfully")
                
                # If there are still failed chunks after retry, collect the data
                if retry_result['failed_chunks']:
                    for retry_start, retry_end in retry_result['failed_chunks']:
                        # Calculate actual indices in the original dataframe
                        actual_start = start_idx + retry_start
                        actual_end = start_idx + retry_end
                        permanently_failed_chunk = df_filtered.iloc[actual_start:actual_end]
                        permanently_failed_data.append(permanently_failed_chunk)
        
        # Save permanently failed data to CSV
        if permanently_failed_data:
            permanently_failed_df = pd.concat(permanently_failed_data, ignore_index=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            failed_filename = f"failed_upload_data_{table_name}_{timestamp}.csv"
            permanently_failed_df.to_csv(failed_filename, index=False)
            print(f"⚠️ {len(permanently_failed_df)} rows failed permanently and saved to: {failed_filename}")
        else:
            print("✅ All chunks uploaded successfully!")
        # YOUR EXACT EXISTING CODE ENDS HERE
        
        return jsonify({"status": "success", "message": "CSV processed successfully"}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
