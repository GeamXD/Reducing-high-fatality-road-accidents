#!/usr/bin/env python3
"""
Convert CSV files to Parquet format and split if larger than 50MB
Optimized for large files with chunked processing - never loads entire file into memory
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

MAX_SIZE_MB = 50
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024
CHUNK_SIZE = 100000  # Process 100k rows at a time

def get_file_size_mb(filepath):
    """Get file size in MB"""
    return os.path.getsize(filepath) / (1024 * 1024)

def convert_large_csv_chunked(csv_path, output_dir, year_filter=None):
    """Convert large CSV to Parquet using chunked processing, writing directly to split files"""
    csv_file = Path(csv_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    base_name = csv_file.stem
    print(f"\nProcessing: {csv_file.name}")
    if year_filter:
        print(f"  Filtering data from {year_filter[0]} to {year_filter[1]}")
    print(f"  Determining schema from first chunk...")
    
    # Read first chunk to establish schema
    first_chunk = pd.read_csv(csv_path, nrows=CHUNK_SIZE, low_memory=False)
    master_schema = pa.Table.from_pandas(first_chunk).schema
    print(f"  Schema established with {len(master_schema)} columns")
    print(f"  Processing in chunks and writing to split files...")
    
    output_files = []
    part_num = 1
    current_writer = None
    current_file = None
    current_size = 0
    total_rows = 0
    filtered_rows = 0
    rows_in_current_part = 0
    
    try:
        for chunk_num, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False, dtype=str), 1):
            # Apply year filter if specified
            if year_filter and 'collision_year' in chunk.columns:
                chunk['collision_year'] = pd.to_numeric(chunk['collision_year'], errors='coerce')
                original_len = len(chunk)
                chunk = chunk[(chunk['collision_year'] >= year_filter[0]) & 
                             (chunk['collision_year'] <= year_filter[1])]
                filtered_rows += (original_len - len(chunk))
                
                # Skip empty chunks
                if len(chunk) == 0:
                    continue
            
            # Convert string columns back to appropriate types based on master schema
            for i, field in enumerate(master_schema):
                col_name = field.name
                if col_name in chunk.columns:
                    # Convert based on the master schema type
                    if pa.types.is_integer(field.type):
                        chunk[col_name] = pd.to_numeric(chunk[col_name], errors='coerce').astype('Int64')
                    elif pa.types.is_floating(field.type):
                        chunk[col_name] = pd.to_numeric(chunk[col_name], errors='coerce')
                    # String types stay as-is
            
            total_rows += len(chunk)
            
            # Create new file if needed
            if current_writer is None:
                current_file = output_path / f"{base_name}_part{part_num:02d}.parquet"
                current_writer = pq.ParquetWriter(current_file, master_schema, compression='snappy')
                rows_in_current_part = 0
            
            # Write chunk to current file using master schema
            arrow_table = pa.Table.from_pandas(chunk, schema=master_schema)
            current_writer.write_table(arrow_table)
            rows_in_current_part += len(chunk)
            
            # Check if we should close this file and start a new one
            if current_file.exists():
                current_size = get_file_size_mb(current_file)
                
                # Close current file if it's approaching the size limit
                if current_size >= MAX_SIZE_MB * 0.9:  # 90% of max size
                    current_writer.close()
                    final_size = get_file_size_mb(current_file)
                    print(f"    Part {part_num}: {rows_in_current_part:,} rows, {final_size:.2f} MB -> {current_file.name}")
                    output_files.append(str(current_file))
                    
                    part_num += 1
                    current_writer = None
                    current_file = None
            
            # Progress update
            if chunk_num % 10 == 0:
                print(f"    Processed {total_rows:,} rows...")
        
        # Close the last file
        if current_writer is not None:
            current_writer.close()
            final_size = get_file_size_mb(current_file)
            print(f"    Part {part_num}: {rows_in_current_part:,} rows, {final_size:.2f} MB -> {current_file.name}")
            output_files.append(str(current_file))
        
        print(f"  Total rows processed: {total_rows:,}")
        if year_filter and filtered_rows > 0:
            print(f"  Rows filtered out: {filtered_rows:,}")
        print(f"  ✓ Saved as {len(output_files)} file(s)")
        
        return output_files
        
    except Exception as e:
        # Clean up on error
        if current_writer is not None:
            current_writer.close()
        raise e

def convert_small_csv(csv_path, output_dir):
    """Convert small CSV files (< 500MB) normally"""
    csv_file = Path(csv_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    base_name = csv_file.stem
    print(f"\nProcessing: {csv_file.name}")
    print(f"  Reading CSV...")
    
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    
    temp_parquet = output_path / f"{base_name}.parquet"
    df.to_parquet(temp_parquet, index=False, engine='pyarrow', compression='snappy')
    
    file_size_mb = get_file_size_mb(temp_parquet)
    print(f"  Parquet size: {file_size_mb:.2f} MB")
    
    if file_size_mb <= MAX_SIZE_MB:
        print(f"  ✓ Saved as single file: {temp_parquet.name}")
        return [str(temp_parquet)]
    
    # File is too large, need to split
    print(f"  File exceeds {MAX_SIZE_MB}MB, splitting...")
    os.remove(temp_parquet)
    
    num_splits = int(file_size_mb / MAX_SIZE_MB) + 1
    rows_per_split = len(df) // num_splits + 1
    
    output_files = []
    for i in range(num_splits):
        start_idx = i * rows_per_split
        end_idx = min((i + 1) * rows_per_split, len(df))
        
        if start_idx >= len(df):
            break
        
        chunk = df.iloc[start_idx:end_idx]
        output_file = output_path / f"{base_name}_part{i+1:02d}.parquet"
        chunk.to_parquet(output_file, index=False, engine='pyarrow', compression='snappy')
        
        chunk_size_mb = get_file_size_mb(output_file)
        print(f"    Part {i+1}/{num_splits}: {len(chunk):,} rows, {chunk_size_mb:.2f} MB -> {output_file.name}")
        output_files.append(str(output_file))
    
    return output_files

def main():
    """Main conversion process"""
    print("=" * 60)
    print("CSV to Parquet Conversion (Memory Optimized)")
    print("=" * 60)
    print("\nNote: 2024_prior files will be filtered to 2010-2024 only")
    
    csv_files = {
        '2025_dataset': [
            'data/2025_dataset/collision-provisional-2025.csv',
            'data/2025_dataset/vehicle-provisional-2025.csv',
            'data/2025_dataset/casualty-provisional-2025.csv'
        ],
        '2024_prior': [
            'data/2024_prior/collision-1979-2024.csv',
            'data/2024_prior/vehicle-1979-2024.csv',
            'data/2024_prior/casualty-1979-2024.csv'
        ]
    }
    
    all_output_files = []
    
    for category, files in csv_files.items():
        print(f"\n{'='*60}")
        print(f"Category: {category}")
        print(f"{'='*60}")
        
        output_dir = f"data/parquet/{category}"
        
        # Set year filter for 2024_prior data
        year_filter = (2015, 2024) if category == '2024_prior' else None
        
        for csv_file in files:
            if not os.path.exists(csv_file):
                print(f"\n⚠ Warning: {csv_file} not found, skipping...")
                continue
            
            try:
                # Check file size to determine processing method
                file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
                print(f"  CSV file size: {file_size_mb:.2f} MB")
                
                if file_size_mb > 500:  # Use chunked processing for files > 500MB
                    output_files = convert_large_csv_chunked(csv_file, output_dir, year_filter)
                else:
                    output_files = convert_small_csv(csv_file, output_dir)
                
                all_output_files.extend(output_files)
            except Exception as e:
                print(f"  ✗ Error converting {csv_file}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("Conversion Complete!")
    print("=" * 60)
    print(f"Total output files: {len(all_output_files)}")
    print(f"\nParquet files saved in: data/parquet/")
    
    # Summary by category
    print("\nFiles created:")
    for category in ['2025_dataset', '2024_prior']:
        category_files = [f for f in all_output_files if f'parquet/{category}/' in f]
        if category_files:
            print(f"\n  {category}:")
            for f in category_files:
                fname = Path(f).name
                fsize = get_file_size_mb(f)
                print(f"    - {fname} ({fsize:.2f} MB)")

if __name__ == "__main__":
    main()