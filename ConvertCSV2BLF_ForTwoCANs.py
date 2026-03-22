import can
import pandas as pd

import datetime
import os
import time
import warnings
try:
    import dask.dataframe as dd
    import dask
    from dask.diagnostics import ProgressBar
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

# --- Configuration ---
# DEBUG = False # Removed global flag

#for STBTL CAN data convert, combine two csv files into one blf file, the two csv files are from PMZ and Debug CAN respectively recoreded in 
#dSpace Control Desk as csv files. this code has been tested and works well.
#updated by xge1 on 06012025
#convert dspace Control Desk CAN recorder csv to lbf format, can be read by CANalyzer


def read_csv_header(input_fp, debug=False):
    """Read only the header part of the CSV to extract date, time and column info."""
    # Read just the first few rows to get the header information
    header_df = pd.read_csv(input_fp, nrows=5, low_memory=False)
    if debug:
        print("I'm in header reading now")
        print("Header DataFrame sample:\n", header_df.head())
    
    date_cell = header_df.iloc[2, 3]  # get date cell
    time_cell = header_df.iloc[2, 2]  # get time cell    
    str_datetime = date_cell + " " + time_cell  # combine date and time cell
    timestamp = datetime.datetime.strptime(
        str_datetime, '%d-%b-%y %H:%M:%S:%f'
    ).timestamp()
    
    # Extract column names
    
    columns = header_df.iloc[0].tolist()
    if debug:
        print("Extracted header columns:", columns)
        print(f"Extracted header timestamp: {timestamp}")
    
    
    return timestamp, columns


def read_csv_in_chunks(input_fp, source, chunksize=100000):
    """Read and process the CSV file in chunks to reduce memory usage.
    
    Returns a generator that yields processed chunks to minimize memory usage.
    """
    # Time tracking
    csv_read_start = time.time()
    processing_start = None
    chunks_processed = 0
    rows_processed = 0
    last_report_time = csv_read_start
    
    # Get the initial timestamp and column names
    initial_timestamp, columns = read_csv_header(input_fp)
    print(f"Header reading took {time.time() - csv_read_start:.2f} seconds")
    
    # Initialize variable to track the first timestamp
    first_timestamp = None
    
    # Read the file in chunks
    for chunk_idx, chunk in enumerate(pd.read_csv(input_fp, chunksize=chunksize, low_memory=False,skiprows=4)):
        if processing_start is None:
            processing_start = time.time()
            print(f"First chunk loading took {processing_start - csv_read_start:.2f} seconds")
        
        chunk_start_time = time.time()
        
        # Skip the header rows for all chunks
        #chunk = chunk[chunk["NumMessages"] != "EVENT"].iloc[:, :-2]
        chunk.columns = columns[:-4]
        

        
        # Skip if empty after filtering
        if chunk.empty:
            continue
            
        # Convert data types
        chunk["MsgID"] = chunk["MsgID"].astype(int)#.apply(hex)
        chunk["SyncTime"] = chunk["SyncTime"].astype(float)
        
        # Record the first timestamp from the first valid chunk
        if first_timestamp is None and not chunk.empty:
            first_timestamp = chunk["SyncTime"].iloc[0]
            # After finding first timestamp, we can start adjusting all timestamps
            offset = initial_timestamp - first_timestamp
        else:
            # Use the already calculated offset for later chunks
            offset = initial_timestamp - first_timestamp if first_timestamp is not None else 0
        
        # Adjust timestamps immediately for this chunk
        if not chunk.empty and first_timestamp is not None:
            chunk["SyncTime"] = chunk["SyncTime"] + offset
        
        # Add required fields
        chunk["dlc"] = 8
        chunk["source"] = source
        
        # Update counters
        chunks_processed += 1
        rows_processed += len(chunk)
        
        # Report progress periodically (every 5 chunks or 3 seconds)
        current_time = time.time()
        if chunks_processed % 5 == 0 and current_time - last_report_time >= 3.0:
            elapsed = current_time - processing_start
            rows_per_sec = rows_processed / elapsed if elapsed > 0 else 0
            print(f"File {source}: Processed {chunks_processed} chunks " +
                  f"({rows_processed} rows, {rows_per_sec:.1f} rows/sec)")
            last_report_time = current_time
        
        chunk_processing_time = time.time() - chunk_start_time
        if chunk_idx == 0:
            print(f"First chunk processing took {chunk_processing_time:.2f} seconds")
        
        # Yield the processed chunk
        yield chunk


def read_csv_with_dask(input_fp, source, chunksize=100000, num_workers=None, debug=False):
    """Read and process the CSV file using Dask for parallel processing.
    
    This function carefully preserves the original header handling logic
    while enabling multi-core processing for the data portion.
    
    Args:
        input_fp: Path to the CSV file
        source: Source identifier (0 for PMZ, 1 for Debug)
        chunksize: Size of chunks to process at once
        num_workers: Number of worker processes (None=auto)
        debug: Whether to enable debug mode
        
    Returns:
        Processed DataFrame ready for BLF conversion
    """
    if not DASK_AVAILABLE:
        print("Dask not available. Falling back to single-core processing.")
        return pd.concat(list(read_csv_in_chunks(input_fp, source, chunksize)))
    
    # Time tracking
    dask_start = time.time()
    print(f"Starting Dask processing for file {os.path.basename(input_fp)}")
    
    # CRITICAL: We need to handle the special header format with standard pandas first
    # Get the initial timestamp and column names using the original logic
    t_header_start = time.time()
    initial_timestamp, columns = read_csv_header(input_fp, debug=debug)
    if debug:
        print(f"Header reading took {time.time() - t_header_start:.2f} seconds")
        print(f"Header Timestamp: {initial_timestamp}")
    

    # --- Get first data timestamp reliably using pandas ---
    first_timestamp = None
    offset = 0.0 # Default offset
    try:
        if debug:
            print("Reading initial chunk with pandas to find first timestamp...")
        # Read first ~1000 data rows after header
        # Adjust chunksize if necessary, nrows limits the read
        df_first_chunk = pd.read_csv(
            input_fp,
            skiprows=4,
            nrows=1000, # Read a reasonable number of rows to find the first data point
            header=None, # No header in the data rows part
            low_memory=False,
            dtype='object' # Read as object initially
        )

        if not df_first_chunk.empty:
            # Assign column names carefully
            num_cols_read = len(df_first_chunk.columns)
            cols_to_assign = columns[:num_cols_read]
            df_first_chunk.columns = cols_to_assign

            # Find the index of SyncTime column
            if 'SyncTime' in df_first_chunk.columns:
                 # Filter for valid numeric-like SyncTime entries
                df_first_chunk = df_first_chunk[pd.to_numeric(df_first_chunk['SyncTime'], errors='coerce').notna()]

                if not df_first_chunk.empty:
                    # Convert the first valid SyncTime to float
                    first_timestamp_val = pd.to_numeric(df_first_chunk['SyncTime'].iloc[0], errors='coerce')
                    if not pd.isna(first_timestamp_val):
                        first_timestamp = float(first_timestamp_val)
                        if debug:
                            print(f"First valid data timestamp found: {first_timestamp}")
                        offset = initial_timestamp - first_timestamp
                    else:
                        if debug:
                            print("Warning: First SyncTime value in initial chunk was not numeric.")
                else:
                     if debug:
                        print("Warning: No valid numeric SyncTime found in the first 1000 data rows after filtering.") # Added filtering detail
            else:
                 if debug:
                    print("Warning: 'SyncTime' column not found after assigning header names to initial chunk.")
        else:
             if debug:
                print("Warning: Initial pandas read chunk was empty.")

    except Exception as e:
        print(f"Error reading initial chunk to find first timestamp: {e}")
        # Fallback: offset remains 0.0

    if debug:
        print(f"Using Timestamp offset: {offset}")
    # --- End of finding first timestamp ---

    # Get number of rows to skip (header rows)
    # We assume the first few rows contain the metadata based on your original code
    header_rows = 4
    
    # Count total rows (we need this to track progress)
    with open(input_fp, 'r') as f:
        total_rows = sum(1 for _ in f)
    if debug:
        print(f"Total rows in file: {total_rows}")
    
    # Skip the complex header when reading with Dask
    dask_read_start = time.time()
    if debug:
        print("Starting Dask DataFrame read with dtype='object'...")
    
    # Configure Dask
    if num_workers is None:
        import multiprocessing
        num_workers = max(1, multiprocessing.cpu_count() - 1)
    
    dask.config.set(scheduler='processes', num_workers=num_workers)
    print(f"Using {num_workers} worker processes")
    
    # Read with Dask, skipping the header rows and treating all as objects initially
    ddf = dd.read_csv(
        input_fp,
        blocksize=chunksize * 200,
        skiprows=header_rows,
        header=None,  # Add this: Don't infer header after skipping
        dtype='object', # Add this: Read all columns as strings first
        assume_missing=True,
        # sample=10000, # Can remove or keep, less critical with dtype='object'
        low_memory=False,
        on_bad_lines='warn' # Changed from skip to warn to see problematic lines
    )
    
    if debug:
        print(f"Dask read initialization took {time.time() - dask_read_start:.2f} seconds")
        print("Beginning parallel processing...")

    # Define the processing functions for Dask
    def process_partition(df):
        if df.empty:
            # Define expected final columns here based on meta_df logic below
            final_cols_list = ['SyncTime', 'MsgID']
            if 'Data' in columns: final_cols_list.append('Data')
            final_cols_list.extend(['dlc', 'source'])
            return pd.DataFrame(columns=final_cols_list) # Use expected final columns

        # Assign column names based on the header reading, matching the number of columns read
        num_cols_read = len(df.columns)
        # Ensure we don't try to assign more names than columns read
        cols_to_assign = columns[:num_cols_read]
        df.columns = cols_to_assign
        # print(f"Assigned columns: {cols_to_assign}")
        # print(f"Partition shape after col assign: {df.shape}")
        # print(df.head(3)) # Keep for debugging if needed

        # --- Filtering Step ---
        initial_rows = len(df)
        if "MsgID" in df.columns:
            # Keep rows where MsgID looks like a number (integer or hex)
            # Use regex that allows optional '0x' prefix for hex later
            df = df[df["MsgID"].astype(str).str.match(r'^(0x)?[0-9a-fA-F]+$', na=False)]
            # Explicitly remove 'EVENT' or other known non-ID strings if the regex isn't enough
            df = df[~df["MsgID"].astype(str).isin(["EVENT", "NumMessages", "MsgID"])]

        if "SyncTime" in df.columns:
            # Filter based on SyncTime looking numeric before conversion attempt
            df = df[pd.to_numeric(df["SyncTime"], errors='coerce').notna()]

        # print(f"Partition shape after filtering: {df.shape} (removed {initial_rows - len(df)} rows)")

        if df.empty:
            final_cols_list = ['SyncTime', 'MsgID']
            if 'Data' in columns: final_cols_list.append('Data')
            final_cols_list.extend(['dlc', 'source'])
            return pd.DataFrame(columns=final_cols_list)

        # --- Type Conversion Step (Robust) ---
        # print("Converting data types...")
        try:
            # Convert MsgID - handle potential hex strings if needed, otherwise assume decimal int
            # If MsgID can be hex (e.g., '0xabc' or 'ABC'), handle it:
            # df["MsgID"] = df["MsgID"].apply(lambda x: int(str(x), 0) if pd.notna(x) else pd.NA) # int(x, 0) handles '0x' prefix
            # If MsgID is always decimal in the data rows:
            df["MsgID"] = pd.to_numeric(df["MsgID"], errors='coerce')

            df["SyncTime"] = pd.to_numeric(df["SyncTime"], errors='coerce')

            # Handle potential NaNs from coercion
            df.dropna(subset=["MsgID", "SyncTime"], inplace=True)

            # Convert MsgID to integer after potential NaNs are handled
            df["MsgID"] = df["MsgID"].astype(int) # No hex conversion here, do it in process_can_messages if needed

            # print("Type conversion successful.")

        except Exception as e:
            print(f"Error during type conversion in partition: {e}")
            final_cols_list = ['SyncTime', 'MsgID']
            if 'Data' in columns: final_cols_list.append('Data')
            final_cols_list.extend(['dlc', 'source'])
            return pd.DataFrame(columns=final_cols_list)


        if df.empty:
             final_cols_list = ['SyncTime', 'MsgID']
             if 'Data' in columns: final_cols_list.append('Data')
             final_cols_list.extend(['dlc', 'source'])
             return pd.DataFrame(columns=final_cols_list)

        # Adjust timestamps
        df["SyncTime"] = df["SyncTime"] + offset

        # Add required fields
        df["dlc"] = 8 # Assuming fixed DLC
        df["source"] = source

        # Select and reorder columns to match the expected output schema
        final_cols = ['SyncTime', 'MsgID']
        if 'Data' in columns and 'Data' in df.columns: # Check if 'Data' exists in both header and current df
             final_cols.append('Data')
        final_cols.extend(['dlc', 'source'])

        # Ensure only existing columns are selected
        df = df[[col for col in final_cols if col in df.columns]]

        # print(f"Processed partition shape: {df.shape}, Columns: {df.columns.tolist()}")
        return df

    # --- Prepare metadata for map_partitions ---
    # Create meta DataFrame with the FINAL expected columns and dtypes AFTER processing
    meta_df_dict = {
        'SyncTime': 'float64',
        'MsgID': 'int64', # Use int64 for safety
        'dlc': 'int8',
        'source': 'int8'
    }
    # Add the 'Data' column if it exists in the original columns list
    if 'Data' in columns:
         meta_df_dict['Data'] = 'object' # Keep as object/string

    # Ensure the order matches the expected output of process_partition
    meta_cols_ordered = ['SyncTime', 'MsgID']
    if 'Data' in meta_df_dict: meta_cols_ordered.append('Data')
    meta_cols_ordered.extend(['dlc', 'source'])

    meta_df = pd.DataFrame(columns=meta_cols_ordered).astype(meta_df_dict)
    meta_df = meta_df[meta_cols_ordered] # Enforce column order


    # Apply the processing function
    if debug:
        print("Processing dataframe partitions in parallel...")
    transformed_ddf = ddf.map_partitions(process_partition, meta=meta_df) # Use the new meta_df

    # Compute the result
    with ProgressBar():
        if debug:
            print("Computing Dask result...")
        result = transformed_ddf.compute()

    if debug:
        print(f"Dask processing complete: {len(result)} rows processed")
        dask_processing_time = time.time() - dask_start # Use overall dask start
        print(f"Total Dask processing time (read+compute): {dask_processing_time:.2f} seconds")
        if dask_processing_time > 0:
            print(f"Dask processing rate: {len(result) / dask_processing_time:.1f} rows/second")

    return result


def process_can_messages(blf_writer, chunk):
    """Process CAN messages from a chunk and write to BLF file."""
    for _, row in chunk.iterrows():
        bytess = row["Data"].split("-")
        list_new = [int(x, 16) for x in bytess]
        new_msg = can.Message(
            timestamp=row['SyncTime'],
            arbitration_id=int(row['MsgID'], 16),
            dlc=row['dlc'],
            data=list_new,
            is_extended_id=False,
            channel=int(row['source'])
        )
        
        blf_writer.on_message_received(new_msg)
        
    return len(chunk)

def stream_merge_sorted(generator1, generator2, generator3=None, key='SyncTime', buffer_size=10000): # <-- ADDED generator3
    """Merge up to three generators of DataFrames keeping them sorted by the key.
    Uses a buffer to manage the merge while keeping memory usage low.
    """
    buffer = pd.DataFrame()  # Working buffer
    processed_rows = 0
    merge_start_time = time.time()
    last_progress_time = merge_start_time
    
    # Get initial chunks
    try: chunk1 = next(generator1); has_more1 = True
    except StopIteration: chunk1 = pd.DataFrame(); has_more1 = False
        
    try: chunk2 = next(generator2); has_more2 = True
    except StopIteration: chunk2 = pd.DataFrame(); has_more2 = False

    # <-- ADDED FOR 3-CAN MODE: Initial chunk for generator3 -->
    has_more3 = False
    chunk3 = pd.DataFrame()
    if generator3:
        try: chunk3 = next(generator3); has_more3 = True
        except StopIteration: chunk3 = pd.DataFrame(); has_more3 = False
    
    # While we have data to process
    while has_more1 or has_more2 or has_more3 or not buffer.empty: # <-- ADDED has_more3
        # Add available chunks to buffer
        frames_to_concat = [buffer]
        if has_more1: frames_to_concat.append(chunk1); has_more1 = False; chunk1 = pd.DataFrame()
        if has_more2: frames_to_concat.append(chunk2); has_more2 = False; chunk2 = pd.DataFrame()
        if has_more3: frames_to_concat.append(chunk3); has_more3 = False; chunk3 = pd.DataFrame() # <-- ADDED has_more3
            
        buffer = pd.concat(frames_to_concat, ignore_index=True)
        
        # If buffer reached the threshold or we have no more data coming, sort and yield
        if len(buffer) >= buffer_size or (not has_more1 and not has_more2 and not has_more3): # <-- ADDED not has_more3
            # Sort buffer
            sort_start = time.time()
            buffer = buffer.sort_values(by=key).reset_index(drop=True)
            sort_time = time.time() - sort_start
            
            # If we have more data coming, only yield a portion
            if has_more1 or has_more2 or has_more3: # <-- ADDED has_more3
                # Keep some rows in buffer to ensure proper time ordering
                yield_size = max(1, len(buffer) - buffer_size//4)
                yield_df = buffer.iloc[:yield_size].copy()
                buffer = buffer.iloc[yield_size:].reset_index(drop=True)
                yield yield_df
            else:
                # No more data coming, yield everything
                yield buffer
                buffer = pd.DataFrame()
        
        # Get next chunks if needed
        if not has_more1 and chunk1.empty:
            try: chunk1 = next(generator1); has_more1 = True
            except StopIteration: pass
                
        if not has_more2 and chunk2.empty:
            try: chunk2 = next(generator2); has_more2 = True
            except StopIteration: pass

        # <-- ADDED FOR 3-CAN MODE: Next chunk for generator3 -->
        if generator3 and not has_more3 and chunk3.empty:
            try: chunk3 = next(generator3); has_more3 = True
            except StopIteration: pass
        
        processed_rows += 1
        # Print progress every 10 batches with timing
        if processed_rows % 10 == 0:
            current_time = time.time()
            if current_time - last_progress_time >= 3.0:  # Only update every 3 seconds
                elapsed = current_time - merge_start_time
                batches_per_sec = processed_rows / elapsed
                print(f"Merge progress: {processed_rows} batches processed " +
                      f"({batches_per_sec:.2f} batches/sec), buffer size: {len(buffer)}")
                last_progress_time = current_time


def write_blf_in_chunks(pmz_fp, debug_fp, output_fp, chunk_size=100000, buffer_size=50000, log_callback=None,sdu_fp=None):
    """Write BLF file by streaming data in chunks to reduce memory usage.
    
    Uses generators and streaming merge to minimize memory consumption.
    """
    # Start timing
    time_start = time.time()
    
    print(f"Processing PMZ file: {pmz_fp}")
    t_pmz_start = time.time()
    pmz_generator = read_csv_in_chunks(pmz_fp, source=0, chunksize=chunk_size)
    
    print(f"Processing Debug file: {debug_fp}")
    t_debug_start = time.time()
    debug_generator = read_csv_in_chunks(debug_fp, source=1, chunksize=chunk_size)
    # <-- ADDED FOR 3-CAN MODE: SDU generator -->
    sdu_generator = None
    if sdu_fp and os.path.exists(sdu_fp):
        print(f"Processing SDU file: {sdu_fp}")
        sdu_generator = read_csv_in_chunks(sdu_fp, source=2, chunksize=chunk_size)
    t_merge_start = time.time()
    print(f"Writing to BLF file: {output_fp} (streaming mode)")
    blf_f = can.BLFWriter(output_fp)
    
    # Track progress
    total_processed = 0
    last_time = time_start
    
    # Stream through merged data
    for i, merged_chunk in enumerate(stream_merge_sorted(pmz_generator, debug_generator, generator3=sdu_generator, buffer_size=buffer_size)):
        chunk_size = process_can_messages(blf_f, merged_chunk)
        total_processed += chunk_size
        
        # Print progress every 5 batches with timing info
        if i % 5 == 0:
            current_time = time.time()
            elapsed_since_last = current_time - last_time
            msgs_per_second = chunk_size * 5 / elapsed_since_last if i > 0 else 0
            print(f"Progress: processed {total_processed} messages so far " +
                  f"({msgs_per_second:.1f} msgs/sec)")
            last_time = current_time
            
    t_end = time.time()
    blf_f.stop()
    
    # Calculate timing for each stage
    pmz_time = t_debug_start - t_pmz_start
    debug_time = t_merge_start - t_debug_start
    processing_time = t_end - t_merge_start
    total_time = t_end - time_start
    
    # Create timing report
    timing_report = f"\nTiming Report:\n" + \
                    f"PMZ file initialization: {pmz_time:.2f} seconds\n" + \
                    f"Debug file initialization: {debug_time:.2f} seconds\n" + \
                    f"Data processing and BLF writing: {processing_time:.2f} seconds\n" + \
                    f"Total execution time: {total_time:.2f} seconds\n" + \
                    f"Processing speed: {total_processed/total_time:.1f} messages/second"
    
    completion_message = f"Conversion Done! Total processed messages: {total_processed}" + timing_report
    print(completion_message)
    
    # If a log callback is provided, call it with the completion message
    if log_callback:
        log_callback(completion_message)
        
    return True


# Helper function for data conversion (with error handling)
def hex_string_to_bytes(hex_str):
    if not isinstance(hex_str, str):
        return [] # Return empty list for non-string input
    byte_list = []
    for part in hex_str.split('-'):
        part_stripped = part.strip()
        if part_stripped:
            try:
                byte_list.append(int(part_stripped, 16))
            except ValueError:
                # Log or handle bad hex parts if necessary, here we skip them
                # print(f"Warning: Skipping invalid hex part '{part_stripped}' in data '{hex_str}'")
                pass # Or append a default like 0
    return byte_list # Or return tuple(byte_list)


def write_blf_optimized(pmz_fp, debug_fp, output_fp, chunk_size=100000, 
                     num_workers=None, use_dask=True, log_callback=None, debug=False, sdu_fp=None):
    """Write BLF file using Dask for multi-core processing while preserving custom header handling.
    
    This function carefully preserves the original header handling logic
    while enabling multi-core processing for the data portion.
    
    Args:
        pmz_fp: Path to the PMZ CSV file
        debug_fp: Path to the Debug CSV file
        output_fp: Path to the output BLF file
        chunk_size: Size of chunks to process at once
        num_workers: Number of worker processes (None=auto)
        use_dask: Whether to use Dask for multi-core processing
        log_callback: Callback function for logging
        debug: Whether to enable debug mode
        
    Returns:
        True if successful
    """
    time_start = time.time()

    if not DASK_AVAILABLE:
        use_dask = False
        print("Dask not available. Using single-core processing.")

    if not use_dask:
        # Fall back to original generator-based approach
        # <-- MODIFIED: Added sdu_fp=sdu_fp to the fallback function -->
        return write_blf_in_chunks(pmz_fp, debug_fp, output_fp, 
                                  chunk_size, log_callback=log_callback, sdu_fp=sdu_fp)
    
    print(f"Processing PMZ file with Dask: {pmz_fp}")
    t_pmz_start = time.time()
    pmz_data = read_csv_with_dask(pmz_fp, source=0, 
                                 chunksize=chunk_size, 
                                 num_workers=num_workers,
                                 debug=debug)
    t_pmz_end = time.time()
    if debug:
        print(f"PMZ processing complete: {t_pmz_end - t_pmz_start:.2f} seconds")
        print(f"PMZ rows: {len(pmz_data)}")
    
    print(f"Processing Debug file with Dask: {debug_fp}")
    t_debug_start = time.time()
    debug_data = read_csv_with_dask(debug_fp, source=1, 
                                   chunksize=chunk_size, 
                                   num_workers=num_workers,
                                   debug=debug)
    t_debug_end = time.time()
    # <-- ADDED FOR 3-CAN MODE: SDU processing -->
    data_frames_to_merge = [pmz_data, debug_data]
    sdu_rows_processed = 0
    t_sdu_start, t_sdu_end = 0, 0

    if debug:
        print(f"Debug processing complete: {t_debug_end - t_debug_start:.2f} seconds")
        print(f"Debug rows: {len(debug_data)}")

    if sdu_fp and os.path.exists(sdu_fp):
        print(f"Processing SDU file with Dask: {sdu_fp}")
        t_sdu_start = time.time()
        sdu_data = read_csv_with_dask(sdu_fp, source=2, # Source 2 for SDU channel
                                       chunksize=chunk_size, 
                                       num_workers=num_workers,
                                       debug=debug)
        t_sdu_end = time.time()
        sdu_rows_processed = len(sdu_data)
        data_frames_to_merge.append(sdu_data)
    
    print("Combining and sorting data...")
    t_merge_start = time.time()
    #combined_data = pd.concat([pmz_data, debug_data]).sort_values(by="SyncTime").reset_index(drop=True)
    combined_data = pd.concat(data_frames_to_merge).sort_values(by="SyncTime").reset_index(drop=True)
    t_merge_end = time.time()
    if debug:
        print(f"Merge complete: {t_merge_end - t_merge_start:.2f} seconds")
        print(f"Total rows: {len(combined_data)}")

    # --- Start Pre-processing ---
    print("Pre-processing Data column...")
    t_preprocess_start = time.time()
    # Apply the conversion function to the 'Data' column
    # Ensure 'Data' column exists before applying
    if 'Data' in combined_data.columns:
        combined_data['data_bytes'] = combined_data['Data'].apply(hex_string_to_bytes)
    else:
        print("Warning: 'Data' column not found for pre-processing. Assuming empty data.")
        # Create an empty list for every row if Data column is missing
        combined_data['data_bytes'] = [[] for _ in range(len(combined_data))]

    t_preprocess_end = time.time()
    if debug:
        print(f"Data pre-processing complete: {t_preprocess_end - t_preprocess_start:.2f} seconds")
    # --- End Pre-processing ---


    # Store lengths before deleting DataFrames
    pmz_rows_processed = len(pmz_data)
    debug_rows_processed = len(debug_data)

    # Clear memory
    del pmz_data
    del debug_data
    # We might not need the original 'Data' string column anymore
    if 'Data' in combined_data.columns:
         del combined_data['Data']


    print(f"Writing to BLF file: {output_fp}")
    t_write_start = time.time()
    blf_f = can.BLFWriter(output_fp)

    total_rows = combined_data.shape[0]
    processed_rows = 0
    last_time = t_write_start # Use write start time for progress reporting baseline

    # --- Modified Writing Loop ---
    # Iterate more efficiently (itertuples is often faster than iterrows)
    # We don't need outer chunking anymore if we process the whole DataFrame at once
    if debug: print("Starting BLF writing...")
    # Define a chunk size for progress reporting (can be same as before or different)
    progress_chunk_size = chunk_size # Reuse the function's chunk_size for reporting interval

    for row in combined_data.itertuples(index=False): # Use itertuples
        try:
            # Access fields by name (requires column names match attributes)
            # Ensure required columns exist: SyncTime, MsgID, data_bytes, dlc, source
            required_cols = ['SyncTime', 'MsgID', 'data_bytes', 'dlc', 'source']
            if not all(hasattr(row, col) for col in required_cols):
                 print(f"Warning: Skipping row due to missing attributes. Row: {row}")
                 continue

            list_new = row.data_bytes # Directly use the pre-processed list/tuple
            msg_id_int = int(row.MsgID) # Assuming MsgID is already numeric
            message_dlc = row.dlc # Assuming dlc is present

            # Create the CAN message
            new_msg = can.Message(
                timestamp=row.SyncTime,
                arbitration_id=msg_id_int,
                dlc=message_dlc,
                data=list_new,
                is_extended_id=False, # Assuming standard IDs
                channel=int(row.source)
            )

            blf_f.on_message_received(new_msg)

            processed_rows += 1

            # --- Progress Reporting (Using progress_chunk_size) ---
            if processed_rows % progress_chunk_size == 0 or processed_rows == total_rows: # Report every chunk_size or at the end
                 current_time = time.time()
                 elapsed_total_write = current_time - t_write_start
                 avg_rate = processed_rows / elapsed_total_write if elapsed_total_write > 0 else 0
                 progress = (processed_rows / total_rows) * 100
                 print(f"Progress: {progress:.1f}% ({processed_rows}/{total_rows}) " +
                       f"(avg rate: {avg_rate:.1f} msgs/sec)")


        except Exception as e:
            # Catch errors during message creation or writing
            print(f"Error processing row (MsgID: {getattr(row, 'MsgID', 'N/A')}): {e}")
            if debug: # Make traceback conditional
                import traceback
                traceback.print_exc()
            continue
    # --- End Modified Loop ---


    t_end = time.time()
    blf_f.stop()

    # --- Update Timing Report Calculation ---
    pmz_time = t_pmz_end - t_pmz_start if t_pmz_end > t_pmz_start else 0
    debug_time = t_debug_end - t_debug_start if t_debug_end > t_debug_start else 0
    # <-- ADDED FOR 3-CAN MODE: SDU time calculation -->
    sdu_time = t_sdu_end - t_sdu_start if t_sdu_end > t_sdu_start else 0

    merge_time = t_merge_end - t_merge_start if t_merge_end > t_merge_start else 0
    preprocess_time = t_preprocess_end - t_preprocess_start if t_preprocess_end > t_preprocess_start else 0 # Added check
    write_time = t_end - t_write_start if t_end > t_write_start else 0
    total_time = t_end - time_start if t_end > time_start else 0

    # Ensure division by zero doesn't happen for rates
    pmz_rate = pmz_rows_processed / pmz_time if pmz_time > 0 else 0
    debug_rate = debug_rows_processed / debug_time if debug_time > 0 else 0

    # <-- ADDED FOR 3-CAN MODE: SDU rate calculation -->
    sdu_rate = sdu_rows_processed / sdu_time if sdu_time > 0 else 0

    write_rate = total_rows / write_time if write_time > 0 else 0
    overall_rate = total_rows / total_time if total_time > 0 else 0

    # Update timing report string
    timing_report = f"\nTiming Report (Dask mode):\n" + \
                    f"PMZ file processing: {pmz_time:.2f} seconds ({pmz_rate:.1f} rows/sec)\n" + \
                    f"Debug file processing: {debug_time:.2f} seconds ({debug_rate:.1f} rows/sec)\n" + \
                    f"SDU file processing: {sdu_time:.2f} seconds ({sdu_rate:.1f} rows/sec)\n" + \
                    f"Data merging and sorting: {merge_time:.2f} seconds\n" + \
                    f"Data pre-processing: {preprocess_time:.2f} seconds\n" + \
                    f"BLF writing: {write_time:.2f} seconds ({write_rate:.1f} rows/sec)\n" + \
                    f"Total execution time: {total_time:.2f} seconds\n" + \
                    f"Overall processing speed: {overall_rate:.1f} rows/second"

    # Keep the full message for console output
    completion_message_console = f"Conversion Done! Total processed messages: {total_rows}" + timing_report
    print(completion_message_console)
    
    # If a log callback is provided, call it with a simpler summary message
    if log_callback:
        # Create a simpler message for the GUI log callback
        completion_message_callback = f"Conversion completed successfully. Processed {total_rows} messages in {total_time:.2f} seconds."
        try:
            log_callback(completion_message_callback)
        except Exception as cb_err:
            # Log an error if the callback itself fails, but don't crash the script
            print(f"Warning: Error executing log_callback on success: {cb_err}")

    return True


def estimate_memory_requirements(file_paths):
    """Estimate and print memory requirements for processing the given files."""
    total_size = sum(os.path.getsize(fp) for fp in file_paths if os.path.exists(fp))
    
    print(f"Input files total size: {total_size / (1024**3):.2f} GB")
    print(f"Estimated peak memory usage: {(total_size * 1.5) / (1024**3):.2f} GB")
    print(f"Recommended available RAM: {(total_size * 2) / (1024**3):.2f} GB")
    
    return total_size


def print_memory_usage(debug=False):
    if not debug: return # Exit early if not in debug mode
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        print(f"Current memory usage: {memory_info.rss / (1024**3):.2f} GB")
    except ImportError:
        print("Install psutil package to monitor memory usage")


def time_execution_with_profile(func, *args, log_callback=None, **kwargs):
    """Run a function with timing profile and handle log callback."""
    start_time = time.time()
    print(f"Starting execution at {datetime.datetime.now().strftime('%H:%M:%S')}")
    print_memory_usage(debug=kwargs.get('debug', False)) # Pass debug if present in kwargs

    success = False # Initialize success
    result = None # Initialize result
    try:
        # Pass log_callback down if the target function accepts it
        # This requires knowing if func (e.g., write_blf_optimized) takes log_callback
        import inspect
        sig = inspect.signature(func)
        if 'log_callback' in sig.parameters:
            kwargs['log_callback'] = log_callback

        result = func(*args, **kwargs)
        success = True # Set success only if func completes without error

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        # Call log_callback if provided, reporting the error
        error_msg_for_callback = f"Error during execution: {str(e)}"
        if log_callback:
            try:
                log_callback(error_msg_for_callback)
            except Exception as cb_err:
                print(f"Warning: Error executing log_callback on error: {cb_err}")
        success = False
        result = None
        # Optional: re-raise the exception if needed, or just return success=False
        # raise e

    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"\nExecution finished at {datetime.datetime.now().strftime('%H:%M:%S')}")
    print(f"Total wall clock time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
    print_memory_usage()
    
    # Format as h:m:s for longer runs
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Time taken: {int(hours):02d}:{int(minutes):02d}:{seconds:.2f}")
    
    return result, success, elapsed


# === Wrapper Function for Compatibility ===
def write_blf(pmz_can_fp, debug_can_fp, blf_fp, log_callback=None, sdu_can_fp=None ):
    """
    Wrapper function to maintain compatibility with the calling signature
    expected by ControlDesk.py.

    Calls the optimized Dask-based implementation (`write_blf_optimized`).
    """
    print("Using write_blf compatibility wrapper...") # Log that the wrapper is used

    # Define default parameters for the optimized function
    chunk_size = 100000  # Default chunk size
    use_dask = True       # Default to use Dask
    num_workers = None    # Default to auto-detect workers
    debug = False         # Default to no debug mode

    try:
        # Call the actual implementation using the wrapper arguments
        # Use the time_execution_with_profile to get proper timing and error handling
        _result, success, _elapsed = time_execution_with_profile(
            write_blf_optimized,
            pmz_can_fp, debug_can_fp, blf_fp,
            chunk_size=chunk_size,
            num_workers=num_workers,
            use_dask=use_dask,
            debug=debug,
            log_callback=log_callback, # Pass the callback here
            sdu_fp=sdu_can_fp
        )
        return success
    except Exception as e:
        # If time_execution_with_profile itself fails (unlikely but possible)
        # or if it re-raises an error.
        error_msg = f"Critical error in write_blf wrapper: {e}"
        print(error_msg)
        if log_callback:
            try:
                log_callback(error_msg)
            except Exception as cb_err:
                print(f"Warning: Error executing log_callback on critical error: {cb_err}")
        return False


def _parse_timestamps_from_filename(file_name):
    """Parse start/end timestamps from BLF filename pattern ST{timestamp}_SP{timestamp}.

    Filename pattern: {batpack_id}PMZ_Debug_Merged_ST2026-03_09 14_14_45_SP2026-03_09 17_33_47.blf
    Timestamp format: %Y-%m_%d %H_%M_%S  (e.g. 2026-03_09 14_14_45)

    Returns:
        tuple: (start_epoch, end_epoch) as floats, or (None, None) on failure
    """
    import re
    try:
        pattern = r'ST(\d{4}-\d{2}_\d{2}\s\d{2}_\d{2}_\d{2})_SP(\d{4}-\d{2}_\d{2}\s\d{2}_\d{2}_\d{2})'
        match = re.search(pattern, file_name)
        if not match:
            return None, None

        fmt = "%Y-%m_%d %H_%M_%S"
        import pytz
        uk_tz = pytz.timezone('Europe/London')

        start_dt = uk_tz.localize(datetime.datetime.strptime(match.group(1), fmt))
        end_dt = uk_tz.localize(datetime.datetime.strptime(match.group(2), fmt))

        return start_dt.timestamp(), end_dt.timestamp()
    except Exception:
        return None, None


def upload_blf_to_gcs(blf_file_path, test_name, test_setup='', sample_no='',
                      environment='dev', backend_api_url='', log_callback=None,
                      battery_pack_id='', start_time=None, end_time=None,
                      api_token=''):
    """
    Upload a generated BLF file to GCS and register it with the backend.

    Args:
        blf_file_path: Local path to the BLF file
        test_name: SBTL test name (e.g., 'SBTL100020')
        test_setup: Test setup name (matches ATFX folder structure)
        sample_no: Sample number (matches ATFX folder structure)
        environment: 'dev' or 'prod' (determines GCS prefix)
        backend_api_url: Backend API base URL (e.g., 'http://server/api')
        log_callback: Optional callback for logging messages
        battery_pack_id: BatPack ID from ControlDesk (e.g., '31MLA-PKL-172IPB7-5040')
        start_time: Recording start time as Unix epoch (float)
        end_time: Recording end time as Unix epoch (float)

    Returns:
        bool: True if upload and registration succeeded
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        print(msg)

    if not os.path.exists(blf_file_path):
        log(f"BLF file not found: {blf_file_path}")
        return False

    try:
        from google.cloud import storage as gcs_storage
    except ImportError:
        log("google-cloud-storage not installed. Run: pip install google-cloud-storage")
        return False

    try:
        # Determine GCS prefix based on environment
        if environment.lower() in ('production', 'prod'):
            base_prefix = '03_SBTL'
        else:
            base_prefix = '00_DDG_DEV_env/03_SBTL'

        # Build GCS path: {prefix}/{test_name}/{test_setup}/{sample_no}/blf/
        path_parts = [base_prefix, test_name]
        if test_setup:
            path_parts.append(test_setup)
        if sample_no:
            path_parts.append(sample_no)
        path_parts.append('blf')

        gcs_folder = '/'.join(path_parts)
        file_name = os.path.basename(blf_file_path)
        gcs_path = f"{gcs_folder}/{file_name}"

        # Upload to GCS
        bucket_name = os.environ.get('SBTL_GCS_BUCKET_NAME',
                                     os.environ.get('GCS_BUCKET_NAME', 'ddg-dashboard-testfiles'))

        log(f"Uploading BLF to GCS: gs://{bucket_name}/{gcs_path}")

        client = gcs_storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(blf_file_path)

        file_size = os.path.getsize(blf_file_path)
        log(f"BLF uploaded successfully ({file_size / 1024 / 1024:.1f} MB)")

        # Register with backend API
        if backend_api_url:
            try:
                import requests
                register_url = f"{backend_api_url}/sbtl/tests/{test_name}/blf-files/register/"

                # Get auth token from parameter or environment
                auth_token = api_token or os.environ.get('DDG_API_TOKEN', '')
                headers = {}
                if auth_token:
                    headers['Authorization'] = f'Token {auth_token}'

                # If start/end times not provided, try to parse from filename
                _start = start_time
                _end = end_time
                if _start is None or _end is None:
                    parsed_start, parsed_end = _parse_timestamps_from_filename(file_name)
                    if _start is None:
                        _start = parsed_start
                    if _end is None:
                        _end = parsed_end

                payload = {
                    'file_name': file_name,
                    'gcs_path': gcs_path,
                    'test_setup': test_setup,
                    'sample_no': sample_no,
                    'source': 'RIG_AUTO',
                }
                if battery_pack_id:
                    payload['battery_pack_id'] = battery_pack_id
                if _start is not None:
                    payload['start_time'] = _start
                if _end is not None:
                    payload['end_time'] = _end

                log(f"Registering BLF with backend: {register_url}")
                response = requests.post(register_url, json=payload, headers=headers, timeout=30, verify=False)

                if response.ok:
                    log(f"BLF registered successfully (ID: {response.json().get('id', 'N/A')})")
                else:
                    log(f"BLF registration failed: {response.status_code} - {response.text}")

            except Exception as api_err:
                log(f"Warning: BLF registration API call failed: {api_err}")
                # Upload succeeded even if registration failed

        return True

    except Exception as e:
        log(f"Error uploading BLF to GCS: {e}")
        import traceback
        log(traceback.format_exc())
        return False


if __name__ == "__main__":
    # Start overall timing
    program_start = time.time()
    
    fpath_blf = r"D:\Jerry\testdata\new_speed_mp4.blf"
    pmz_csv = r"D:\Jerry\testdata\31EMA_PKL_168IPB2_8132PMZ_CAN_ST2025-04_06 13_46_55_SP2025-04_06 14_09_52.csv"
    debug_csv = r"D:\Jerry\testdata\31EMA_PKL_168IPB2_8132Debug_CAN_ST2025-04_06 13_46_55_SP2025-04_06 14_09_52.csv"
    
    print(f"=== CAN CSV to BLF Conversion ===")
    print(f"Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Estimate memory requirements
    estimate_memory_requirements([pmz_csv, debug_csv])
    
    # Set processing parameters
    chunk_size = 100000      # How many rows to read at once from CSV
    use_dask = True          # Whether to use Dask for multi-core processing
    num_workers = None       # Number of worker processes (None = auto-detect)
    run_in_debug_mode = False # Control debug for standalone run
    
    # Print processing configuration
    print(f"\nConfiguration:")
    print(f"- Multi-core processing (Dask): {'Enabled' if use_dask and DASK_AVAILABLE else 'Disabled'}")
    if use_dask and DASK_AVAILABLE:
        if num_workers is None:
            import multiprocessing
            num_workers = max(1, multiprocessing.cpu_count() - 1)
        print(f"- Number of worker processes: {num_workers}")
    print(f"- Chunk size: {chunk_size}")
    print(f"- Debug mode: {run_in_debug_mode}")
    
    print("\nStarting conversion...")
    print_memory_usage(debug=run_in_debug_mode)
    
    # Dummy callback for standalone run
    def standalone_logger(message):
        print(f"STANDALONE LOG: {message}")

    # --- Standalone run now uses the wrapper function for consistency ---
    print("\nRunning via write_blf wrapper for standalone test...")
    success = write_blf(
        pmz_csv, debug_csv, fpath_blf,
        log_callback=standalone_logger
    )
    # --- End standalone run modification ---


    # Summary
    print("\n=== Conversion Summary ===")
    print(f"PMZ file: {os.path.basename(pmz_csv)}")
    print(f"Debug file: {os.path.basename(debug_csv)}")
    print(f"Output BLF: {os.path.basename(fpath_blf)}")
    print(f"Status: {'Success' if success else 'Failed'}") # Status based on wrapper return
    print(f"Multi-core processing: {'Enabled' if use_dask and DASK_AVAILABLE else 'Disabled'}")

    # Calculate processing rate (based on overall time if using wrapper)
    # Note: Detailed timing report is now printed by time_execution_with_profile inside the wrapper
    # This calculation here might be less accurate if the wrapper adds overhead.
    elapsed_total = time.time() - program_start
    try:
        total_size = sum(os.path.getsize(fp) for fp in [pmz_csv, debug_csv] if os.path.exists(fp))
        if elapsed_total > 0:
             processing_rate = total_size / (1024**2) / (elapsed_total / 60)  # MB per minute
             print(f"Processing rate (Overall): {processing_rate:.2f} MB/minute")
    except Exception as rate_err:
        print(f"Could not calculate overall processing rate: {rate_err}")


    print(f"\nTotal program runtime: {elapsed_total:.2f} seconds")
    print(f"Completed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")