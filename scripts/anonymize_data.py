# anonymize_data.py
import csv
import os
import json
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv
import hashlib

class Tokenizer:
    """Simple Tokenizer using SHA256 hash."""
    def __init__(self, key):
        self.key = key

    def tokenize(self, value):
        return hashlib.sha256((self.key + value).encode()).hexdigest()[:16]

def load_tokenization_key():
    """Load the tokenization key from the .env file."""
    load_dotenv()
    key = os.getenv("TOKENIZATION_KEY")
    if not key:
        raise ValueError("TOKENIZATION_KEY not found in .env file")
    return key

def tokenize_chunk(chunk, columns_to_tokenize, key):
    """Tokenize a chunk of data."""
    tokenizer = Tokenizer(key)
    tokenized_chunk = []
    for row in chunk:
        for col in columns_to_tokenize:
            if col in row:
                row[col] = tokenizer.tokenize(row[col])
        tokenized_chunk.append(row)
    return tokenized_chunk

def read_csv_in_chunks(file_path, chunk_size=10000):
    """Read CSV file in chunks."""
    with open(file_path, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def write_chunk_to_csv(file_path, chunk, fieldnames, mode='w'):
    """Write a chunk of data to a CSV file."""
    with open(file_path, mode=mode, newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        writer.writerows(chunk)

def create_output_dir_if_not_exists(output_dir):
    """Create output directory if it does not exist."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def tokenize_csv(input_path, output_dir, columns_to_tokenize, chunk_size=10000, checkpoint_file="checkpoint.json", min_file_size=10*1024*1024):
    """Tokenize the CSV file with checkpointing."""
    key = load_tokenization_key()
    temp_files = []
    processed_chunks = set()
    
    # Load checkpoint
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            processed_chunks = set(json.load(f))

    total_chunks = math.ceil(sum(1 for _ in open(input_path)) / chunk_size)
    chunk_index = 0

    create_output_dir_if_not_exists(output_dir)  # Ensure the output directory exists

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_chunk = {
            executor.submit(tokenize_chunk, chunk, columns_to_tokenize, key): (chunk, chunk_index)
            for chunk_index, chunk in enumerate(read_csv_in_chunks(input_path, chunk_size))
            if chunk_index not in processed_chunks
        }
        
        for future in as_completed(future_to_chunk):
            try:
                tokenized_chunk = future.result()
                chunk, chunk_index = future_to_chunk[future]
                temp_file_path = os.path.join(output_dir, f"chunk_{chunk_index}.csv")
                temp_files.append(temp_file_path)
                write_chunk_to_csv(temp_file_path, tokenized_chunk, chunk[0].keys(), mode='w')

                # Update checkpoint
                processed_chunks.add(chunk_index)
                with open(checkpoint_file, 'w') as f:
                    json.dump(list(processed_chunks), f)

                print(f"Processed {chunk_index+1}/{total_chunks} chunks.")

            except Exception as e:
                print(f"An error occurred during chunk processing: {e}")

    print("All chunks processed successfully.")

    # Combine chunks into larger files if needed
    combined_index = 0
    combined_file_path = os.path.join(output_dir, f"combined_{combined_index}.csv")
    combined_file_size = 0
    writer = None
    for temp_file in temp_files:
        with open(temp_file, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if writer is None:
                writer = csv.DictWriter(open(combined_file_path, mode='w', newline='', encoding='utf-8'), fieldnames=reader.fieldnames)
                writer.writeheader()
            for row in reader:
                writer.writerow(row)
                combined_file_size += len(str(row))
                if combined_file_size >= min_file_size:
                    combined_index += 1
                    combined_file_path = os.path.join(output_dir, f"combined_{combined_index}.csv")
                    writer = csv.DictWriter(open(combined_file_path, mode='w', newline='', encoding='utf-8'), fieldnames=reader.fieldnames)
                    writer.writeheader()
                    combined_file_size = 0
        os.remove(temp_file)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: anonymize_data.py <input_path> <output_dir> <columns_to_tokenize>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    columns_to_tokenize = sys.argv[3].split(',')

    tokenize_csv(input_path, output_dir, columns_to_tokenize)