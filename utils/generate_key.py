# generate_key.py
import os
from dotenv import set_key

# Function to generate a random key
def generate_key():
    return os.urandom(16).hex()

# Generate a key and write it to the .env file
key = generate_key()
env_file = ".env"
set_key(env_file, "TOKENIZATION_KEY", key)

print(f"Generated TOKENIZATION_KEY and saved to {env_file}")