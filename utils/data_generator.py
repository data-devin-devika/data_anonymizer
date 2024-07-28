import csv
import random
import datetime
import os
import sys

def generate_csv(filename: str, num_records: int):
    """Generates a CSV file with random data."""
    first_names = ['Aarav', 'Vivaan', 'Aditya', 'Vihaan', 'Arjun', 'Sai', 'Reyansh', 'Ayaan', 'Krishna', 'Ishaan', 'Shaurya', 'Atharv', 'Dhruv', 'Kabir', 'Ritvik', 'Nishant', 'Aryan', 'Laksh', 'Shivansh', 'Tanmay']
    last_names = ['Patel', 'Sharma', 'Reddy', 'Naidu', 'Kumar', 'Jain', 'Mehta', 'Nair', 'Singh', 'Yadav', 'Choudhary', 'Gupta', 'Pandey', 'Joshi', 'Agrawal', 'Kapoor', 'Verma', 'Sinha', 'Thakur', 'Rao']
    addresses = ['123 MG Road', '456 Nehru Street', '789 Tagore Lane', '101 Patel Marg', '202 Gandhi Avenue', '303 Ambedkar Road', '404 Nehru Nagar', '505 Vivekananda Path', '606 Indira Colony', '707 Tilak Nagar', '808 Patel Lane', '909 Ambedkar Colony', '1010 Gandhi Nagar', '1111 Nehru Street', '1212 Tagore Path', '1313 Patel Nagar', '1414 Gandhi Path', '1515 Ambedkar Lane', '1616 Nehru Marg', '1717 Vivekananda Street']

    start_date = datetime.date(1970, 1, 1)
    end_date = datetime.date(2000, 12, 31)

    def random_date(start, end):
        return start + datetime.timedelta(days=random.randint(0, int((end - start).days)))

    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['first_name', 'last_name', 'address', 'date_of_birth']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for _ in range(num_records):
                writer.writerow({
                    'first_name': random.choice(first_names),
                    'last_name': random.choice(last_names),
                    'address': random.choice(addresses),
                    'date_of_birth': random_date(start_date, end_date).strftime("%Y-%m-%d")
                })
        print(f"CSV file '{filename}' generated with {num_records} records.")
    except Exception as e:
        print(f"An error occurred while generating the CSV file: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: data_generator.py <filename> <num_records>")
        sys.exit(1)
    
    filename = sys.argv[1]
    num_records = int(sys.argv[2])
    
    generate_csv(filename, num_records)