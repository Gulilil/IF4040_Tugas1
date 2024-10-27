import csv

# Path to the CSV file
input_file = 'data/transactions.csv'
output_file = 'cleaned/transactions.csv'

# Column that needs cleaning
bigint_columns = ['id']  # Add other columns if needed

# Read and clean the data
with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
    
    writer.writeheader()
    for row in reader:
        for col in bigint_columns:
            # Remove decimal places for BIGINT fields
            if row[col]:
                row[col] = str(int(float(row[col])))
        writer.writerow(row)

print("Data cleaned and saved to", output_file)
