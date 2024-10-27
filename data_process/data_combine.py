import os
import pandas as pd

tables_list = ['albums', 'companies', 'customers', 'groups', 'idols', 'songs', 'transaction_albums', 'transactions', 'countries']
DATA_CSV_DIR = os.path.join(os.getcwd(), "data_csv")
DATA_TEMP_DIR = os.path.join(os.getcwd(), "data_temp")
DATA_FINAL_DIR = os.path.join(os.getcwd(), "data_final")


for table in tables_list:
  print(f"[PROGRESS] Reading table {table}")
  init_data = f"{table}.csv"

  df = None
  try: 
    data_path = os.path.join(DATA_CSV_DIR, init_data)
    df = pd.read_csv(data_path)
  except Exception as e:
    print(f"[FAILED] There is no {init_data}")

  # Read temp data
  for i in range(1, 5):
    temp_data = f"{table}_P{i}.csv"
    data_path = os.path.join(DATA_TEMP_DIR, temp_data)

    try:
      temp_df = pd.read_csv(data_path)

      if (df is None):
        df = temp_df
      else:
        df = pd.concat([df, temp_df])

    except Exception as e:
      print(f"[FAILED] Error in opening {data_path} : {e}")

  filename = os.path.join(DATA_FINAL_DIR, f"{table}.csv")
  df.to_csv(filename, index=False)
