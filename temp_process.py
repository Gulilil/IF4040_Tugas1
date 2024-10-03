import os
import pandas as pd
import random

DATA_CSV_DIR = os.path.join(os.getcwd(), "data_csv")

if __name__ == "__main__":
  filename = os.path.join(DATA_CSV_DIR, "albums.csv")
  albums_df = pd.read_csv(filename)
  albums_df['stock'] = None
  albums_df['price'] = None

  for index, row in albums_df.iterrows():
    albums_df.at[index, 'stock'] =  random.randint(0, 200)
    albums_df.at[index, 'price'] = random.randint(200000, 16000000)

  albums_df.to_csv(filename, index= False)