import pandas as pd
import os
import randomname
import random
from multiprocessing import Process
from names_generator import generate_name
import time
from random_address import real_random_address

DATA_CSV_DIR = os.path.join(os.getcwd(), "data_csv")
DATA_FINAL_DIR = os.path.join(os.getcwd(), "data_final")
DATA_TEMP_DIR = os.path.join(os.getcwd(), "data_temp")
LIMIT = 500000
LIMIT_MORE = 2000000
LIMIT_MORE_MORE = 5000000
COMPANIES_LAST_ID = 1125
GROUPS_LAST_ID = 447
COUNTRIES_LAST_ID = 195
ALBUMS_LAST_ID = 203


def generate_groups_data(cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  quarter = count // 4
  for i in range(1, count+1):
    # ID
    id = offset + i
    # Name
    name = randomname.get_name()
    names = name.split("-")
    f_name = names[0].title()
    l_name = names[1].title()

    rand_int = random.randint(0, 1)
    if (rand_int == 0):
      res_name = f"{f_name} {l_name}"
    else:
      res_name = f"{l_name} {f_name}"
    # Debut
    year = random.randint(2010, 2023)
    month = random.randint(1, 12)
    date = random.randint(1, 29)
    debut = f"{year}-{month}-{date}"
    # Company_id 
    company_id = random.randint(1+COMPANIES_LAST_ID, LIMIT+COMPANIES_LAST_ID)
    # Fanclub_name 
    fanclub_name = randomname.get_name().split("-")[0].title() if random.randint(0,1) == 1 else None
    # Active
    active = True if (random.randint(1, 5) <= 4) else False
    # Type
    group_type = "girl groups" if (random.randint(0,1) == 1) else "boy groups"

    row_data = [id, res_name, debut, company_id, fanclub_name, active, group_type]
    result_data.append(row_data)

    if (i % quarter == 0):
      print(f"[CHECKPOINT] Generating Groups data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"groups_P{process}.csv")
  result_df.to_csv(filename, index=False)

def generate_companies_data(df_countries: pd.DataFrame, cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  quarter = count // 4
  for i in range(1, count+1):
    # ID
    id = offset + i
    # Name
    name = randomname.get_name()
    names = name.split("-")
    f_name = names[0].title()
    l_name = names[1].title()

    rand_int = random.randint(0, 1)
    if (rand_int == 0):
      res_name = f"{f_name} {l_name}"
    else:
      res_name = f"{l_name} {f_name}"
    
    # Headquarter and country_id
    country_id = random.randint(1, COUNTRIES_LAST_ID)
    row_country_data = df_countries[df_countries['id'] == country_id]
    headquarter = row_country_data['capital'].values[0]

    # Founded year
    founded_year = random.randint(2000, 2023)


    row_data = [id, res_name, headquarter, country_id, founded_year]
    result_data.append(row_data)

    if (i % quarter == 0):
      print(f"[CHECKPOINT] Generating Companies data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"companies_P{process}.csv")
  result_df.to_csv(filename, index=False)

def generate_idols_data(df_countries: pd.DataFrame,cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  quarter = count // 4
  for i in range(1, count+1):
    # ID
    id = offset + i
    
    # Full Name
    name = generate_name(style='capital')
    names = name.split(" ")
    f_name = names[0]
    l_name = names[1]

    rand_int = random.randint(0, 1)
    if (rand_int == 0):
      res_name = f"{f_name} {l_name}"
    else:
      res_name = f"{l_name} {f_name}"

    # Stage_name
    rand_int = random.randint(0, 1)
    if rand_int == 0 : stage_name = f"{f_name[:len(f_name)//2]}{l_name[-len(l_name)//2:]}" 
    else: stage_name = f"{l_name[:len(l_name)//2]}{f_name[-len(f_name)//2:]}" 

    # Date_of_birth
    year = random.randint(1995, 2008)
    month = random.randint(1, 12)
    date = random.randint(1, 29)
    dob = f"{year}-{month}-{date}"

    # Group_id
    group_id = random.randint(1+GROUPS_LAST_ID, LIMIT+GROUPS_LAST_ID)

    # Birthplace and Country_id
    country_id = random.randint(1, COUNTRIES_LAST_ID)
    row_country_data = df_countries[df_countries['id'] == country_id]
    birthplace = row_country_data['capital'].values[0]

    # Gender
    gender = "F" if random.randint(0, 1) == 0 else "M" # Ini ngasal harusnya di cek ke group

    # Weight
    weight = random.randint(40, 55) if gender == "F" else random.randint(55, 65)

    # Founded year
    height = random.randint(155, 170) if gender == "F" else random.randint(170, 185)

    row_data = [id, stage_name, res_name, dob, birthplace, group_id, country_id, gender, weight, height]
    result_data.append(row_data)

    if (i % quarter == 0):
      print(f"[CHECKPOINT] Generating Idols data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"idols_P{process}.csv")
  result_df.to_csv(filename, index=False)

def generate_albums_data(cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  quarter = count // 4
  for i in range(1, count+1):
    # ID
    id = offset + i

    # Title
    name = randomname.get_name()
    names = name.split("-")
    f_name = names[0].title()
    l_name = names[1].title()

    rand_int = random.randint(0, 3)
    if (rand_int == 0):
      title = f"{f_name}: {l_name}"
    elif (rand_int == 1):
      title = f"{l_name}-{f_name}"
    elif (rand_int == 2):
      title = f"{f_name}"
    elif (rand_int == 3):
      title = f"{l_name}: {f_name}"

    # Release date
    year = random.randint(2010, 2023)
    month = random.randint(1, 12)
    date = random.randint(1, 29)
    release = f"{year}-{month}-{date}"

    # Type
    album_types = ['Mini Album', 'Full Length Album', 'Single Album', 'EP Album', 'Compilation Album', 'Digital Single']
    album_type = album_types[random.randint(0, len(album_types)-1)]

    # Duration 
    duration = random.randint(100, 600)

    # Genre 
    genre = "Pop"

    # Group _id
    group_id = random.randint(1+GROUPS_LAST_ID, LIMIT+GROUPS_LAST_ID)

    # Stock
    stock = random.randint(0, 200)

    # Price
    price = random.randint(200000, 16000000)

    row_data = [id, title, release, album_type, duration, genre, group_id, stock, price]
    result_data.append(row_data)

    if (i % quarter == 0):
      print(f"[CHECKPOINT] Generating Albums data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"albums_P{process}.csv")
  result_df.to_csv(filename, index=False)

def generate_songs_data(cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  quarter = count // 4
  for i in range(1, count+1):
    # ID
    id = offset + i

    # Album_id
    album_id = random.randint(1+ALBUMS_LAST_ID, LIMIT+ALBUMS_LAST_ID)

    # Title
    name = randomname.get_name()
    names = name.split("-")
    f_name = names[0].title()
    l_name = names[1].title()

    name2 = randomname.get_name()
    names2 = name2.split("-")
    f_name2 = names2[0].title()
    l_name2 = names2[1].title()

    rand_int = random.randint(0, 3)
    if (rand_int == 0):
      title = f"{f_name} {l_name} {f_name2}"
    elif (rand_int == 1):
      title = f"{l_name}-{f_name} {l_name2}"
    elif (rand_int == 2):
      title = f"{f_name} {l_name2}"
    elif (rand_int == 3):
      title = f"{l_name2} {f_name} {l_name2}"

    # Is title track
    is_title_track = True if (random.randint(0,1) == 0) else False

    # Duration 
    duration = random.randint(120, 240)

    row_data = [id, album_id, title, is_title_track, duration]
    result_data.append(row_data)

    if (i % quarter == 0):
      print(f"[CHECKPOINT] Generating Songs data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"songs_P{process}.csv")
  result_df.to_csv(filename, index=False)

def generate_customers_data(cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  checkpoint_count = count // 10
  for i in range(1, count+1):
    # ID
    id = offset + i

    # Album_id
    album_id = random.randint(1, LIMIT)

    # Full Name
    name = generate_name(style='capital')
    names = name.split(" ")
    f_name = names[0]
    l_name = names[1]

    rand_int = random.randint(0, 1)
    if (rand_int == 0):
      res_name = f"{f_name} {l_name}"
      # Email
      email = f"{f_name}.{l_name}@email.com"

    else:
      res_name = f"{l_name} {f_name}"
      # Email
      email = f"{l_name}.{f_name}@email.com"

    # Username
    rand_int = random.randint(0, 1)
    if rand_int == 0 : username = f"{f_name[:len(f_name)//2]}{l_name[-len(l_name)//2:]}" 
    else: username = f"{l_name[:len(l_name)//2]}{f_name[-len(f_name)//2:]}" 

    # Password
    password = "password"

    # Address 
    address = real_random_address()


    # Country_id
    country_id = random.randint(1, COUNTRIES_LAST_ID)

    row_data = [id, res_name, email, username, password, address, country_id]
    result_data.append(row_data)

    if (i % checkpoint_count == 0):
      print(f"[CHECKPOINT] Generating Customers data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"customers_P{process}.csv")
  result_df.to_csv(filename, index=False)


def generate_transactions_data(cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  checkpoint_count = count // 10
  for i in range(1, count+1):
    # ID
    id = offset + i

    # Customer ID
    customer_id = random.randint(1, LIMIT)

    # Full Name
    year = random.randint(2010, 2023)
    month = random.randint(1, 12)
    date = random.randint(1, 29)
    hour = random.randint(0, 23)
    minute = random.randint(0,59)
    second = random.randint(0,59)
    time_string = f'{year}-{month}-{date} {hour}:{minute}:{second}'

    # Status
    status_list = ['paid', 'cancelled', 'pending']
    status = status_list[random.randint(0, len(status_list)-1)]

    row_data = [id, customer_id, time_string, status]
    result_data.append(row_data)

    if (i % checkpoint_count == 0):
      print(f"[CHECKPOINT] Generating Transactions data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"transactions_P{process}.csv")
  result_df.to_csv(filename, index=False)

def generate_transaction_albums_data(cols: list, count: int, offset: int, process: int = 1):
  result_data = []
  checkpoint_count = count // 10
  for i in range(1, count+1):
    # ID
    trans_id = random.randint(1, LIMIT_MORE)

    # Customer ID
    albums_id = random.randint(1, LIMIT)

    # Quantity
    quantity = random.randint(1, 10)

    row_data = [trans_id, albums_id, quantity]
    result_data.append(row_data)

    if (i % checkpoint_count == 0):
      print(f"[CHECKPOINT] Generating Transaction_Albums data checkpoint {i} data on P{process}")
  
  result_df = pd.DataFrame(result_data, columns=cols)
  filename = os.path.join(DATA_TEMP_DIR, f"transaction_albums_P{process}.csv")
  result_df.to_csv(filename, index=False)


if __name__ == "__main__":
  files = []
  filename = os.listdir(DATA_CSV_DIR)

  # Start time
  start = time.time()
  print("[START] ")


  # Reading each file
  # for file in filename:
  #   file_path = os.path.join(DATA_CSV_DIR, file)

  #   # Read df
  #   amount = int(LIMIT // 4)
  #   df = pd.read_csv(file_path)
  #   cols = df.columns.tolist()
  #   offset = df['id'].max()


    # if (file == 'companies.csv'):
    #   df_countries = pd.read_csv(os.path.join(DATA_CSV_DIR, "countries.csv"))
    #   p1 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset, 1))
    #   p2 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset + 3*amount, 4))

    # if (file == 'groups.csv'):      
    #   p1 = Process(target=generate_groups_data, args=(cols, amount, offset, 1))
    #   p2 = Process(target=generate_groups_data, args=(cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_groups_data, args=(cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_groups_data, args=(cols, amount, offset + 3*amount, 4))

    # if (file == 'idols.csv'):
    #   df_countries = pd.read_csv(os.path.join(DATA_CSV_DIR, "countries.csv"))
    #   p1 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset, 1))
    #   p2 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset + 3*amount, 4))

    # if (file == 'albums.csv'):
    #   p1 = Process(target=generate_albums_data, args=(cols, amount, offset, 1))
    #   p2 = Process(target=generate_albums_data, args=(cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_albums_data, args=(cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_albums_data, args=(cols, amount, offset + 3*amount, 4))

    # if (file == 'songs.csv'):
    #   p1 = Process(target=generate_songs_data, args=(cols, amount, offset, 1))
    #   p2 = Process(target=generate_songs_data, args=(cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_songs_data, args=(cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_songs_data, args=(cols, amount, offset + 3*amount, 4))


    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()

    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()


  # =========================================================
  # For data that do not have the initial csv
  amount = int(LIMIT // 4)
  amount_more = int(LIMIT_MORE//4)
  amount_more_more = int(LIMIT_MORE_MORE//4)
  customers_cols = ['id', 'name', 'email', 'username', 'password', 'address', 'country_id']
  transactions_cols = ['id', 'customer_id', 'created_on', 'status']
  transaction_albums_cols = ['transaction_id', 'customer_id', 'quantity']
  offset = 0

  # # Customers
  # p1 = Process(target=generate_customers_data, args=(customers_cols, amount, offset, 1))
  # p2 = Process(target=generate_customers_data, args=(customers_cols, amount, offset + amount, 2))
  # p3 = Process(target=generate_customers_data, args=(customers_cols, amount, offset + 2*amount, 3))
  # p4 = Process(target=generate_customers_data, args=(customers_cols, amount, offset + 3*amount, 4))

  # # Transactions
  # p1 = Process(target=generate_transactions_data, args=(transactions_cols, amount_more, offset, 1))
  # p2 = Process(target=generate_transactions_data, args=(transactions_cols, amount_more, offset + amount_more, 2))
  # p3 = Process(target=generate_transactions_data, args=(transactions_cols, amount_more, offset + 2*amount_more, 3))
  # p4 = Process(target=generate_transactions_data, args=(transactions_cols, amount_more, offset + 3*amount_more, 4))

  # # TransactionAlbums
  # p1 = Process(target=generate_transaction_albums_data, args=(transaction_albums_cols, amount_more_more, offset, 1))
  # p2 = Process(target=generate_transaction_albums_data, args=(transaction_albums_cols, amount_more_more, offset + amount_more, 2))
  # p3 = Process(target=generate_transaction_albums_data, args=(transaction_albums_cols, amount_more_more, offset + 2*amount_more, 3))
  # p4 = Process(target=generate_transaction_albums_data, args=(transaction_albums_cols, amount_more_more, offset + 3*amount_more, 4))

  # p1.start()
  # p2.start()
  # p3.start()
  # p4.start()

  # p1.join()
  # p2.join()
  # p3.join()
  # p4.join()


  # End time
  end = time.time()
  duration = int(end-start)
  print(f"The execution time: {time.strftime('%H:%M:%S', time.gmtime(duration))}")
  print("[FINISHED]")

  


        