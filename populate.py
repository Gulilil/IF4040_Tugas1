import pandas as pd
import os
import randomname
import random
from multiprocessing import Process
from names_generator import generate_name

DATA_CSV_DIR = os.path.join(os.getcwd(), "data_csv")
DATA_FINAL_DIR = os.path.join(os.getcwd(), "data_final")
DATA_TEMP_DIR = os.path.join(os.getcwd(), "data_temp")
LIMIT = 100000
COMPANIES_LAST_ID = 1125
GROUPS_LAST_ID = 447
COUNTRIES_LAST_ID = 195



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



if __name__ == "__main__":
  files = []
  filename = os.listdir(DATA_CSV_DIR)

  # Reading each file
  for file in filename:
    file_path = os.path.join(DATA_CSV_DIR, file)

    # Read df
    amount = int(LIMIT // 4)
    df = pd.read_csv(file_path)
    cols = df.columns.tolist()
    offset = df['id'].max()


    # if (file == 'groups.csv'):      
    #   p1 = Process(target=generate_groups_data, args=(cols, amount, offset, 1))
    #   p2 = Process(target=generate_groups_data, args=(cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_groups_data, args=(cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_groups_data, args=(cols, amount, offset + 3*amount, 4))

    # if (file == 'companies.csv'):
    #   df_countries = pd.read_csv(os.path.join(DATA_CSV_DIR, "countries.csv"))
    #   p1 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset, 1))
    #   p2 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset + amount, 2))
    #   p3 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset + 2*amount, 3))
    #   p4 = Process(target=generate_companies_data, args=(df_countries, cols, amount, offset + 3*amount, 4))

    if (file == 'idols.csv'):
      df_countries = pd.read_csv(os.path.join(DATA_CSV_DIR, "countries.csv"))
      p1 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset, 1))
      p2 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset + amount, 2))
      p3 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset + 2*amount, 3))
      p4 = Process(target=generate_idols_data, args=(df_countries, cols, amount, offset + 3*amount, 4))


      p1.start()
      p2.start()
      p3.start()
      p4.start()

      p1.join()
      p2.join()
      p3.join()
      p4.join()


        