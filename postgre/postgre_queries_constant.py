
ALBUMS_CREATE="""CREATE TABLE IF NOT EXISTS albums (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_date DATE,
    type VARCHAR(255),
    duration BIGINT,
    genre VARCHAR(255),
    stock INT NOT NULL,
    price INT NOT NULL,
    group_id INT NOT NULL,
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

COMPANIES_CREATE="""CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    headquarter VARCHAR(255) NOT NULL,
    country_id INT NOT NULL,
    founded_year INT NOT NULL,
    FOREIGN KEY(country_id) REFERENCES countries(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

COUNTRIES_CREATE="""CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    capital VARCHAR(255) NOT NULL,
    continent VARCHAR(20) NOT NULL,
    currency VARCHAR(255) NOT NULL
)"""

GROUPS_TYPE_ENUM="""CREATE TYPE groups_type_enum AS ENUM ('boy groups', 'girl groups');"""

GROUPS_CREATE="""CREATE TABLE IF NOT EXISTS groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    debut DATE NOT NULL,
    company_id INT,
    fanclub_name VARCHAR(255),
    active BOOLEAN NOT NULL,
    type groups_type_enum NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

IDOLS_GENDER_ENUM="""CREATE TYPE idols_gender_enum AS ENUM ('F', 'M');"""

IDOLS_CREATE="""CREATE TABLE IF NOT EXISTS idols (
    id SERIAL PRIMARY KEY,
    stage_name VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    date_of_birth DATE NOT NULL,
    birthplace VARCHAR(255),
    group_id INT,
    country_id INT NOT NULL,
    gender idols_gender_enum,
    weight FLOAT,
    height FLOAT,
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(country_id) REFERENCES countries(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

SONGS_CREATE="""CREATE TABLE IF NOT EXISTS songs (
    id SERIAL PRIMARY KEY,
    album_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    is_title_track BOOLEAN NOT NULL,
    duration BIGINT NOT NULL,
    FOREIGN KEY(album_id) REFERENCES albums(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

CUSTOMERS_CREATE="""CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    address TEXT,
    country_id INT,
    FOREIGN KEY(country_id) REFERENCES countries(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

TRANSACTIONS_STATUS_ENUM="""CREATE TYPE transactions_status_enum AS ENUM ('pending', 'cancelled', 'paid');"""

TRANSACTIONS_CREATE="""CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    status transactions_status_enum NOT NULL,
    customer_id INT NOT NULL,
    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""

TRANSACTION_ALBUMS_CREATE="""CREATE TABLE IF NOT EXISTS transaction_albums (
    album_id INT NOT NULL,
    transaction_id INT NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY(album_id) REFERENCES albums(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(transaction_id) REFERENCES transactions(id) ON DELETE CASCADE ON UPDATE CASCADE
)"""