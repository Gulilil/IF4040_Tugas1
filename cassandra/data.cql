-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS pedal WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Use the keyspace
USE pedal;

-- Create table for songs
CREATE TABLE IF NOT EXISTS songs (
    id INT PRIMARY KEY,
    album_id INT,
    title VARCHAR,
    is_title_track BOOLEAN,
    duration BIGINT
);

-- Create table for albums
CREATE TABLE IF NOT EXISTS albums (
    id INT PRIMARY KEY,
    title VARCHAR,
    release_date DATE,
    type VARCHAR,
    duration BIGINT,
    genre VARCHAR,
    stock INT,
    price INT,
    group_id INT,
    transaction_albums LIST<FROZEN<tuple<INT, INT>>>, 
    songs LIST<INT> 
);

-- Create table for customers
CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    username VARCHAR,
    password VARCHAR,
    address TEXT,
    country_id INT,
    transactions LIST<INT>
);


-- Create table for transactions
CREATE TABLE IF NOT EXISTS transactions (
    id INT PRIMARY KEY,
    transaction_date DATE,
    status VARCHAR,
    customer_id INT,
    transaction_albums LIST<FROZEN<tuple<INT, INT, VARCHAR, INT>>>
);

-- Create table for groups
CREATE TABLE IF NOT EXISTS groups (
    id INT PRIMARY KEY,
    name VARCHAR,
    debut DATE,
    company_id INT,
    fanclub_name VARCHAR,
    active BOOLEAN,
    type VARCHAR,
    idols LIST<FROZEN<tuple<INT, VARCHAR, VARCHAR, INT, VARCHAR>>>,
    albums LIST<FROZEN<tuple<INT, VARCHAR>>>
);


-- Create table for idols
CREATE TABLE IF NOT EXISTS idols (
    id INT PRIMARY KEY,
    stage_name VARCHAR,
    full_name VARCHAR,
    date_of_birth DATE,
    birthplace VARCHAR,
    group_id INT,
    country_id INT,
    gender TEXT,
    weight FLOAT,
    height FLOAT
);

-- Create table for countries
CREATE TABLE IF NOT EXISTS countries (
    id INT PRIMARY KEY,
    name VARCHAR,
    capital VARCHAR,
    continent VARCHAR,
    currency VARCHAR
);

-- Create table for companies
CREATE TABLE IF NOT EXISTS companies (
    id INT PRIMARY KEY,
    name VARCHAR,
    headquarter VARCHAR,
    country_id INT,
    founded_year INT
);
