-- create hello_world_table
DROP TABLE IF EXISTS pet;
CREATE TABLE pet (
    pet_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    pet_type VARCHAR NOT NULL,
    birth_date DATE NOT NULL,
    OWNER VARCHAR NOT NULL
    );