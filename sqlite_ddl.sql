CREATE TABLE IF NOT EXISTS raw
(
    client_number INTEGER,
    client_name TEXT,
    client_birthdate TEXT,
    client_contact TEXT,
    card_number TEXT,
    expiry_date TEXT
);

CREATE TABLE clients
(
    client_number INTEGER NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    birthdate TEXT NOT NULL
);

CREATE TABLE contacts
(
    client_number INTEGER NOT NULL,
    info TEXT NOT NULL
);

CREATE TABLE cards
(
    client_number INTEGER NOT NULL,
    card_number TEXT NOT NULL,
    expiry_date TEXT NOT NULL
);

INSERT INTO clients
SELECT DISTINCT client_number, client_name, client_birthdate
FROM raw;

INSERT INTO contacts
SELECT DISTINCT client_number, client_contact
FROM raw;

INSERT INTO cards
SELECT DISTINCT client_number, card_number, expiry_date
FROM raw;

CREATE INDEX idx_clients_name_dob ON clients (name, birthdate);

CREATE INDEX idx_contacts_client_num ON contacts (client_number);

CREATE INDEX idx_cards_client_num ON cards (client_number);

CREATE INDEX idx_cards_card_number ON cards (card_number);

DROP TABLE raw;

VACUUM;

ALTER TABLE contacts ADD info_reversed TEXT;

CREATE INDEX idx_contacts_client_info ON contacts (info);

CREATE INDEX idx_contacts_client_info_rev ON contacts (info_reversed);
