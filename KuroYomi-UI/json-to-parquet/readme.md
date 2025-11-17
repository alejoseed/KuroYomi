Current schema for the sqlite db


-- Dictionaries definition

CREATE TABLE Dictionaries (
    id INTEGER PRIMARY KEY,
    title TEXT UNIQUE NOT NULL,
    revision TEXT
, author TEXT);

CREATE UNIQUE INDEX idx_dict_unique ON Dictionaries(title, author, revision);

-- Terms definition

CREATE TABLE Terms (
    id INTEGER PRIMARY KEY,
    term TEXT NOT NULL,
    reading TEXT NOT NULL,
    dictionary_id INTEGER NOT NULL,
    score INTEGER,
    sequence INTEGER,
    FOREIGN KEY (dictionary_id) REFERENCES Dictionaries(id),
    UNIQUE(term, reading, dictionary_id)
);

-- Definitions definition
CREATE TABLE Definitions (
    id INTEGER PRIMARY KEY,
    term_id INTEGER NOT NULL,
    definition TEXT NOT NULL,
    sense_order INTEGER,
    FOREIGN KEY (term_id) REFERENCES Terms(id) ON DELETE CASCADE
);