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
    sequence INTEGER, tags TEXT, deflection TEXT, term_tags TEXT, hash TEXT,
    FOREIGN KEY (dictionary_id) REFERENCES Dictionaries(id),
    UNIQUE(id, hash)
);

CREATE UNIQUE INDEX Terms_hash_IDX ON Terms (hash);
CREATE INDEX idx_terms_term ON Terms(term);


-- Definitions definition

CREATE TABLE Definitions (
	id INTEGER,
	definition TEXT NOT NULL,
	term_hash TEXT NOT NULL,
	CONSTRAINT DEFINITIONS_PK PRIMARY KEY (id),
	CONSTRAINT Definitions_Terms_FK FOREIGN KEY (term_hash) REFERENCES Terms(hash)
);

CREATE INDEX idx_definitions_term_hash ON Definitions(term_hash);
