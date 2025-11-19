-- schema.sql

-- Book metadata table
CREATE TABLE IF NOT EXISTS book_metadata (
    book_id TEXT PRIMARY KEY,
    title TEXT,
    average_rating FLOAT,
    num_pages INTEGER,
    publication_year INTEGER,
    image_url TEXT
);

-- Authors table
CREATE TABLE IF NOT EXISTS authors (
    author_id TEXT PRIMARY KEY,
    name TEXT,
    average_rating FLOAT,
    ratings_count INTEGER
);

-- Book-Author join table (many-to-many)
CREATE TABLE IF NOT EXISTS book_authors (
    book_id TEXT REFERENCES book_metadata(book_id),
    author_id TEXT REFERENCES authors(author_id),
    PRIMARY KEY (book_id, author_id)
);
