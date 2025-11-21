-- schema.sql

-- Book metadata table
CREATE TABLE users (
  user_id TEXT PRIMARY KEY,
  join_date DATE,
  location TEXT,
  num_books_read INT,
  avg_rating_given FLOAT
);


CREATE TABLE books (
    book_id TEXT PRIMARY KEY,

    title TEXT,
    description TEXT,

    author_id TEXT,
    author_name TEXT,

    average_rating FLOAT,
    ratings_count INTEGER,
    publication_year INTEGER,

    genres TEXT[],
    top_shelves TEXT[],

    num_pages INTEGER,
    cover_image_url TEXT,

    isbn TEXT,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE interactions (
  user_id TEXT REFERENCES users(user_id),
  book_id TEXT REFERENCES books(book_id),
  user_rating INT,
  date_read DATE,
  shelves TEXT[],
  PRIMARY KEY (user_id, book_id)
);
