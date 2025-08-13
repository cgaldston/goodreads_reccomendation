import psycopg2

def connect():
    return psycopg2.connect(
        dbname="goodreads",
        user="postgres",
        password="ZippyKiko88",
        host="localhost",
        port="5432"
    )

def create_schema(conn):
    with conn.cursor() as cur:
        # Book metadata
        cur.execute("""
        CREATE TABLE IF NOT EXISTS book_metadata (
            book_id TEXT PRIMARY KEY,
            title TEXT,
            average_rating FLOAT,
            num_pages INTEGER,
            publication_year INTEGER,
            image_url TEXT
        );
        """)

        # Authors
        cur.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            author_id TEXT PRIMARY KEY,
            name TEXT,
            average_rating FLOAT,
            ratings_count INTEGER
        );
        """)

        # Book-author join table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS book_authors (
            book_id TEXT REFERENCES book_metadata(book_id),
            author_id TEXT REFERENCES authors(author_id),
            PRIMARY KEY (book_id, author_id)
        );
        """)

        conn.commit()

if __name__ == "__main__":
    conn = connect()
    create_schema(conn)
    conn.close()
    print("Schema created successfully.")
