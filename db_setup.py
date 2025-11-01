import psycopg2
from psycopg2 import sql

# Import database configuration
from config import DB_CONFIG

def test_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("PostgreSQL connection successful!")
        print(f"Database version: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return False

def create_tables():
    """Create necessary tables for crawler management"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Page URLs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_urls (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                page_number INTEGER NOT NULL,
                url TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(50),
                UNIQUE(mall_name, page_number)
            );
        """)
        print("[OK] Created table: page_urls")

        # 2. XPath selectors table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS xpath_selectors (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                page_type VARCHAR(50) NOT NULL,
                data_field VARCHAR(100) NOT NULL,
                xpath TEXT,
                css_selector TEXT,
                description TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(50),
                UNIQUE(mall_name, page_type, data_field)
            );
        """)
        print("[OK] Created table: xpath_selectors")

        # 3. Collected data table (for storing scraped TV data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collected_data (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                sku VARCHAR(200),
                page_number INTEGER,
                retailer_sku_name TEXT,
                product_url TEXT,
                final_sku_price VARCHAR(100),
                savings VARCHAR(100),
                comparable_pricing VARCHAR(100),
                offer TEXT,
                star_rating VARCHAR(50),
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(mall_name, sku)
            );
        """)
        print("[OK] Created table: collected_data")

        # 4. Product detail data table (for data from product pages)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_details (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                sku VARCHAR(200) NOT NULL,
                retailer_sku_name TEXT,
                samsung_sku_name TEXT,
                screen_size VARCHAR(50),
                resolution VARCHAR(50),
                refresh_rate VARCHAR(50),
                panel_type VARCHAR(50),
                smart_tv VARCHAR(50),
                operating_system VARCHAR(100),
                count_of_star_ratings INTEGER,
                pros TEXT,
                cons TEXT,
                top_mentions TEXT,
                detailed_review_content TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(mall_name, sku)
            );
        """)
        print("[OK] Created table: product_details")

        conn.commit()
        cursor.close()
        conn.close()
        print("\n[OK] All tables created successfully!")
        return True

    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

def insert_sample_data():
    """Insert sample Amazon page URLs"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Insert Amazon page 1 URL
        cursor.execute("""
            INSERT INTO page_urls (mall_name, page_number, url, updated_by)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (mall_name, page_number)
            DO UPDATE SET url = EXCLUDED.url, updated_at = CURRENT_TIMESTAMP;
        """, ('Amazon', 1, 'https://www.amazon.com/s?k=TV&crid=RY3XOG3VC795&sprefix=tv%2Caps%2C287&ref=nb_sb_noss_1', 'system'))

        print("[OK] Inserted sample data for Amazon page 1")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error inserting sample data: {e}")
        return False

def show_tables():
    """Show all tables in the database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)

        tables = cursor.fetchall()
        print("\nExisting tables:")
        for table in tables:
            print(f"  - {table[0]}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error showing tables: {e}")

if __name__ == "__main__":
    print("="*60)
    print("Samsung DX Crawler - Database Setup")
    print("="*60)
    print()

    # Test connection
    print("1. Testing database connection...")
    if test_connection():
        print()

        # Show existing tables
        print("2. Checking existing tables...")
        show_tables()
        print()

        # Create tables
        print("3. Creating tables...")
        if create_tables():
            print()

            # Insert sample data
            print("4. Inserting sample data...")
            insert_sample_data()
            print()

            print("="*60)
            print("Database setup completed successfully!")
            print("="*60)
