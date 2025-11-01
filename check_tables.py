import psycopg2

# Import database configuration
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Show all tables
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")

print("Tables in database:")
print("="*50)
for table in cursor.fetchall():
    table_name = table[0]
    print(f"\n{table_name}:")

    # Show columns for each table
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)

    for col in cursor.fetchall():
        print(f"  - {col[0]} ({col[1]})")

# Show data in page_urls
print("\n" + "="*50)
print("\nData in page_urls table:")
cursor.execute("SELECT * FROM page_urls;")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
