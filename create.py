import evadb
cursor = evadb.connect().cursor()

cursor.query("""
    DROP TABLE IF EXISTS Fragment;
""").df()

cursor.query("""
    CREATE TABLE Fragment (
        context TEXT(300),
        input TEXT(300),
        output TEXT(300)
    );
""").df()

cursor.query("""
    LOAD CSV 'data.csv' INTO Fragment;
""").df()


cursor.query("""
    DROP FUNCTION IF EXISTS TinyLLama
""").df()

cursor.query("""
    CREATE FUNCTION IF NOT EXISTS TinyLLama
    IMPL './tinyllama.py'
""").df()


cursor.query("""
    DROP TABLE IF EXISTS middl;
""").df()


cursor.query("""
    CREATE TABLE middl AS
    SELECT TinyLLama(*)
    FROM Fragment;
""").df()

mid = cursor.query("""
    SELECT * FROM middl;
""").df()
print(mid)

csv_file_path = './generated.csv'
def delete_current_data(filename):
    with open(filename, 'w', newline='') as csvfile:
        pass
try:
    with open(csv_file_path, "r"):
        file_exists = True
except FileNotFoundError:
    file_exists = False

if not file_exists:
    mid.columns = ["id", "response"]
    mid.to_csv(csv_file_path, encoding='utf-8', header=True, index=False)
    
else:
    delete_current_data(csv_file_path)
    mid.columns = ["id", "response"]
    mid.to_csv(csv_file_path, encoding='utf-8', mode='a', header=True,index=False)
    