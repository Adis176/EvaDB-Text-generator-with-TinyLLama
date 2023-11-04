import evadb
import pandas as pd
cursor = evadb.connect().cursor()

cursor.query("""
    DROP TABLE IF EXISTS Frag1;
""").df()

cursor.query("""
    CREATE TABLE Frag1 (
        context TEXT(300),
        input TEXT(300),
        output TEXT(300)
    );
""").df()

cursor.query("""
    LOAD CSV 'data.csv' INTO Frag1;
""").df()

exec_file = cursor.query("""
    SELECT * FROM Frag1;
""").df()


cursor.query("""
    DROP TABLE IF EXISTS Frag2;
""").df()

cursor.query("""
    CREATE TABLE Frag2 (
        response TEXT(300)
    );
""").df()

cursor.query("""
    LOAD CSV 'generated.csv' INTO Frag2;
""").df()

exec_file2 = cursor.query("""
    SELECT * FROM Frag2;
""").df()

cursor.query("""
    CREATE FUNCTION IF NOT EXISTS Analyzer
    TYPE HuggingFace
    TASK 'text-classification'
    MODEL 'distilbert-base-uncased-finetuned-sst-2-english';
""").df()



cursor.query("""
    DROP TABLE IF EXISTS Ans1;
""").df()

a1 = cursor.query("""
    CREATE TABLE Ans1 AS
    SELECT input, Analyzer(input)
    FROM Frag1
""").df()

a11 = cursor.query("""
    SELECT * FROM Ans1;
""").df()
a11.columns = ["id", "incomplete_input", "incomplete_label", "incomplete_score"]

cursor.query("""
    DROP TABLE IF EXISTS Ans2;
""").df()

a2 = cursor.query("""
    CREATE TABLE Ans2 AS
    SELECT output, Analyzer(output)
    FROM Frag1
""").df()

a22 = cursor.query("""
    SELECT * FROM Ans2;
""").df()
a22.columns = ["id", "complete_input", "complete_label", "complete_score"]

cursor.query("""
    DROP TABLE IF EXISTS Ans3;
""").df()

a3 = cursor.query("""
    CREATE TABLE Ans3 AS
    SELECT response, Analyzer(response)
    FROM Frag2
""").df()

a33 = cursor.query("""
    SELECT * FROM Ans3;
""").df()
a33.columns = ["id", "generated_input", "generated_label", "generated_score"]
print(a33)

combined_df = pd.concat([a11["id"], a11["incomplete_input"], a11["incomplete_label"], a22["complete_input"], a22["complete_label"], a33["generated_input"], a33["generated_label"]], axis=1)
csv_file_path = './ans.csv'
def delete_current_data(filename):
    with open(filename, 'w', newline='') as csvfile:
        pass

try:
    with open(csv_file_path, "r"):
        file_exists = True
except FileNotFoundError:
    file_exists = False

if not file_exists:
    combined_df.to_csv(csv_file_path, encoding='utf-8', mode='a', header=True, index=False)
else:
    delete_current_data(csv_file_path)
    combined_df.to_csv(csv_file_path, encoding='utf-8', header=True, index=False)
