import evadb
import pandas as pd
cursor = evadb.connect().cursor()

df = pd.read_csv('./ans.csv')
df_not_match_complete_input = pd.DataFrame(columns=['id','complete_input', 'complete_label','generated_input','generated_label'])
df_not_match_fully = pd.DataFrame(columns=['id', 'incomplete_input', 'incomplete_label','complete_input', 'complete_label','generated_input','generated_label'])

rows_not_match_complete_input = []
rows_not_match_fully = []

for index, row in df.iterrows():
    if row['complete_label'] != row['generated_label']:
        if row['incomplete_label'] != row['generated_label']:
            rows_not_match_fully.append({'id': row['id'], 'incomplete_input':   row['incomplete_input'], 'incomplete_label':row['incomplete_label'], 'complete_input':row['complete_input'], 'complete_label':row['complete_label'], 'generated_input':  row['generated_input'], 'generated_label':row['generated_label']})
        else:
            rows_not_match_complete_input.append({'id': row['id'], 'complete_input':row['complete_input'], 'complete_label':row['complete_label'], 'generated_input':  row['generated_input'], 'generated_label':row['generated_label']})

df_not_match_complete_input = pd.concat([df_not_match_complete_input,pd.DataFrame(rows_not_match_complete_input)],ignore_index=True)
df_not_match_fully = pd.concat([df_not_match_fully,pd.DataFrame(rows_not_match_fully)], ignore_index=True)


print("\nDataframe representing those rows in which sentiment of generated text doesn't match with any given text \nGenerated text's sentiment labels doesn't match with either of the incomplete or complete given inputs")
print(df_not_match_fully)

# print('\nDataframe representing those rows in which sentiment of given incomplete text = sentiment of generated text, BUT sentiment of given complete text IS NOT EQUAL TO sentiment of generated text')
# print(df_not_match_complete_input)