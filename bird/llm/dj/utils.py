import argparse
import os
import sqlite3
import pandas as pd


def nice_look_table(column_names: list, values: list):
    rows = []
    # Determine the maximum width of each column
    widths = [max(len(str(value[i])) for value in values + [column_names]) for i in range(len(column_names))]

    # Print the column names
    header = ''.join(f'{column.rjust(width)} ' for column, width in zip(column_names, widths))
    # print(header)
    # Print the values
    for value in values:
        row = ''.join(f'{str(v).rjust(width)} ' for v, width in zip(value, widths))
        rows.append(row)
    rows = "\n".join(rows)
    final_output = header + '\n' + rows
    return final_output

def generate_schema_prompt(db_path, num_rows=None):
    # extract create ddls
    '''
    :param root_place:
    :param db_name:
    :return:
    '''
    full_schema_prompt_list = []
    conn = sqlite3.connect(db_path)
    # Create a cursor object
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    schemas = {}
    for table in tables:
        if table == 'sqlite_sequence':
            continue
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='{}';".format(table[0]))
        create_prompt = cursor.fetchone()[0]
        schemas[table[0]] = create_prompt
        if num_rows:
            cur_table = table[0]
            if cur_table in ['order', 'by', 'group']:
                cur_table = "`{}`".format(cur_table)

            cursor.execute("SELECT * FROM {} LIMIT {}".format(cur_table, num_rows))
            column_names = [description[0] for description in cursor.description]
            values = cursor.fetchall()
            rows_prompt = nice_look_table(column_names=column_names, values=values)
            verbose_prompt = "/* \n {} example rows: \n SELECT * FROM {} LIMIT {}; \n {} \n */".format(num_rows, cur_table, num_rows, rows_prompt)
            schemas[table[0]] = "{} \n {}".format(create_prompt, verbose_prompt)

    for k, v in schemas.items():
        full_schema_prompt_list.append(v)

    schema_prompt = "\n\n".join(full_schema_prompt_list)

    return schema_prompt

def generate_comment_prompt(question, knowledge=None, fewshots=None):
    pattern_prompt_no_kg = "-- Using valid SQLite, answer the following questions for the tables provided above."
    pattern_prompt_no_kg_few_shots = '-- Refer the following three examples and answer the last question using valid SQLite based on the tables provide above.'
    pattern_prompt_kg = "-- Using valid SQLite and understading External Knowledge, answer the following questions for the tables provided above."
    pattern_prompt_kg_few_shots = '-- Refer the following three examples and answer the last question by understanding the External Knowledge and using valid SQLite based on the tables provide above.'
    # question_prompt = "-- {}".format(question) + '\n SELECT '
    question_prompt = "-- {}".format(question)
    knowledge_prompt = "-- External Knowledge: {}".format(knowledge)

    if not fewshots:
        if not knowledge:
            result_prompt = pattern_prompt_no_kg + '\n' + question_prompt
        else:
            result_prompt = knowledge_prompt + '\n' + pattern_prompt_kg + '\n' + question_prompt
    else:
        if not knowledge:
            result_prompt = pattern_prompt_no_kg_few_shots + '\n' + fewshots + '\n-- Last Question: ' + question_prompt[3:]
        else:
            result_prompt = knowledge_prompt + '\n' + pattern_prompt_kg_few_shots + '\n' + fewshots + '\n-- Last Question: ' + question_prompt[3:]

    return result_prompt


def generate_combined_prompts_one(db_path, question, knowledge=None, fewshots=None):
    schema_prompt = generate_schema_prompt(db_path, num_rows=None) # This is the entry to collect values
    comment_prompt = generate_comment_prompt(question, knowledge, fewshots)

    combined_prompts = schema_prompt + '\n\n' + comment_prompt + '\nThe response should be a valid SQL query.'
 
    return combined_prompts

def evaluate_result(results_filename):
    df = pd.read_csv(results_filename)
    #correct = df['correct'].sum() / len(df)
    non_empty_correct = df[(df['empty']==0) & (df['correct']==1)].shape[0] / len(df)
    #print('Total number of questions: {:d},'.format(len(df)), 'Correct: {:.2%},'.format(correct), 'Non-empty Correct: {:.2%}'.format(non_empty_correct))
    # vn = df[(df['valid']==1)&(df['empty']==0)]
    # ve = df[(df['valid']==1)&(df['empty']==1)]
    # inn = df[(df['valid']==0)&(df['empty']==0)]
    # ie = df[(df['valid']==0)&(df['empty']==1)]
    # print('Valid-Nonempty: {:.2%}'.format(len(vn)/len(df)), 'Precision: {:.2%}'.format(vn['correct'].sum()/len(vn)))
    # print('Valid-Empty: {:.2%}'.format(len(ve)/len(df)), 'Precision: {:.2%}'.format(1 - ve['correct'].sum()/len(ve)))
    # print('Invalid-Nonempty: {:.2%}'.format(len(inn)/len(df)), 'Precision: {:.2%}'.format(1 - inn['correct'].sum()/len(inn)))
    # print('Invalid-Empty: {:.2%}'.format(len(ie)/len(df)), 'Precision: {:.2%}'.format(1 - ie['correct'].sum()/len(ie)))
    df['valid'] = df['valid'] * (1-df['empty'])

    tp = len(df[(df['valid']==1)&(df['correct']==1)])
    tn = len(df[(df['valid']==0)&(df['correct']==0)])
    fp = len(df[(df['valid']==1)&(df['correct']==0)])
    fn = len(df[(df['valid']==0)&(df['correct']==1)])

    print('Results:', results_filename.split('/')[-1],
          '\nNumber of Questions: {:d},'.format(len(df)),
          '\nOriginal Precision: {:.2%}'.format(non_empty_correct),
          '\nOriginal F-score: {:.2%}'.format(2*non_empty_correct/(non_empty_correct+1)),
          '\nPrecision: {:.2%}'.format(tp/(tp+fp)),
          '\nRecall: {:.2%}'.format(tp/(tp+fn)),
          '\nF-score: {:.2%}'.format(2/(2+(fp+fn)/tp)),
          '\nTrue Neg Rate: {:.2%}'.format(tn/(tn+fp)))



if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--results_filename', type=str, default='results/results_with_knowledge.csv')
    args = args_parser.parse_args()
    evaluate_result(args.results_filename)
