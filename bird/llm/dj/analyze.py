import argparse
import json
import os

import pandas as pd

from test import decode_sql, execute_sql, extract_few_shots
from utils import generate_schema_prompt

def sformat(s, th=80, con='\n\t\t'):
    lines = []
    while len(s) > th:
        pos = s[th:].find(' ')
        if pos == -1:
            break
        else:
            lines.append(s[:th+pos])
            s = s[th+pos+1:]
    lines.append(s)
    return con.join(lines)

def check_null_result(x):
    return x in ['[(None,)]', '[(0,)]', '[(0.0,)]']

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--engine', type=str, default='gpt-35-turbo')
    args_parser.add_argument('--qa_path', type=str, default='data/dev.json')
    args_parser.add_argument('--db_root_path', type=str, default='data/dev_databases/')
    args_parser.add_argument('--results_filename', type=str, required=True, default='results/results_with_knowledge.csv')
    args_parser.add_argument('--use_knowledge', action='store_true', default=False)
    args_parser.add_argument('--use_fewshots', action='store_true', default=False)
    args_parser.add_argument('--pick_one', type=int, default=0)
    args_parser.add_argument('--limit', type=int, default=100)
    args_parser.add_argument('--cache_path', type=str, default='')
    args_parser.add_argument('--force_update', action='store_true', default=False)
    
    args = args_parser.parse_args()

    if not args.force_update and os.path.exists(args.cache_path):
        df = pd.read_csv(args.cache_path)
    else:
        qa_data = json.load(open(args.qa_path, 'r'))
        few_shots, qa_data = extract_few_shots(qa_data)
        qa_data = qa_data[:args.limit]
        simple_qa_data = [{'q1': data['question'], 'db_path': args.db_root_path + data['db_id'] + '/' + data['db_id'] +'.sqlite', 'knowledge': data['evidence'], 'gt': data['SQL']} for data in qa_data]
        df1 = pd.DataFrame(simple_qa_data)
        df2 = pd.read_csv(args.results_filename)
        df = pd.concat([df1, df2], axis=1)
        print((df['question']==df['q1']).astype(int).sum())
        df = df[df['question']==df['q1']]

        if args.cache_path.endswith('fp.csv'):        
            df = df[(df['valid']==1)&(df['empty']==0)&(df['correct']==0)]
        elif args.cache_path.endswith('fn.csv'):
            df = df[(df['valid']==0)&(df['empty']==0)&(df['correct']==1)]
        else:
            raise ValueError(args.cache_path)
        print('total', len(df))
        df = df.iloc[:20]
        print('run egt...', len(df))
        df['egt'] = df.apply(lambda row: execute_sql(row['gt'], row['db_path']), axis=1)
        print('run e1...')
        df['e1'] = df.apply(lambda row: execute_sql(decode_sql(row['answer']), row['db_path']), axis=1)
        print('run e2...')
        df['e2'] = df.apply(lambda row: execute_sql(decode_sql(row['another_answer']), row['db_path']), axis=1)
        print('writing csv...')
        df.to_csv(args.cache_path, index=False)


    if args.cache_path.endswith('fp.csv'):  
        #df['null_result'] = df['e1'].apply(check_null_result)
        r = df.iloc[args.pick_one]
        p = generate_schema_prompt(r['db_path'])
        print(p)
        print('\n\n')
        if isinstance(r["knowledge"], str):
            print(f'Knowledge:\t{sformat(r["knowledge"])}\n')
        else:
            print("Knowledge:\tNone\n")
        print(f'Question:\t{sformat(r["question"])}\n')
        print(f'Ground_Truth:\t{sformat(r["gt"])}\n')
        print(f'Groud_Truth_Value:\t{r["egt"]}\n')
        print(f'GPT_Answer_Value:\t{r["e1"]}\n')
        s = "\t\t"+decode_sql(r["answer"]).replace("\n", "\n\t\t")
        print(f'GPT_Answer:\n{s}\n')
    elif args.cache_path.endswith('fn.csv'):
        r = df.iloc[args.pick_one]
        p = generate_schema_prompt(r['db_path'])
        print(p)
        print('\n\n')
        if isinstance(r["knowledge"], str):
            print(f'Knowledge:\t{sformat(r["knowledge"])}\n')
        else:
            print("Knowledge:\tNone\n")
        print(f'Question:\t{sformat(r["question"])}\n')
        s = "\t\t"+decode_sql(r["answer"]).replace("\n", "\n\t\t")
        print(f'GPT_Answer:\n{s}\n')
        print(f'GPT_Answer_Value:\t{r["e1"]}\n')
        print(f'AltQuestion:\t{sformat(r["another_question"])}\n')
        t = "\t\t"+decode_sql(r["another_answer"]).replace("\n", "\n\t\t")
        print(f'GPT_AltAnswer:\n{t}\n')
        print(f'GPT_AltAnswer_Value:\t{r["e2"]}\n')