import argparse
import json
import os
import sqlite3

import pandas as pd

from utils import set_env, generate_combined_prompts_one

set_env()
import openai


def decouple_question_schema(datasets, db_root_path):
    for data in datasets:
        yield data['question'], db_root_path + data['db_id'] + '/' + data['db_id'] +'.sqlite', data['evidence'], data['SQL']


def gpt_request(engine, prompt, max_tokens, temperature, stop):
    print('requesting gpt...', end='', flush=True)
    try:
        result = openai.ChatCompletion.create(
            engine=engine,
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=max_tokens, temperature=temperature, stop=stop)
    except Exception as e:
        result = 'error:{}'.format(e)
    print('gpt response received')
    return result


def get_more_questions(engine, question, db_path, knowledge, use_knowledge):
    prompt = f"""
        I would like to test a student's ability to write SQL queries based on the tables above.
        The following delimited by triple backticks is the question I gave: ```{question}```.
        Please write another question for me so that the answer to your question 
        is just the same as the answer to my question, but the descriptions are drastically different.
        The response should be a single sentence of your question.
    """
    #print(prompt)
    plain_result = gpt_request(engine=engine, prompt=prompt, max_tokens=256, temperature=0, stop=['--', '\n\n', ';', '#'])
    try:
        ret = plain_result['choices'][0]['message']['content']
    except Exception as e:
        print(plain_result)
        raise ValueError('error request return')
    return ret

def get_answer(engine, question, db_path, knowledge, use_knowledge, fewshots=None):
    if use_knowledge:
        prompt = generate_combined_prompts_one(db_path, question, knowledge, fewshots)
    else:
        prompt = generate_combined_prompts_one(db_path, question, None, fewshots)
    plain_result = gpt_request(engine=engine, prompt=prompt, max_tokens=256, temperature=0, stop=['--', '\n\n', ';', '#'])
    try:
        ret = plain_result['choices'][0]['message']['content']
    except Exception as e:
        print(plain_result)
        raise ValueError('error request return')
    return ret

def execute_sql(sql, db_path):
    conn = sqlite3.connect(db_path, timeout=60)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        result = []
    return result

def encode_sql(sql):
    weird_char = '\xfe'
    return sql.replace('\n', weird_char)

def decode_sql(sql):
    weird_char = '\xfe'
    return sql.replace(weird_char, '\n')
    

def run(engine, question, db_path, knowledge, use_knowledge, ground_truth, fewshots, use_fewshots):
    content = get_more_questions(engine, question, db_path, knowledge, use_knowledge)
    questions = [question, content]
    
    results = []
    answers = []
    for q in questions:
        if use_fewshots:
            fs = fewshots[db_path.split('/')[-1][:-7]]
            answer = get_answer(engine, q, db_path, knowledge, use_knowledge, fs)
        else:
            answer = get_answer(engine, q, db_path, knowledge, use_knowledge)
        answers.append(answer)
        result = set(execute_sql(answer, db_path))
        results.append(result)
    
    return {
        'question': question,
        'another_question': content,
        'answer': encode_sql(answers[0]),
        'another_answer': encode_sql(answers[1]),
        'valid': int(results[0] == results[1]),
        'empty': int(len(results[0]) == 0),
        'correct': int(results[0] == set(execute_sql(ground_truth, db_path))),
    }

def get_few_shots_prompt(data):
    examples = []
    for i, item in enumerate(data):
        examples.append(f'-- Example {i}:\n---- Question: {item["question"]}\n---- Answer: {item["SQL"]}')
    return '\n'.join(examples)

def extract_few_shots(d):
    x = [{'rank': i, 'db': item['db_id']} for i, item in enumerate(d)]
    df = pd.DataFrame(x)
    s = df.groupby('db').agg({'rank': ['min', 'max']})
    s.columns = s.columns.get_level_values(1)
    s = s.to_dict(orient='index')
    fs = {}
    data = []
    for i in s:
        y = s[i] 
        fs[i] = get_few_shots_prompt(d[y['min']: y['min']+3])
        data = data + d[y['min']+3: y['max']+1]
    return fs, data


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--engine', type=str, default='gpt-35-turbo')
    args_parser.add_argument('--qa_path', type=str, default='data/dev.json')
    args_parser.add_argument('--db_root_path', type=str, default='data/dev_databases/')
    args_parser.add_argument('--results_filename', type=str, required=True, default='results/')
    args_parser.add_argument('--use_knowledge', action='store_true', default=False)
    args_parser.add_argument('--use_fewshots', action='store_true', default=False)
    args_parser.add_argument('--limit', type=int, default=100)
    args = args_parser.parse_args()
    
    qa_data = json.load(open(args.qa_path, 'r'))
    few_shots, qa_data = extract_few_shots(qa_data)
    assert len(qa_data) >= args.limit, f"args.limit {args.limit} exceeds all data"
    qa_data = qa_data[:args.limit]

    if os.path.exists(args.results_filename):
        df = pd.read_csv(args.results_filename)
        results = df.to_dict('records')
    else:
        results = []
    for i, (question, db_path, knowledge, ground_truth) in enumerate(decouple_question_schema(datasets=qa_data, db_root_path=args.db_root_path)):
        if i < len(results):
            continue
        r = run(args.engine, question, db_path, knowledge, args.use_knowledge, ground_truth, few_shots, args.use_fewshots)
        results.append(r)
        df = pd.DataFrame(results)
        print(i, df[['valid', 'empty', 'correct']])
        df.to_csv(args.results_filename, index=False, lineterminator='\r\n')
