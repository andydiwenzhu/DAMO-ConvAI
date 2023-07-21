import pandas as pd

if __name__ == '__main__':
    df1 = pd.read_csv('results/results_with_knowledge_fewshots.csv')
    df2 = pd.read_csv('results/results_with_knowledge.csv')
    print(df1)
    print(df2)
    inner = df1[['question']].merge(df2[['question']], how='inner')
    print(inner)