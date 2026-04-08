import pandas as pd

df = pd.read_excel('mapping_rule 재설계.xlsx', sheet_name=None)
print('--- Excel Structure ---')
for k, v in df.items():
    print(f'\nSheet: {k}')
    v = v.dropna(how='all').dropna(axis=1, how='all')
    for index, row in v.iterrows():
        print(row.dropna().tolist())
