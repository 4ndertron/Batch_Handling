import pandas as pd

d1 = {
    'one': [1., 2., 3., 4.],
    'two': [4., 3., 2., 1.]
}

d2 = {
    'three': [4., 3., 2., 1.],
    'four': [1., 2., 3., 4.]
}

sheets = [d1, d2]

df1 = pd.DataFrame(d1)
df2 = pd.DataFrame(d2)

with pd.ExcelWriter('sheet_export.xlsx') as writer:
    df1.to_excel(writer, sheet_name='Sheet1')
    df2.to_excel(writer, sheet_name='Sheet2')
