import pandas as pd
import sqlite3
from pathlib import Path


def Users(path, connection, cursor):
  df_orders = list(map(lambda x:  [f'{x[1]}'] + [f'{x[1]}'] + [f'{x[0]}'], list(map(list, pd.read_excel(path, sheet_name='Users', usecols=['name', 'email']).values))))
  df_orders = list(map(lambda x: [f'{str(x)}'] + df_orders[int(x) - 1], [i for i in range(1, len(df_orders) + 1)]))
  df_orders = pd.DataFrame(df_orders, columns=['id','first_name','last_name','email'])

  def insert_data(df, table_name):
    quantity_error = 0
    x = df.keys().values.tolist()
    for i in df.values:

      list1 = pd.DataFrame([list(i)], columns=x)
      try:
        pd.DataFrame(list1).to_sql(table_name, con=connection, if_exists='append', index=False)
      except Exception as e:
        print(e)
        quantity_error += 1
    return quantity_error

  quantity_error = insert_data(df_orders, 'targets')
  if not quantity_error:
    print('WITHOUT ERRORS. OK')
  else:
    print(f'errors:{quantity_error}, successful: {len(df_orders)}')
  connection.commit()

if __name__ == "__main__":
  path = Path("gophish.db")
  path2 = Path("D:/all_data_truncated.xlsx")
  connection = sqlite3.connect(path)
  cursor = connection.cursor()
  Users(path2, connection, cursor)
  connection.close()