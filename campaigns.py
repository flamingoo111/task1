import pandas as pd
import sqlite3
from pathlib import Path
from sqlalchemy.dialects.sqlite import insert
import numpy as np

def Campaigns(path, connection, cursor):
  df_orders = pd.read_excel(path, sheet_name='Campaigns')
  df_orders = df_orders.rename(columns={'title': 'name', 'started': 'created_date'})
  df_orders2 = list(map(lambda x: list(filter(None,x)), list(map(list, pd.read_sql('SELECT * FROM campaigns', connection).values))))
  index = list(map(lambda x: x[0], list(map(list, df_orders.values))))
  index2 = list(map(lambda x: x[0], df_orders2))
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

  quantity_error = insert_data(df_orders, 'campaigns')
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
  Campaigns(path2, connection, cursor)
  connection.close()