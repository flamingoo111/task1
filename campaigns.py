import pandas as pd
import sqlite3

def Campaigns(path, connection, cursor):
  df_orders = list(map(list, pd.read_excel(path, sheet_name='Campaigns').values))
  errors = []
  for i in df_orders:
    try:
      cursor.execute(
        f'INSERT INTO campaigns (id, name, created_date) VALUES ("{str(i[0])}", "{str(i[1])}", "{str(i[2])}")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
  print(len(errors))
  [print(f'{i} \n') for i in errors]
  if not errors:
     print('WITHOUT ERRORS. OK')
  else:
     print(f'{len(errors)} / {len(df_orders)} errors found.')
  connection.commit()