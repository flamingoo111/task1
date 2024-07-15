import pandas as pd
import sqlite3

def Users(path, connection, cursor):
  df_orders2 = list(map(list, pd.read_excel(path, sheet_name='Users').values))
  spisok = list(set(list(map(lambda x: x[0], list(
    map(list, pd.read_excel(path, sheet_name='Users', usecols=['function']).values))))))
  errors = []
  for i in spisok:
    try:
      cursor.execute(f'INSERT INTO roles (slug, name) VALUES ("{str(i).lower()}", "{str(i)}")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
  print(len(errors))
  [print(f'{i} \n') for i in errors]
  if not errors:
    print('WITHOUT ERRORS. OK')
  else:
    print(f'{len(errors)} / {len(spisok)} errors found.')
  errors = []
  for i in df_orders2:
    try:
      cursor.execute(f'INSERT INTO users (username, api_key, role_id, last_login) VALUES ("{i[1]}", "zero API: {i[1]}"'
                     f', (SELECT id FROM roles WHERE name = "{i[2]}"), "{i[3]}")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
    try:
      cursor.execute(f'INSERT INTO targets (first_name, email) VALUES ("{i[1]}", "{i[0]}")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
  print(len(errors))
  [print(f'{i} \n') for i in errors]
  if not errors:
     print('WITHOUT ERRORS. OK')
  else:
     print(f'{len(errors)} / {len(df_orders2)} errors found.')
  connection.commit()