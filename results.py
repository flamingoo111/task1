import pandas as pd
import sqlite3

def Results(path, connection, cursor):
  df_orders3 = list(map(list, pd.read_excel(path, sheet_name='Results').values))
  errors = []
  for i in df_orders3:
    try:
      cursor.execute(f'INSERT INTO events (campaign_id, email, time) VALUES ("{i[2]}", "{i[1]}", "{i[7]}")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
    try:
      cursor.execute(f'INSERT INTO templates (name, modified_date, envelope_sender) VALUES ("{i[3]}", '
                     f'"{i[6]}", "{i[1].split("@")[0]}")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
    try:
      cursor.execute(f'INSERT INTO results (user_id, email, first_name, send_date, modified_date, status) '
                     f'VALUES ((SELECT id FROM users WHERE username = "{i[1].split("@")[0]}"),'
                     f' "{i[2]}", "{i[1].split("@")[0]}", "{i[6]}", "{i[6]}", "NOT FOUND")')
    except (sqlite3.Warning, BaseException) as e:
      errors.append([e, i])
  print(len(errors))
  [print(f'{i} \n') for i in errors]
  if not errors:
     print('WITHOUT ERRORS. OK')
  else:
     print(f'{len(errors)} / {len(df_orders3) * 3} errors found.')
  connection.commit()