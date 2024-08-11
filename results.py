import pandas as pd
import sqlite3
from pathlib import Path

def Results(path, connection, cursor):
  df_orders_events = list(map(lambda x: [f'{x[1]}'] + [f'{x[0]}'] + [f'{x[2]}'],
                              list(map(list, pd.read_excel(path, sheet_name='Results',
                              usecols=['test_id', 'recipient_email', 'opened_at']).values))))
  df_orders_events = list(map(lambda x: [f'{str(x)}'] + df_orders_events[int(x) - 1],
                              [i for i in range(1, len(df_orders_events) + 1)]))
  print(len(df_orders_events))
  df_orders_events = pd.DataFrame(df_orders_events, columns=['id', 'campaign_id', 'email', 'time'])
  df_orders_templates = list(map(lambda x: [f'{x[1]}'] + [f'{x[2]}'] + [f'{str(x[0]).split("@")[0]}'],
                                 list(map(list, pd.read_excel(path, sheet_name='Results',
                                 usecols=['template', 'opened_at', 'recipient_email']).values))))
  df_orders_templates = list(map(lambda x: [f'{str(x)}'] + df_orders_templates[int(x) - 1],
                                 [i for i in range(1, len(df_orders_templates) + 1)]))
  print(len(df_orders_templates))
  df_orders_templates = pd.DataFrame(df_orders_templates, columns=['id', 'name', 'modified_date', 'envelope_sender'])
  df_orders_results = list(map(lambda x: [f'{x[1]}'] + [f'{x[0]}'] + [f'{str(x[0]).split("@")[0]}'] +
                                         [f'{str(x[0]).split("@")[0]}'] + ['NO STATUS'] + [f'{x[2]}'] + [f'{x[2]}'],
                                         list(map(list, pd.read_excel(path, sheet_name='Results',
                                         usecols=['test_id', 'recipient_email', 'opened_at']).values))))
  df_orders_results = list(map(lambda x: [f'{str(x)}'] + df_orders_results[int(x) - 1],
                                 [i for i in range(1, len(df_orders_results) + 1)]))
  print(len(df_orders_results))
  df_orders_results = pd.DataFrame(df_orders_results, columns=['id', 'campaign_id', 'email', 'first_name', 'last_name', 'status',
                                                                   'send_date', 'modified_date'])
  def insert_data(df, table_name):
    quantity_error = 0
    x = df.keys().values.tolist()
    for i in df.values:
      print(i[0])
      try:
        pd.DataFrame([list(i)], columns=x).to_sql(table_name, con=connection, if_exists='append', index=False)
      except Exception as e:
        print(e, i)
        quantity_error += 1
    return quantity_error

  quantity_error = insert_data(df_orders_events, 'events')
  if not quantity_error:
    print('WITHOUT ERRORS. OK')
  else:
    print(f'errors:{quantity_error}, successful: {len(df_orders_events)}')


  quantity_error = insert_data(df_orders_templates, 'templates')
  if not quantity_error:
    print('WITHOUT ERRORS. OK')
  else:
    print(f'errors:{quantity_error}, successful: {len(df_orders_templates)}')


  quantity_error = insert_data(df_orders_results, 'results')
  if not quantity_error:
    print('WITHOUT ERRORS. OK')
  else:
    print(f'errors:{quantity_error}, successful: {len(df_orders_results)}')



if __name__ == "__main__":
  path = Path("gophish.db")
  path2 = Path("D:/all_data_truncated.xlsx")
  connection = sqlite3.connect(path)
  cursor = connection.cursor()
  Results(path2, connection, cursor)
  connection.commit()
  connection.close()