import pandas as pd
import sqlite3
from pathlib import Path
import campaigns
import results
import users

if __name__ == "__main__":
  try:
    path = Path(input('Введите абсолютный или относительный путь базы данных: '))
    path2 = Path(input('Введите абсолютный или относительный путь файла формата excel: '))
    if path.is_file():
      print("Database exists and is a file.")
      if path2.is_file():
        print("Excel exists and is a file.")
        connection = sqlite3.connect(path)
        cursor = connection.cursor()
        campaigns.Campaigns(path2, connection, cursor)
        users.Users(path2, connection, cursor)
        results.Results(path2, connection, cursor)
        connection.commit()
        connection.close()
      else:
        raise BaseException('Oops, this file EXCEL not exists or happened error. Try again.')
    else:
      raise BaseException('Oops, this file DATABASE not exists or happened error. Try again.')
  except BaseException as e:
    print(e)
