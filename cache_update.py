from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

import shutil
import os
import pgconn
import settings

__path = Path(__file__).parents[0]


def get_cache_lvl_0():
    """Эта функция кэширует часть таблицы up_puco_export
    (ту часть, которая сейчас используется на при построениее графиков)"""

    dt = 20200101

    dt2 = datetime.today() - timedelta(days=1)
    dt2 = int(datetime.strftime(dt2, format="%Y%m%d"))

    try:
        df_lvl_0 = pgconn.send_query(dt, dt2, "write")

        df_lvl_0["cache"] = "cached"

    except:
        print("DB is not avalaible")
        df_lvl_0 = pd.DataFrame([])

    if not df_lvl_0.empty:
        df_lvl_0.to_csv(__path / "data/line_data.csv", sep=";", index=False)
        print("{}.data/line_data.csv saved!".format(__path))
    else:
        print("DataFrame is empty. Check connection to the 4CAN DB")

    return df_lvl_0


def get_cache_as_line_speed():

    try:

        cursor = settings.init_dbcon()

        cursor.execute(
            """SELECT po_id, product_id
                    FROM as_line_speed """
        )

        df_as_line_speed = pd.DataFrame(cursor.fetchall())

        df_as_line_speed.columns = ["Order", "Index"]

    except:
        print("DB is not avalaible")
        df_as_line_speed = pd.DataFrame([])

    if not df_as_line_speed.empty:
        df_as_line_speed.to_csv(__path / "data/as_line_speed.csv", sep=";", index=False)
    else:
        print("DataFrame is empty. Check connection to the database")


def get_cache_as_material_data():
    """Эта функция кэширует таблицу as_material_data"""

    try:

        cursor = settings.init_dbcon()

        cursor.execute(
            """SELECT article, the_name_of_the_holding_company
                    FROM as_material_data """
        )

        as_material_data = pd.DataFrame(cursor.fetchall())

        as_material_data.columns = ["Index", "Holding Name"]

    except:
        print("DB is not avalaible")
        as_material_data = pd.DataFrame([])

    if not as_material_data.empty:
        as_material_data.to_csv(
            __path / "data/as_material_data.csv", sep=";", index=False
        )
    else:
        print("DataFrame is empty. Check connection to the database")


def ibea_connect():
    """Эта функция обходит все камеры ibea и собирает с них аккумулированную
    статистику и раскладывает по заранее заготовленным папкам"""

    cols = [1, 2, 4, 5, 6, 8, 9]

    for line in settings.IBEA_ADDRESS:

        try:

            # создание директории с именем камеры, если она не существует
            if not os.path.exists(__path / f"data/IBEA/{line}"):
                os.makedirs(__path / f"data/IBEA/{line}")

            # директории копирования из камеры в локальную папку
            copyfrom = "//{}/ibea/statistics/z_cumulated.csv".format(
                settings.IBEA_ADDRESS.get(line)
            )
            copyto = __path / "data/IBEA/{}/z_cumulated.csv".format(line)

            # копирование
            shutil.copyfile(copyfrom, copyto)

            print(f"{line} copied")

            # чтение скопированного csv и обработка на месте.
            df = pd.read_csv(
                copyto,
                sep=";",
                encoding="ISO-8859-1",
                usecols=cols,
            )

            df.columns = [
                "Date Start",  # 1
                "Time Start",  # 2
                "Date End",  # 4
                "Time End",  # 5
                "Order",  # 6
                "Total",  # 8
                "Rejected",  # 9
            ]

            # идентификация фрейма - добавление имени линии
            df["line"] = line

            # исходные csv содержат пробелы перед датой, и, в принципе,
            # имеют не очень адекватное содержание. Здесь удаляются пробелы
            # перед датой или после
            df["Date Start"] = df["Date Start"].str.strip()
            df["Date End"] = df["Date End"].str.strip()

            # сохранение обработанного файла
            df.to_csv(copyto, sep=";", index=False, encoding="utf-8")

            print("saved")

        # возможно, здесь добавится логирование, а не принты
        except UnicodeDecodeError:

            print(f"{line} failed because of encoding")

        except:
            print(f"{line} failed ")

    _ibea_agregate()


def _ibea_agregate():
    """Эта функция собирает все обработанные csv файлы и сливает их в один.
    Не учитывает последние 10 лет работы камер за ненадобностью"""

    dfs = []

    # здесь сливаются все файлы в один
    for line in settings.IBEA_ADDRESS:

        pth = __path / "data/IBEA/{}/z_cumulated.csv".format(line)

        try:

            df = pd.read_csv(pth, sep=";")
            df = df.loc[
                pd.to_datetime(df["Date End"])
                >= pd.to_datetime("01.01.2020", format="%d.%m.%Y")
            ]

            dfs.append(df)
            print("{} added!".format(line))
        except FileNotFoundError:

            print("file {}.csv not found".format(line))

    # сливание всех файлов в один
    df_full = pd.concat(dfs)

    # сохранение
    df_full.to_csv(
        __path / "data/IBEA/full_commulated.csv", sep=";", index=False, encoding="utf-8"
    )


if __name__ == "__main__":

    # get_cache_lvl_0()
    # get_cache_as_line_speed()
    # get_cache_as_material_data()
    ibea_connect()