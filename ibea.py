import os
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import psycopg2

import settings
from pgconn import cont_material

path = Path(__file__).parents[0]


def ibea_connect():
    """Эта функция обходит все камеры ibea и собирает с них аккумулированную
    статистику и раскладывает по заранее заготовленным папкам"""

    cols = [1, 2, 4, 5, 6, 8, 9]

    for line in settings.IBEA_ADDRESS:

        try:

            # создание директории с именем камеры, если она не существует
            if not os.path.exists(path / f"data/IBEA/{line}"):
                os.makedirs(path / f"data/IBEA/{line}")

            # директории копирования из камеры в локальную папку
            copyfrom = "//{}/ibea/statistics/z_cumulated.csv".format(
                settings.IBEA_ADDRESS.get(line)
            )
            copyto = path / "data/IBEA/{}/z_cumulated.csv".format(line)

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

        pth = path / "data/IBEA/{}/z_cumulated.csv".format(line)

        try:

            df = pd.read_csv(pth, sep=";")
            df = df.loc[
                pd.to_datetime(df["Date Start"])
                >= pd.to_datetime("01.01.2020", format="%d.%m.%Y")
            ]

            dfs.append(df)

        except FileNotFoundError:

            print("file {}.csv not found".format(line))

    # сливание всех файлов в один
    df_full = pd.concat(dfs)

    # сохранение
    df_full.to_csv(
        path / "data/IBEA/full_commulated.csv", sep=";", index=False, encoding="utf-8"
    )


def ibea_orders():
    """ Эта функция добавляет описание к заказам по номерам заказов"""

    df_ibea_orders = pd.read_csv(path / "data/IBEA/full_commulated.csv", sep=";")

    order_set = set(df_ibea_orders["Order"])

    return dict(zip(order_set, [cont_material(_, usedb=False) for _ in order_set]))


def ibea_stat(orderno):
    """Эта функция обрабатывает файлы статистики камеры и возвращает df для создания таблицы"""

    df_ibea_raw = pd.read_csv(path / "data/IBEA/full_commulated.csv", sep=";")

    # разметка по сменам - средний переход происходит
    # в период 7:50-8:00 и 19:50-20:00
    shift_start = pd.to_datetime("07:50:00", format="%H:%M:%S").time()
    shift_end = pd.to_datetime("19:50:00", format="%H:%M:%S").time()

    # обработка дат и времени
    df_ibea_raw["Date Start"] = pd.to_datetime(
        df_ibea_raw["Date Start"], format="%d.%m.%Y"
    )
    df_ibea_raw["Date End"] = pd.to_datetime(df_ibea_raw["Date End"], format="%d.%m.%Y")
    df_ibea_raw["Time Start"] = [
        time.time() for time in pd.to_datetime(df_ibea_raw["Time Start"].astype(str))
    ]
    df_ibea_raw["Time End"] = [
        time.time() for time in pd.to_datetime(df_ibea_raw["Time End"].astype(str))
    ]

    # отрезание выборок менее 1000 шт и фильтрация по номеру заказа
    df_ibea_raw = df_ibea_raw.loc[
        (df_ibea_raw["Total"] > 1000) & (df_ibea_raw["Order"] == orderno)
    ]

    # словарь сопоставлений заказов и описаний к ним
    descript_dict = ibea_orders()

    # маппинг описаний к заказам
    df_ibea_raw["Description"] = df_ibea_raw["Order"].map(descript_dict)

    # средний процент брака
    df_ibea_raw["Percent"] = df_ibea_raw["Rejected"] / df_ibea_raw["Total"] * 100

    # разметка смен. Возвращение True и False
    df_ibea_raw["Shift"] = (df_ibea_raw["Time Start"] < shift_end) & (
        df_ibea_raw["Time End"] > shift_start
    )

    # маппинг смен
    df_ibea_raw["Shift"] = df_ibea_raw["Shift"].apply(lambda x: 1 if x else 2)

    # отбрасывание процентов больше 10. В случае больше 10, скорее всего
    df_ibea_raw = df_ibea_raw.loc[df_ibea_raw["Percent"] <= 10]

    # сводная
    df_table = pd.pivot_table(
        df_ibea_raw,
        values=["Total", "Rejected", "Percent"],
        index=[
            "Date Start",
            "Shift",
            "Order",
            "Description",
            "line",
        ],
        aggfunc={
            "Total": np.sum,
            "Rejected": np.sum,
            "Percent": np.mean,
        },
    ).reset_index()

    # print(df_table)

    return df_table


if __name__ == "__main__":

    def _ibea_cal():

        # ibea_connect()
        # _ibea_agregate()
        # ibea_orders()

        # pth= path / r'.\ibea_test.csv'
        # df_test = pd.read_csv(pth,sep=';')
        ibea_stat("00864")

        # ibea_graph(ibea_stat())

    # _call_line()
    # _call_bar()
    _ibea_cal()
