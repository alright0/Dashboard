from pathlib import Path
from datetime import date

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from datetime import timedelta

import settings
from pgconn import cont_material

path = Path(__file__).parents[0]


def ibea_orders():
    """ Эта функция добавляет описание к заказам по номерам заказов"""

    df_ibea_orders = pd.read_csv(path / "data/IBEA/full_commulated.csv", sep=";")

    order_set = set(df_ibea_orders["Order"])

    return dict(zip(order_set, [cont_material(_, usedb=False) for _ in order_set]))


def _make_df():

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

    # словарь сопоставлений заказов и описаний к ним
    descript_dict = ibea_orders()

    # маппинг описаний к заказам
    df_ibea_raw["Description"] = df_ibea_raw["Order"].map(descript_dict)

    # средний процент брака
    df_ibea_raw["Percent"] = df_ibea_raw["Rejected"] / df_ibea_raw["Total"] * 100

    # разметка смен. Возвращение True и False
    df_ibea_raw["Shift"] = (df_ibea_raw["Time End"] < shift_end) & (
        df_ibea_raw["Time End"] > shift_start
    )

    # маппинг смен
    df_ibea_raw["Shift"] = df_ibea_raw["Shift"].apply(lambda x: 1 if x else 2)

    # отбрасывание процентов больше 10. В случае больше 10, скорее всего
    df_ibea_raw = df_ibea_raw.loc[df_ibea_raw["Percent"] <= 10]

    df_ibea_raw.loc[df_ibea_raw["Shift"] == 2, "Date End"] = df_ibea_raw[
        "Date End"
    ] - timedelta(days=1)

    return df_ibea_raw


def ibea_stat(orderno):
    """Эта функция обрабатывает файлы статистики камеры и возвращает df для создания таблицы"""

    df_ibea_raw = _make_df()

    # отрезание выборок менее N шт и фильтрация по номеру заказа
    df_ibea_raw = df_ibea_raw.loc[
        (df_ibea_raw["Total"] > 1000) & (df_ibea_raw["Order"] == orderno)
    ]

    # сводная
    df_table = pd.pivot_table(
        df_ibea_raw,
        values=["Total", "Rejected", "Percent"],
        index=[
            "Date End",
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

    df_table = df_table.sort_values(
        by=["Date End"],
    )

    print(df_table)

    return df_table


def ibea_date(dt, dt2):

    dt = pd.to_datetime(dt)
    dt2 = pd.to_datetime(dt2)

    df_ibea_raw = _make_df()

    # отрезание выборок менее N шт и фильтрация по номеру заказа
    df_ibea_raw = df_ibea_raw.loc[
        (df_ibea_raw["Total"] > 1000)
        & (df_ibea_raw["Date Start"] > dt)
        & (df_ibea_raw["Date End"] < dt2)
    ]

    # сводная
    df_table = pd.pivot_table(
        df_ibea_raw,
        values=["Total", "Rejected", "Percent"],
        index=[
            "Date End",
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

    # df_table.loc[df_table["Shift"] == 2, "Date End"] = df_table["Date End"] - timedelta(
    #    days=1
    # )

    df_table = df_table.sort_values(
        by=["Date End"],
    )

    print(df_table)

    return df_table


if __name__ == "__main__":

    def _ibea_cal():

        # ibea_stat("00908")
        ibea_date(date(2020, 12, 1), date(2020, 12, 4))

    _ibea_cal()
