import psycopg2
import pandas as pd
from datetime import datetime

from settings import init_dbcon
from pathlib import Path

# Этот модуль обращается к таблице up_puco_export

path = Path(__file__).parents[0]

# Сегодняшняя дата в формате 20201231
dt0 = int(datetime.today().strftime("%Y%m%d"))

dt3 = pd.read_csv(path / r".\data\line_data.csv", sep=";", dtype="object")

dt3 = dt3["DateEnd"].iloc[-1]

""" Содержимое базы up_puco_export
a1 -  линия,              a2 - id,              a3 - id
a4 -  дата начала стопа,  а5 - время стопа,     а6 - время работы до стопа
a7 -  дата конца стопа,   а8 - время конца стопа
а9 -  № заказа,           а10 - № заказа,       а11 - дата(хз начала заказа)
а12 - смена,              а13 - неизвестно,     а14 - код пуко
а15 - неизвестно,         а16 - счетчик входа,  а17 - счетчик выхода
indpuco - время записи из контроллера
"""


def _up_puco_export_cache(dt, dt2):
    """Эта функция читает "кэшированный" файл базы данных.
    Быстрее работает с ранними датами, снижает нагрузку на БД"""

    df_lvl_0_cache = pd.read_csv(
        path / r".\data\line_data.csv", sep=";", dtype="object"
    )

    df_lvl_0_cache["DateStart"] = df_lvl_0_cache["DateStart"].astype(int)

    # возврат не всего файла, а только выборки по дате
    df_lvl_0_cache = df_lvl_0_cache.loc[
        (df_lvl_0_cache["DateStart"] >= int(dt))
        & (df_lvl_0_cache["DateStart"] <= int(dt2))
    ]

    return df_lvl_0_cache


def send_query(dtstart, dtend, mode="read"):
    """Этот запрос получает данные для df нулевого уровня для
    создания графиков линий, таблиц и фактической выработки"""

    columns = [
        "Line",  # a1
        "Order",  # a9
        "Counter IN",  # a16
        "Counter OUT",  # a17
        "Shift",  # a12
        "Stop Code",  # a14
        "DateStart",  # a4
        "TimeStart",  # a5
        "DateEnd",  # a7
        "TimeEnd",  # a8
    ]

    # если дата меньше сегодня и больше начала кэшированного фрейма,
    # то вернуть данные из кэша, если нет - из базы.
    # Если база недоступна - вернуть пустой df
    if dt0 > int(dtend) and int(dtstart) < int(dt3) and mode == "read":

        return _up_puco_export_cache(dtstart, dtend)

    else:

        try:
            cursor = init_dbcon()

            cursor.execute(
                """SELECT a1, a9, a16, a17, a12, a14, a4, a5, a7, a8
                FROM up_puco_export
                WHERE
                    CAST(a4 AS INT) >= '{}'
                    AND CAST(a4 AS INT) <= '{}'
                    AND CAST(a12 AS INT) > 0
                ORDER BY a4, a5 """.format(
                    dtstart, dtend
                )
            )

            df_lvl_0 = pd.DataFrame(cursor.fetchall(), columns=columns)

            return df_lvl_0
        except:

            return pd.DataFrame([], columns=columns)


""" Содержимое базы up_line_def
nr_line - int. хз,      fc_line - название линии,       prod_order - номер заказа,
shift - смена(0,1,2),   starus_line - стоп/старт,       puco_need - хз,
puco_code - хз,         counter_start - счетчик входа,  counter_end - счетчик выхода,
stop_time - время стоп, puco_string - стоп-код,         qv_name - хз,
id_worker - хз,         id_master - хз,                 local_name - имя линии
"""


def cont_query():
    """Эта функция получает данные о состоянии линии для
    создания графика непрерывной работы линии"""

    try:
        cursor = init_dbcon()

        cursor.execute(
            """SELECT
                fc_line, prod_order, shift, starus_line, puco_need,
                counter_start, counter_end, stop_time, puco_string
            FROM up_line_def"""
        )

        df_query = pd.DataFrame(cursor.fetchall())

        df_query.columns = [
            "line",  # fc_line
            "order",  # prod_order
            "shift",  # shift
            "status",  # starus_line
            "puco_need",  # puco_need
            "counter in",  # counter_start
            "counter out",  # counter_end
            "stop_time",  # stop_time
            "code",  # puco_string
        ]

        return df_query

    except:

        return pd.read_csv(
            path / r".\data\test_cont_table.csv", sep=";", dtype="object"
        )


def cont_material(orderno, usedb=True):
    """Этот запрос обращается в as_line_speed и as_material_data
    и передает наименование заказа по его номеру. usedb=True - обращение к базе.
    Можно отключить для ускорения работы"""

    # сначала попытаться найти необходимый интекс в файл,
    # если не найден, обратиться к базе
    try:
        df_indexes = pd.read_csv(
            path / r".\data\as_line_speed.csv", sep=";", dtype="object"
        )

        df_indexes = df_indexes.loc[df_indexes["Order"] == orderno]
        index = df_indexes["Index"].iloc[0]

    except IndexError:

        if usedb:
            try:
                # инициализовать подключение
                cursor = init_dbcon()

                cursor.execute(
                    """SELECT product_id
                    FROM as_line_speed 
                    WHERE po_id = '{}' """.format(
                        str(orderno)
                    )
                )

                index = cursor.fetchall()[0][0]

            except:

                index = "index not found"
        else:
            index = "index not found"

    # по индексу получить его описание. Сначала посмотреть в файле,
    # если не найдено, обратиться в таблицу
    try:
        df_orders = pd.read_csv(
            path / r".\data\as_material_data.csv", sep=";", dtype="object"
        )

        df_orders = df_orders.loc[df_orders["Index"] == index]

        # обрезать имя до 30 символов, если надо
        return cut_description(df_orders["Holding Name"].iloc[0])

    except IndexError:

        if usedb:
            try:

                cursor.execute(
                    """SELECT the_name_of_the_holding_company
                    FROM as_material_dat
                    WHERE article = '{}' """.format(
                        index
                    )
                )

                result = cursor.fetchall()[0][0]

                return cut_description(result)

            except:

                return "description not found"
        else:
            return "description not found"


def cut_description(result):
    """Эта функция обрезает длинное имя заказа до 30 символов"""

    if len(result) > 30:
        result = result[:30] + "..."

    return result


if __name__ == "__main__":

    dt = "20201001"
    dt2 = "20201205"

    print(cont_material("00901"))
