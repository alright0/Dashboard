from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

import pgconn
import settings

__path = Path(__file__).parents[0]

def get_cache_lvl_0():
    """Эта функция кэширует часть таблицы up_puco_export
    (ту часть, которая сейчас используется на при построениее графиков) """
    
    dt = 20200101

    dt2 = datetime.today() - timedelta(days=1)
    dt2 = int(datetime.strftime(dt2, format='%Y%m%d'))

    try:
        df_lvl_0 = pgconn.send_query(dt,dt2,'write')

        df_lvl_0['cache'] = 'cached'

    except:
        print('DB is not avalaible')
        df_lvl_0 = pd.DataFrame([])


    if not df_lvl_0.empty:
        df_lvl_0.to_csv( __path / 'data/line_data.csv', sep=';', index=False)
    else:
        print('DataFrame is empty. Check connection to the database')

    return df_lvl_0

def get_cache_as_line_speed():

    try:

        cursor=settings.init_dbcon()

        cursor.execute(
                    """SELECT po_id, product_id
                    FROM as_line_speed """)

        df_as_line_speed = pd.DataFrame(cursor.fetchall())

        df_as_line_speed.columns = ['Order', 'Index']

    except:
        print('DB is not avalaible')
        df_as_line_speed = pd.DataFrame([])

    if not df_as_line_speed.empty:
        df_as_line_speed.to_csv( __path / 'data/as_line_speed.csv', sep=';', index=False)
    else:
        print('DataFrame is empty. Check connection to the database')

def get_cache_as_material_data():
    """Эта функция кэширует таблицу as_material_data"""

    try:

        cursor=settings.init_dbcon()

        cursor.execute(
                    """SELECT article, the_name_of_the_holding_company
                    FROM as_material_data """)

        as_material_data = pd.DataFrame(cursor.fetchall())

        as_material_data.columns = ['Index', 'Holding Name']

    except:
        print('DB is not avalaible')
        as_material_data = pd.DataFrame([])


    if not as_material_data.empty:
        as_material_data.to_csv( __path / 'data/as_material_data.csv', sep=';', index=False)
    else:
        print('DataFrame is empty. Check connection to the database')


if __name__ == '__main__':

    get_cache_as_material_data()