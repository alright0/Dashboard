import psycopg2

from settings import init_dbcon
# Этот модуль обращается к таблице up_puco_export



""" Содержимое базы up_puco_export
a1 -  линия,              a2 - id,              a3 - id
a4 -  дата начала стопа,  а5 - время стопа,     а6 - время работы до стопа
a7 -  дата конца стопа,   а8 - время конца стопа
а9 -  № заказа,           а10 - № заказа,       а11 - дата(хз начала заказа)
а12 - смена,              а13 - неизвестно,     а14 - код пуко
а15 - неизвестно,         а16 - счетчик входа,  а17 - счетчик выхода
indpuco - время записи из контроллера
"""

def send_query(dtstart, dtend):
    """ Этот запрос получает данные для df нулевого уровня для 
    создания графиков линий, таблиц и фактической выработки """
    
    init_dbcon()
    
    cursor.execute(
        "SELECT \
            a1, a9, a16, a17, a12, a14, a4, a5, a7, a8\
        FROM \
            up_puco_export \
        WHERE \
            CAST(a4 AS INT) >= '" + dtstart + "' \
            AND CAST(a4 AS INT) <= '" + dtend + "' \
            AND CAST(a12 AS INT) > 0 \
        ORDER BY a4, a5 ")

    return cursor.fetchall()


""" Содержимое базы up_line_def
nr_line - int. хз,      fc_line - название линии,       prod_order - номер заказа,
shift - смена(0,1,2),   starus_line - стоп/старт,       puco_need - хз,
puco_code - хз,         counter_start - счетчик входа,  counter_end - счетчик выхода,
stop_time - время стоп, puco_string - стоп-код,         qv_name - хз,
id_worker - хз,         id_master - хз,                 local_name - имя линии
"""

def cont_query2(line):
    """ Эта функция получает данные о состоянии линии для 
    создания графика непрерывной работы линии """

    init_dbcon()

    cursor.execute(
        "SELECT \
            fc_line,prod_order,shift,starus_line,puco_need,counter_start,counter_end,stop_time,puco_string\
        FROM \
            up_line_def \
        WHERE \
            fc_line = '" + line + "'")

    return cursor.fetchall()

def cont_query():
    """ Эта функция получает данные о состоянии линии для 
    создания графика непрерывной работы линии"""

    init_dbcon()

    cursor.execute(
        "SELECT \
            fc_line,prod_order,shift,starus_line,puco_need,counter_start,counter_end,stop_time,puco_string\
        FROM \
            up_line_def")

    return cursor.fetchall()

def cont_material(orderno):
    """Этот запрос обращается в as_line_speed и as_material_data 
    и передает наименование заказа по его номеру"""

    init_dbcon()

    cursor.execute(
        "SELECT \
            product_id \
        FROM \
            as_line_speed \
        WHERE \
            po_id = '" + str(orderno) + "'")


    article = cursor.fetchall()[0]


    cursor.execute(
        """SELECT
            the_name_of_the_holding_company
        FROM
            as_material_data
        WHERE
            article = '""" + article[0] + "'")

    result = cursor.fetchall()[0][0]


    
    return cut_description(result) 

def cut_description(result):
    """Эта функция обрезает длинное имя заказа до 30 символов"""
    
    if len(result) > 30:
        result = result[:30] + '...'

    return result