import csv
import time
from calendar import monthrange
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import dash
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import pandas as pd
import plotly as plt
import plotly.figure_factory as ff
import plotly.graph_objects as go
import psycopg2
from dash.dash import no_update
from dash.dependencies import Input, Output

import settings
from pgconn import cont_material, cont_query, cut_description, send_query

lines = [
    'LZ-01', 'LZ-02', 'LZ-03',
    'LZ-04', 'LZ-05', 'LN-01',
    'LN-03', 'LL-01', 'LL-02',
    'LP-01'
    ]

# эта строка реализует относительные пути через pathlib
path = Path(__file__).parents[0]

df_codes = pd.read_csv(path / '.\Codes.csv', index_col=0, sep=';')
df_codes.reset_index(inplace=True)


def get_df_lvl_0(dt, dt2):
    """Эта функция создает 'сырой' df для последующей обработки"""

    df_lvl_0 = send_query(dt,dt2)

    #if not df_lvl_0.empty:

    # преобразование дата финиши из текста в дату
    df_lvl_0['DateEnd'] = pd.to_datetime(df_lvl_0['DateEnd'], format='%Y%m%d')

    # Преобразование даты старта из текста в дату
    df_lvl_0['DateStart'] = pd.to_datetime(df_lvl_0['DateStart'], format='%Y%m%d')

    # Смена
    df_lvl_0['Shift'] = pd.to_numeric(df_lvl_0['Shift'])

    # преобразование значения датчика входа в цифровой вид
    df_lvl_0['Counter IN'] = pd.to_numeric(df_lvl_0['Counter IN'])

    # преобразование значения датчика выхода в цифровой вид
    df_lvl_0['Counter OUT'] = pd.to_numeric(df_lvl_0['Counter OUT'])

    # форматирование кода остановки. Отрезание первых трех нулей
    df_lvl_0['Stop Code'] = df_lvl_0['Stop Code'].str[3:]

    # форматирование номера заказа. отрезание первых трех нулей
    df_lvl_0['Order'] = df_lvl_0['Order'].str[3:]

    # форматирование времени старта простоя
    df_lvl_0['TimeStart'] = df_lvl_0['TimeStart'].str[2:]
    df_lvl_0['TimeStart'] = (
        df_lvl_0['TimeStart'].str[:2] + ':' + 
        df_lvl_0['TimeStart'].str[2:4] + ':' + 
        df_lvl_0['TimeStart'].str[4:].replace('60','59'))

    # форматирование времени финиша простоя
    df_lvl_0['TimeEnd']=df_lvl_0['TimeEnd'].str[2:]
    df_lvl_0['TimeEnd']=(
        df_lvl_0['TimeEnd'].str[:2] + ':' + 
        df_lvl_0['TimeEnd'].str[2:4] + ':' + 
        df_lvl_0['TimeEnd'].str[4:].replace('60','59'))

    # конкатенция даты и времени начала остановки
    df_lvl_0['DateTimeStart'] = pd.to_datetime(
        df_lvl_0['DateStart'].astype(str) + ' ' + 
        df_lvl_0['TimeStart'])

    # конкатенация даты и времени конца остановки
    df_lvl_0['DateTimeEnd'] = pd.to_datetime(
        df_lvl_0['DateEnd'].astype(str) + ' ' + df_lvl_0['TimeEnd'].astype(str))

    # получение времени простоя из разницы дат начала и конца
    df_lvl_0['TimeStop'] = df_lvl_0['DateTimeEnd'] - df_lvl_0['DateTimeStart']

    # перевод времени из форматированного вида в строки
    df_lvl_0['TimeStopRaw'] = df_lvl_0['TimeStop'].astype('timedelta64[s]')

    # взятие времени остановок по модулю(необходимо, вероятно,
    # для нулевых значений
    df_lvl_0['TimeStopRaw'] = df_lvl_0['TimeStopRaw']
    df_lvl_0['TimeStopRaw'] = df_lvl_0['TimeStopRaw'].astype(int)

    return df_lvl_0

def get_df_line_lvl_1(df, line):
    """Эта функция формирует принимает датафрейм нулевого уровня(df_lvl_0) и
    создает фрейм первого уровня, который выступает списком данных для создания гарфика """

    #if not df.empty:

    df2 = df.loc[(df['Line'] == line) & (df['TimeStopRaw'] > 0)]
    
    #print(df2)

    # расчет разницы между предыдущим и следующим значениями 
    # для возможности рассчитать суммарный выпуск
    df2['Sheets'] = pd.to_numeric(df2['Counter OUT'].diff())

    # заполнить NaN нулями
    df2['Sheets'].fillna(0, inplace=True)

    # Значения меньше нуля заменить нулями(при переходе с заказа на заказ)
    df2['Sheets'].clip(lower=0, inplace=True) 

    #удаление ненужных столбцов 
    del df2['DateStart']
    del df2['TimeStart']
    del df2['DateEnd']
    del df2['TimeEnd']

    df3=df2.copy()

    df2['Stop Time'] = df2['DateTimeStart']
    df3['Stop Time'] = df3['DateTimeEnd']

    df2['Status'] = 'RUN'
    df3['Status'] = 'STOP'
    df2['Stop Code'] = 'RUN'

    df4 = [df2,df3]
    df4 = pd.concat(df4)
    
    # присвоение описания кодам остановок
    df4 = pd.merge(
        df4,
        df_codes,
        how='inner',
        left_on='Stop Code',
        right_on='code'
    )

    # удаление лишних описаний
    del df4['group_eng']
    del df4['group_ru']
    del df4['code']
    del df4['DateTimeStart']
    del df4['DateTimeEnd']
    del df4['TimeStop']
    del df4['TimeStopRaw']
    del df4['Sheets']
    
    # сортировка значений по дате, необходима для правильного расчета 
    # промежуточного выпуска и времени работы/стопа
    df4=df4.sort_values(by=['Stop Time'])

    df4['Minutes']=df4['Stop Time'].diff()
    df4['Minutes'].fillna(pd.Timedelta(days=0), inplace=True)

    df4['Sheets']= pd.to_numeric(df4['Counter OUT'].diff())
    df4['Sheets'].fillna(0, inplace=True)
    df4['Sheets'].clip(lower=0, inplace=True)

    # перевод времени в секунды. В дальнейшем выступает 
    # размером столбца остановки по оси Х
    df4['TimeRaw']=df4['Minutes'].dt.seconds

    # фильтрация значений стопов. В данной базе стоп может быть нуленвым или отрицательным,
    # если остановка короткая. Это стоит фильтровать.
    df4 = df4.loc[(df4['TimeRaw'] > 0)]
    df4=df4.reset_index()

    # временный столбец, берет дату и смену и возвращает столбец типа "21.10.2020 1". 
    # Далее мержится с таким же столбцом из df c буквой смены, чтобы правильно 
    df4['letter'] = df4['Stop Time'].astype(str).str[:10] + ' ' + df4['Shift'].astype(str)
    
    # получение df с буквами смен
    df_letter = shift_let_list()
    
    # объединение основного df и df c буквами смен
    df4 = pd.merge(
        df4, df_letter,
        how='inner',
        left_on='letter',
        right_on='date')
    
    # удаление лишних значений
    del df4['letter_x']
    del df4['date']

    df4.rename(columns={'letter_y': 'letter'}, inplace=True)
    
    df4['Date'] = df4['Stop Time'].astype(str).str[:10]

    # повторная сортировка по дате, которая сбивается после смерживания
    df4=df4.sort_values(by=['Stop Time'])
    
    return df4

    #else:

    #    return df

def shift_let_list():
    # эта функция размечает смены(ABCD) в зависимости от даты и номера смены.
    # Сейчас ничего не принимает на вход и размечает весь 2020 год.
    # на выходе возвращает df для операции pd.merge

    # лист со списком дат
    datelist = []

    # Наполнение листа со списком дат для работы
    datelist = pd.date_range(
        start=date(2020, 1, 1),
        end=date(2022, 1, 1),
        freq='12H').tolist()

    # Константное значение порядка смен.
    LETTER = 'DADACDCDBCBCABAB'*94

    # Разметка смен в зависимости от четности элемента(чет = 1, нечет = 2)
    shift_list = [1 if shift % 2 == 0 else 2 for shift in range(len(datelist))]

    # Сборка конечного df
    df = pd.DataFrame(list(zip(datelist, shift_list, LETTER)))

    # Присвоение имен колонок для выходного df
    df.columns = [
        'dateraw',
        'shift',
        'letter',
    ]

    # переразметка даты. превращает дату в сторку и отрезает значение времени
    # далее конкатенация номера смены к дате.
    # Является ключевым полем(left inner join)
    df['date'] = (df['dateraw'].astype(str).str[:10] 
        + ' ' + df['shift'].astype(str))

    # удаление номера смены за ненадобностью
    del df['shift']

    # Возврат df
    return(df)

def rd_plan_csv():
    # Эта функция читает CSV плана производства

    plan_letter = path / r'Shift.csv'

    df_letter = pd.read_csv(plan_letter,  sep=';')
    df_letter.reset_index(inplace=True)

    return df_letter

def make_bar(df_line_lvl_1,indicat_df,line):
    """ Эта функция принимает df_line_lvl_1 и строит план-фактный график работы линий.
    НЕ ПОДХОДИТ ДЛЯ ЛИНИЙ LL и LP, поскольку к ним понятие плана в таком виде не применимо"""

    if not df_line_lvl_1.empty :
        line_name=df_line_lvl_1['Line'].iloc[0]
    
        # подготовка df для вывода на график: 4 смены "A B C D" и фактический выпуск
        ready_df=df_line_lvl_1[['Date','letter','Sheets']]

        # Группирование по букве для расчета фактических смен
        df_shift_fact = df_line_lvl_1[['Date','Shift','letter','Line']]

        month = df_line_lvl_1['Stop Time'].iloc[0].month
        year = df_line_lvl_1['Stop Time'].iloc[0].year
    
    else:
        line_name=line
        ready_dict = {
            'Date':[0,0,0,0],
            'letter': ['A','B','C','D'],
            'Sheets': [0,0,0,0],
        }

        shift_fact_dict = {
            'Date':[0,0,0,0],
            'Shift':[0,0,0,0],
            'letter': ['A','B','C','D'],
            'Line': [line,line,line,line],
        }

        ready_df=pd.DataFrame.from_dict(ready_dict)

        df_shift_fact=pd.DataFrame.from_dict(shift_fact_dict)


        month = datetime.today().month
        year = datetime.today().year

    # группирование по букве смены
    ready_df=ready_df.groupby(['letter']).sum() 

    # сброс индексов
    ready_df.reset_index(inplace=True)



    # граппирование по дате, смене и букве
    df_shift_fact = df_shift_fact.groupby(['Date','Shift','letter']).sum()

    # группировка по букве
    df_shift_fact = df_shift_fact.groupby(['letter']).count()

    
    # сброс индексов
    df_shift_fact.reset_index(inplace=True)

    # переименование 
    df_shift_fact.rename(columns={'Line': 'Fact shifts',}, inplace=True)


    #print(df_shift_fact)

    df_letter = rd_plan_csv()
    
    df_letter = (df_letter.loc[
        (df_letter['year'] == year) & 
        (df_letter['month'] == month) &
        (df_letter['line'] == line_name)])
    
    #print(df_letter)
    df_letter.reset_index(inplace=True)

    plan = df_letter['plan'].iloc[0]

    sh_val = settings.LINE_OUTPUT.get(line)


    A = df_letter['A'].iloc[0]
    B = df_letter['B'].iloc[0]
    C = df_letter['C'].iloc[0]
    D = df_letter['D'].iloc[0]

    df_dict = {
        'letter': ['A','B','C','D'],
        'Planned shifts': [A,B,C,D],
    }

    df_graph = pd.DataFrame.from_dict(df_dict)
    df_graph['Planned Output'] = df_graph['Planned shifts']*sh_val

    ready_df = pd.merge(
        ready_df,
        df_graph,
        how='outer',
        left_on='letter',
        right_on='letter',
    )

    ready_df=pd.merge(
        df_shift_fact,
        ready_df,
        how='outer',
        left_on='letter',
        right_on='letter',
    )
    
    ready_df.rename(
        columns={
            'Sheets': 'Fact Output',
            }, inplace=True)

    color, label, description, order, order_description, ln_input, ln_output, shift = get_df_bar_indicat(indicat_df,line_name)

    # график фактического выпуска
    fig = go.Figure(
        data=[go.Bar(
            y=ready_df['Fact Output'],
            x=ready_df['letter'],
            width=0.5,
            marker={'color':'#002F6C'},)],
            layout=go.Layout(
                margin=dict(t=30, l=10, b=10, r=10),
                showlegend=False),
            )

    # эта функция сравнивает значения фактического и планового выпуска, 
    # чтобы добавить к максимальному значению коэффициент, который добавит 
    # свободное место на графике. В свободное место будут добавлены аннотации 
    # с номером заказа, статусом линии и т.п. 
    if max(ready_df['Planned Output']) > max(ready_df['Fact Output']):
        max_y = max(ready_df['Planned Output'])
    else:
        max_y = max(ready_df['Fact Output'])

    # добавление плана
    fig.add_trace(go.Bar(
        y=ready_df['Planned Output'],
        x=ready_df['letter'],
        width=0.25,
        offset=-0.5,
        marker={'color':'#9c9c9c'}
    ))

    # этот блок добавляет аннотации на график с именем линии, номером заказа и т.д.
    fig.update_layout(
        showlegend=False,
        yaxis=dict(range=[0,max_y*1.3]),
        annotations=[{
            # статус, номер заказа, описание стопа, номенклатура заказа
            'text':'Status:<b> {}</b><br>{}<br>{} {}'.format(
                label,description,order, order_description),
            'align':'right',
            "xref": "paper",
            "yref": "paper",
            'x':0.98,
            'y':1,
            "showarrow": False,
            "font": {
                "size": 12,
                'color':color,
                },
            },
            {
            # Название линии
            'text':'<b>' + line_name,
            'align':'right',
            "xref": "paper",
            "yref": "paper",
            'x':0.02,
            'y':1,
            "showarrow": False,
            "font": {
                "size": 24,
                'color':'#002F6C',
                },
            },
            ]
        )
    
    #fig.show()

    shift_df=ready_df.copy()
    shift_df.loc['Total']=shift_df.sum()
    
    # переназначение ячейки со сменами на значение 'total' <b></b> - жирный шрифт
    shift_df['letter'][-1:]='<b>Total</b>'

    # создание таблицы 
    fig_t = go.Figure(
        data=[go.Table(
            columnwidth=[20,15,30,15,30],
            header=dict(
                values=shift_df.columns.values,
                align=['center','center','center','center','center'],
                fill=dict(color=['#768fad','#768fad','#768fad','#919191','#919191']),
                ),
            cells=dict(
                values=shift_df.transpose(),
                fill=dict(color=['#cde0f7','#cde0f7','#cde0f7','#dedede','#dedede']),
                align=['center','center','right','center','right'],
                format=[None,',d', ',d',',d',',d'],
                height=20
                )
            )],
        layout=go.Layout(
            margin=dict(t=0, l=0, b=0, r=0)))
    
    fig_t.update_layout(height=165)

    #fig_t.show()
    
    return fig, fig_t

def make_line(df, dt, dt2):
    """Эта функция создает график линий с отметками по остановкам и 
    простой таблицей по выпуску за смену"""

    # здесь создается переменная, хранящая норму выпуска. Если имя линии не найдено в 
    # словаре, то норма равна нулю. 
    try:
        lineq = settings.LINE_OUTPUT[df['Line'].iloc[0]]
    except:
        lineq=0

    # даты начала и конца временных промежутков(сутки с 8.00 до 8.00)
    dtst = datetime.strptime(dt+'080000', '%Y%m%d%H%M%S')
    dtst2 = datetime.strptime(dt2+'080000', '%Y%m%d%H%M%S')
    
    #if not df.empty:

    df['Stop Start'] = df['Stop Time']-df['Minutes']
    # Ограничение df по времени
    df=df.loc[(df['Stop Time']>dtst) & (df['Stop Time']<dtst2)]


    # Список кодов остановок за вышеуказанный период
    codes = pd.unique(df['Stop Code']).tolist()

    # Output - общий выпуск
    fig = go.Figure(
        data=go.Scatter( 
            y = df['Counter OUT'], 
            x = df['Stop Time'],
            hoverinfo = 'none',
            name = 'Output',
            line_color='#3d8dd1',
        ),
        layout=go.Layout(
            margin=dict(t=10,l=10,b=10,r=10)),
    )

    # Input(sheets) - счетчик входа
    fig.add_trace(
        go.Scatter(
            y = df['Counter IN'], 
            x = df['Stop Time'],
            name = 'Input(sheets)',
            line_color='#4b9bde',
            hoverinfo='none',
            line = dict(
                width=2, 
                dash='dash'
            )
        )
    )

    # Output Shift - выпуск за смену - основной показатель
    fig.add_trace(
        go.Scatter(
            y = df.groupby('Shift')['Sheets'].cumsum(), 
            x = df['Stop Time'],
            name = 'Output Shift',
            line_color='#0f2994',
            hoverinfo='none',
        )
    )

    # eff. 75% - верхняя граница графика(переменная lineq)
    fig.add_trace(
        go.Scatter(
            y0 = lineq,
            x=df['Stop Time'],
            name='eff. 75%:{:3}'.format('') + str(lineq/1000) + 'K',
            line_color='#8ad9eb',
            hoverinfo='none',
            line = dict(width=2, dash='dash')
        )
    )

    # в этом df лежат стопы больше указанного времени, по ним строятся аннотации
    df2=df.loc[(df['Minutes'] > timedelta(minutes=30)) & (df['Status'] == 'STOP')]

    # получение списка заказов
    orders = set(df['Order'].values.tolist())

    """
    else:
        df2=df.copy()
        codes=[]
        orders=[]

        print(df2)
        fig = go.Figure(
            data=go.Scatter( 
                y = [], 
                x = [],
            )
        )

    """
    # здесь добавляются аннотации на основании df2
    if not df2.empty:
        for i in range(len(df2)):
            fig.add_annotation(
                x=df2['Stop Time'].iloc[i]-df2['Minutes'].iloc[i]/2,
                y=df2['Counter OUT'].iloc[i],
                text='{} min. {}:<br> {}'.format(
                    str(df2['Minutes'].iloc[i])[-8:-3], 
                    df2['Stop Code'].iloc[i], 
                    df2['name_ru'].iloc[i],
                    ),
                bgcolor="#ffffff",
                opacity=0.9,
            )

    # Здесь создаются столбцы по кодам остановок. Цикл проходит по каждому коду и 
    # добавляет все столбцы. Этот цикл организован таким образом, чтобы можно было 
    # выставлять разные цвета
    for code in codes:
        
        # фильтрация df
        df2 = df.loc[
            (df['Stop Code'] == code) & 
            (df['TimeRaw'] > 60) & 
            (df['Status'] == 'STOP') 
        ]

        # добавление столбцов кодов на основную фигуру
        fig.add_trace(
            go.Bar(
                y = df2['Counter OUT'], 
                x = df2['Stop Time']-df2['Minutes']/2, 
                width = df2['TimeRaw']*1000,
                hovertext = (
                    df2['Stop Code'] + ': ' + df2['name_ru'] + 
                    '.<br>Stopped at '+ df2['Stop Start'].astype(str).str[11:] +
                    '<br>Duration: ' + df2['Minutes'].astype(str).str[7:15]),
                name=code + ' {:>10}'.format(str(df2['Minutes'].sum())[-8:]),
                hoverinfo='text',
                marker_color = df2['color']))
    
    
    
    # добавление заказов на график.
    for order in orders:

        linex = df.loc[df['Order'] == order]

        fig.add_trace(
            go.Scatter(
                y = linex['Counter OUT'],#[lineq], 
                x = linex['Stop Time'], 
                line = dict(
                    width=12,
                ),
                hovertext = '{}: {}'.format(order, cont_material(order)),
                name=order,
                hoverinfo='text',
                showlegend=False,
                opacity=0.5,
                text=order,
            )
        )

    # если df не пустой, то добавить легенду и имя линии как аннотацию. 
    # иначе скрыть все оси и вывести на график аннотацию "NO DATA" 
    if not df.empty:

        fig.update_layout(
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
            )
        )
        
        """
        fig.add_annotation(
            text=df['Line'].iloc[0],
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            opacity=0.4,
            showarrow=False,
            font=dict(
                size=72
            )
        )
        """
    else:
        fig.update_layout(
            xaxis={'visible':False},
            yaxis={'visible':False},
            plot_bgcolor='white',
            annotations=[{
                'text':'No Data',
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {
                    "size": 28
                }}]
        )
    

    #fig.show()

    return fig

def get_df_con():
    """Эта функция обращается к таблице up_line_def и получает из 
    нее текущие значения по состоянию линии, возвращает сформированный 
    df. Передается в get_df_bar_indicat """

    df = cont_query()

    # номер смены имеет формат TEXT в таблице.
    # его необходимо преобразовать
    df['shift'] = df['shift'].astype(int)

    return df

def get_df_bar_indicat(df, line):
    """Эта функция управляет индикатором и передает ему 
    состояние(value), цвет(color) и номер кода(напр. OGE04)"""

    # получение строки по названию линии
    df = df.loc[df['line'] == line]

    # сброс индексов
    df = df.reset_index()
    
    orderno = df['order'].iloc[0]
    
    order_description = cont_material(orderno)

    ln_input = df['counter in'].iloc[0]
    ln_output = df['counter out'].iloc[0]
    shift = df['shift'].iloc[0]

    # проверка значения кода. Если код пустой,
    # присвоить ему OGE04 - причина не определена
    # если код не пустой, присвоить реальный номер.
    if df['code'].iloc[0] == '00000000':
        stopcode = 'OGE04'
    else:
        stopcode = df['code'].iloc[0][-5:]

    # Определение состояния линии.
    # Присвоение цвета индикатору и комментария(STOP,RUN...)
    if df['status'].iloc[0] and shift != 0:
        # статус линии 1(старт), смена не равна 0

        color_indicator = '#002F6C'
        label_indicator = 'RUN'
        description = 'Линия работает'
    elif df['status'].iloc[0] == 0 and shift != 0:
        # статус линии 0(стоп), смена не равна 0

        color_indicator = '#517096'
        label_indicator = '{} {} min'.format(
            stopcode,
            df['stop_time'].iloc[0])
        #print(df_codes)
        description = df_codes.loc[df_codes['code'] == stopcode]['name_ru'].iloc[0]

    else:
        # статус линии 0(стоп), смена равна 0

        color_indicator = '#9c9c9c'
        label_indicator = "STOP"
        description = 'Линия не работает'
        order_description = ''
        orderno = ''
    # возврат значения статуса линии(indicator.value),
    # цвета индикатора(indicator.color) и подписи индикатора(indicatior.label)
    return color_indicator, label_indicator, description, orderno, order_description, ln_input, ln_output, shift



def make_line_table(df_line_lvl_1, dt, dt2):
    """Эта функция принимает df I уровня df_line_lvl_1 и возвращает таблицу
    с суммой выпуска за указанные сутки(или период)"""

    # форматирование входящей даты за период с 8:00 до 8:00
    dtst = datetime.strptime(dt+'080000', '%Y%m%d%H%M%S')
    dtst2 = datetime.strptime(dt2+'080000', '%Y%m%d%H%M%S')

    #if not df_line_lvl_1.empty:

    # Обрезание df до указанного периода времени
    df_line_lvl_1 = df_line_lvl_1.loc[
        (df_line_lvl_1['Stop Time'] > dtst) &
        (df_line_lvl_1['Stop Time'] < dtst2)]

    # группирование по смене и суммирование листов
    df_output_table = df_line_lvl_1.groupby(['Shift'])['Sheets'].sum()

    df_temp_shift = pd.DataFrame.from_dict({'Shift':[1,2]})
    
    df_output_table = pd.merge(
        df_output_table,
        df_temp_shift,
        how='outer',
        left_on='Shift',
        right_on='Shift')

    
    df_output_table=df_output_table[['Shift','Sheets']]
    df_output_table=df_output_table.sort_values(by=['Shift'])
    # создание столбца итогов. Создание суммы
    df_output_table.loc['Total'] = df_output_table.sum()

    # назначение ячейки со сменами на значение 'total' <b></b> - жирный шрифт
    df_output_table['Shift'][-1:] = '<b>Total</b>'

    df_output_table.fillna(0, inplace=True)
    df_output_table.reset_index(inplace=True)

    del df_output_table['index']

    # создание таблицы с результатами за сутки
    fig = go.Figure(
        data=[go.Table(
            columnwidth=[20, 80],
            header=dict(
                values=df_output_table.columns.values,
                height=30),
            cells=dict(
                values=df_output_table.transpose(),
                align=['center', 'right'],
                format=[None, ',d'],
                height=20
                )
            )],
        layout=go.Layout(
            margin=dict(t=0, l=0, b=0, r=0)))
    
    # обновление лейаута первой таблицы
    fig.update_layout(height=105)


    # создание df для подробной таблицы с кодами
    df_codes_table = (df_line_lvl_1.groupby(['Shift', 'Stop Code'])
        ['TimeRaw'].agg(['sum', 'count']))
    
    # форматирование количества секунд простоя в минуты
    df_codes_table['sum'] = df_codes_table['sum']/60
    df_codes_table['sum'] = df_codes_table['sum'].astype(int)

    # фильтрация кодов нулевой длины
    df_codes_table = df_codes_table.loc[df_codes_table['sum'] > 1]

    # сброс индексов
    df_codes_table = df_codes_table.reset_index()

    # переименование столбца суммы
    df_codes_table.rename(columns={'sum': 'sum (minutes)'}, inplace=True)

    # создание столбца итогов. Создание суммы
    df_codes_table.loc['Total'] = df_codes_table.sum()
    
    # переназначение ячейки со сменами на значение 'total'
    try:
        df_codes_table['Shift'][-1:] = '<b>Total</b>'
    except:
        df_codes_table.rename(columns={'index': 'Shift'}, inplace=True)
        df_codes_table['Shift'][-1:] = '<b>Total</b>'

    # переименование последней ячейки столбца кодов
    df_codes_table['Stop Code'][-1:] = ''

    # создание таблицы с подробной информацией по кодам
    fig2 = go.Figure(
        data=[go.Table(
            columnwidth=[20,30,20,20],
            header=dict(
                values=df_codes_table.columns.values,
                height=30,
                ),
            cells=dict(
                values=df_codes_table.transpose(),
                align=['center', 'left', 'right', 'right'],
                format=[None, None, ',d', ',d'],
                height=20
                )
            )],
        layout=go.Layout(
            margin=dict(t=0, l=0, b=0, r=0)))

    fig2.update_layout(height=150)

    #fig.show()

    return fig, fig2

def Plan_table(df_line_lvl_1):

    df_report=df_line_lvl_1[['Line','letter','Date', 'Shift','Sheets']]

    #print(df_report.groupby(['Line','Date','Shift','letter']).sum())


def ibea_stat():
    """Эта функция обрабатывает файлы статистики камеры"""

    shift_start = pd.to_datetime('07:50:00',format='%H:%M:%S').time()
    shift_end = pd.to_datetime('19:50:00',format='%H:%M:%S').time()


    cols = [1, 2, 4, 5, 6, 8, 9]
    df_ibea_raw = pd.read_csv(path / 'z_cumulated.csv', sep=';', usecols=cols, parse_dates=[0,1,2,3])


    
    
    df_ibea_raw.columns=[
        'Date Start',       # 1
        'Time Start',       # 2
        'Date End',         # 4
        'Time End',         # 5
        'Order',            # 6
        'Total',            # 8
        'Rejected',         # 9
    ]
    df_ibea_raw=df_ibea_raw.sort_values(by='Date Start')

    df_ibea_raw = df_ibea_raw.loc[df_ibea_raw['Date End']>pd.to_datetime('01.01.2020', format='%d.%m.%Y')]
    df_ibea_raw = df_ibea_raw.loc[df_ibea_raw['Total']>1000]

    df_ibea_raw['Time Start'] = [time.time() for time in pd.to_datetime(df_ibea_raw['Time Start'])]
    df_ibea_raw['Time End'] = [time.time() for time in pd.to_datetime(df_ibea_raw['Time End'])]

    df_ibea_raw.to_csv('21223.csv', sep=';')
    #df_ibea_raw['Date Start'] = pd.to_datetime(df_ibea_raw['Date Start'])
    #df_ibea_raw['Date End'] = pd.to_datetime(df_ibea_raw['Date End'])
    


    df_ibea_raw['Percent'] = df_ibea_raw['Rejected']/df_ibea_raw['Total']*100
    df_ibea_raw['line']='LZ-01'


    df_ibea_raw['Shift'] = (df_ibea_raw['Time Start'] < shift_end) & (df_ibea_raw['Time End'] > shift_start)
    
    df_ibea_raw['Shift'] = df_ibea_raw['Shift'].apply(lambda x: 1 if x else 2 )

    df_ibea_raw = df_ibea_raw.loc[df_ibea_raw['Percent'] <= 10] 


    #
    
    #df_ibea_raw = df_ibea_raw.groupby('Date Start').mean()
    #df_ibea_raw.reset_index(inplace=True)
    
    df_ibea_raw.to_csv('213.csv', sep=';')

    print(df_ibea_raw)

    return df_ibea_raw

def ibea_graph(df_ibea):

    fig = go.Figure(
        data=go.Scatter( 
            x=df_ibea['Date Start'],
            y=df_ibea['Percent']
        )
    )
    

    #fig.show()

if __name__ == '__main__':

    dt= '20201125'
    dt2='20201126'

    df_lvl_0 = get_df_lvl_0(dt,dt2)
    print(df_lvl_0)

    line = 'LZ-01'

    def _call_line():
    
       


        df_line_lvl_1 = get_df_line_lvl_1(df_lvl_0, line)
        make_line(df_line_lvl_1,dt,dt2)


    def _call_bar():

        df_line_lvl_1 = get_df_line_lvl_1(df_lvl_0, line)
        indicat_df=get_df_con()
        make_bar(df_line_lvl_1,indicat_df,line)

    def _ibea_cal():

        #ibea_stat()
        ibea_graph(ibea_stat())

    _call_line()
    #_call_bar()
    #_ibea_cal()
