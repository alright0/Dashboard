# Dashboard

Данный репозиторий - это набор мини-приложений, написанный на <a href="https://dash.plotly.com/">dash</a> - фреймворке для создания дэшбордов с использованием plotly. Основной целью этого приложения было создание набора приложений для быстрого получения информации о работе линий, выпуске, простоях и остановках. В процессе работы над этим проектом я укрепил свои знания **pandas, plotly, dash** и **стандартной библиотеки**. Полученные знания я применяю для работы над текущим проектом - <a href="https://github.com/alright0/intranet_api">внутренним сайтом</a>, генерирующим разнообразные отчеты и графики, который я пишу на **flask**.

Я не могу выложить здесь все данные для полноценного запуска приложений из-за NDA, но могу показать, что они из себя представляют:

Приложения**app1** и **app3**:

**app1** - это основное приложение в этой связке, оно обращается в базу и выводит информацию по производству на текущий момент. синие столбцы - это выгрузка из базы, серые - данные о планах, полученные из **app3**. Само приложение выводит такие блоки по всем линиям на одну страницу: 
<p align="center"><img src="https://user-images.githubusercontent.com/71926912/113470254-d9fbec00-945c-11eb-8277-7dabc30efdda.PNG"></p>

**app3** - это страница, на которой можно заполнить информацию о планах производства(1 такой блок на одну 1 линию), разбитого по категориям смен(A B C D), эта информация сохранится в csv файле в директории приложений и подтянется при автообновлении **app1**. 
<p align="center"><img src="https://user-images.githubusercontent.com/71926912/113470286-247d6880-945d-11eb-8fc2-8093db1ed178.PNG"></p>

**app2** наиболее интересное, с моей точки зрения, приложение, которое выгружает остановки линий из базы и строит на основании этой информации хронологию работы линии за указанные сутки(2 смены - день и ночь). Для простоты восприятия периоды, когда линия не работает(цветные столбцы), раскрашиваются определенными цветамии в зависимости от кода остановки. Сопроводительная информация всплывает при наведении на столбец, простои более 30 минут выделяются дополнительной сноской:
 <p align="center"><img src="https://user-images.githubusercontent.com/71926912/113470458-85597080-945e-11eb-8359-06bad9d6f4a9.PNG"></p> 

**app4** - это приложение, отражающее работу линий в реальном времени. У него есть недостаток - фиксированные отсчеты времени для получение данных из базы. Более приемлемем вариатом было бы воспользоваться функионалом listen/notify в postgres и psycopg2, но у меня было недостаточно знаний на тот момент. Вторым серьезным недостатком здесь я вижу использование intervals в dash для обновления данных. intervals - метод, который делает обратный вызов на стороне браузера с фиксированными интервалами. Из-за этого все открытые вкладки(во всяком случае, в локальной сети), влияют на количество отсчетов всей системы.
В нормальной ситуации работа приложения выглядит следующим образом(ступенчатость графиков вызвана тем, что время интервалов немного меньше, чем скорость агрегации данных в базу):
 <p align="center"><img src="https://user-images.githubusercontent.com/71926912/113471004-31e92180-9462-11eb-8889-ed7decf0e0d7.gif"></p>

**app5** - это приложение, которое основано на результатах получения информации с камер IBEA, которые осуществляют контроль качества продукции на производственных линиях.
Данное приложение обрабатывает и выгружает информацию о работе камер в хронологическом порядке. Основная цель - видеть процент брака линии по сменам
<p align="center"><img src="https://user-images.githubusercontent.com/71926912/113471670-9312f400-9466-11eb-87be-340ca821ba5d.PNG"></p>

Большая часть проделанной здесь работы и полученных знаний стала основой для создания более глобальных компонентов системы. На основе работы приложения **app5**, работающего со всем камерами последовательно и берущего информиацию из csv файлов статистики камеры, появился <a href="https://github.com/alright0/ibea_to_pg">скрипт</a>, опрашивающий камеры асинхронно и агрегирующий полученную информацию в postgres. 

На основе приложений **app1**(план-факт месяца) и **app3**(страница для указания плановых выпусков) появилась доработанная версия отчетов в <a href="https://github.com/alright0/intranet_api">основном приложении</a>. Работа над внедрением функционала из **app4* и **app2** сейчас ведется. Думаю, в ближайшее время можно будет увидеть ее в readme основного приложения.

<!-- <p align="center"><img width=700px src=""></p> -->
