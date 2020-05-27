
from xml.dom import minidom 
import os
import psycopg2, time
from datetime import datetime

conn = psycopg2.connect(database="roll_grinders",
                        user='postgres',
                        password='2378951',
                        host='localhost',
                        port=5432)

def create_hercules_table(name_table):
	cur.execute(f"""CREATE TABLE {name_table} (id serial PRIMARY KEY,
										       Name varchar(30),
										       Description integer,
										       MeasuringLength integer,
										       ProgramNo varchar(8),
										       StartGrindDate timestamp, 
										       EndGrindDate timestamp, 
										       GrindingTime time, 
										       MeasOffsetHeadstock numeric, 
										       MeasOffsetTailstock numeric, 
										       ShapeNo varchar(30),
										       GrindId varchar(30),
										       FormTolerance numeric, 
										       TargetDiameter numeric,
										       RollDiameterBeforeGrindingHeadstock numeric,
										       RollDiameterAfterGrindingHeadstock numeric,
										       RollDiameterBeforeGrindingMiddle numeric,
										       RollDiameterAfterGrindingMiddle numeric,
										       RollDiameterBeforeGrindingTailstock numeric,
										       RollDiameterAfterGrindingTailstock numeric,
										       ShapeRef varchar,
										       ShapeAfterGrinding varchar,
										       DeviationAfterGrinding varchar,
										       Bruise varchar,
										       Crack varchar,
										       Magnetism varchar,									  
											   Recording_date timestamp);""")
def parse_xml(filename):
	xmldoc = minidom.parse(filename)
	xmldoc.normalize()
	"""
	Если тег в документе один, то мы можем сразу обратится к
	нему по индексу [0], далее чтобы получить значение тега
	нужно обратится к его дочернему узлу через ChildNodes, да -
	здесь значения тега, это его дочерние узлы так называемые.
	И далее с помощью nodeValue мы получаем само значение дочернего
	узла.
	"""
	FileProperties = xmldoc.getElementsByTagName('FileProperties')[0].childNodes[0].nodeValue[1:-1].split('\n')

	StartGrindDate = datetime.strptime(xmldoc.getElementsByTagName('StartGrind')[0].childNodes[1].nodeValue, '%d_%m_%Y_%H_%M_%S')
	EndGrindDate = datetime.strptime(xmldoc.getElementsByTagName('EndGrind')[0].childNodes[1].nodeValue, '%d_%m_%Y_%H_%M_%S')
	GrindingTime = EndGrindDate - StartGrindDate
	GrindingTimeHours = GrindingTime.seconds // 3600
	GrindingTimeMinutes = GrindingTime.seconds // 60
	GrindingTimeSeconds = GrindingTime.seconds - GrindingTimeMinutes * 60 - GrindingTimeHours * 3600
	ProgramNo = xmldoc.getElementsByTagName('ProgramNo')[0].childNodes[1].nodeValue
	MeasuringLength = xmldoc.getElementsByTagName('MeasuringLength')[0].childNodes[1].nodeValue[1:]
	MeasOffsetTailstock = xmldoc.getElementsByTagName('MeasOffsetTailstock')[0].childNodes[1].nodeValue[1:]
	MeasOffsetHeadstock = xmldoc.getElementsByTagName('MeasOffsetHeadstock')[0].childNodes[1].nodeValue[1:]
	ShapeNo = xmldoc.getElementsByTagName('ShapeNo')[0].childNodes[1].nodeValue
	Operator = xmldoc.getElementsByTagName('Operator')[0].childNodes[1].nodeValue
	GrindId = xmldoc.getElementsByTagName('GrindId')[0].childNodes[1].nodeValue
	FormTolerance = float(xmldoc.getElementsByTagName('FormTolerance')[0].childNodes[1].nodeValue[1:])
	TargetDiameter = float(xmldoc.getElementsByTagName('TargetDiameter')[0].childNodes[1].nodeValue[1:])

	try:RollDiameterBeforeGrindingTailstock = float(xmldoc.getElementsByTagName('RollDiameterBeforeGrindingTailstock')[0].childNodes[1].nodeValue[1:])
	except IndexError:RollDiameterBeforeGrindingTailstock = None

	try:RollDiameterBeforeGrindingMiddle = float(xmldoc.getElementsByTagName('RollDiameterBeforeGrindingMiddle')[0].childNodes[1].nodeValue[1:])
	except IndexError:RollDiameterBeforeGrindingMiddle = None

	try:RollDiameterBeforeGrindingHeadstock = float(xmldoc.getElementsByTagName('RollDiameterBeforeGrindingHeadstock')[0].childNodes[1].nodeValue[1:])
	except IndexError:RollDiameterBeforeGrindingHeadstock = None

	try:RollDiameterAfterGrindingTailstock = float(xmldoc.getElementsByTagName('RollDiameterAfterGrindingTailstock')[0].childNodes[1].nodeValue[1:])
	except IndexError:RollDiameterAfterGrindingTailstock = None

	try:RollDiameterAfterGrindingMiddle = float(xmldoc.getElementsByTagName('RollDiameterAfterGrindingMiddle')[0].childNodes[1].nodeValue[1:])
	except IndexError:RollDiameterAfterGrindingMiddle = None

	try:RollDiameterAfterGrindingHeadstock = float(xmldoc.getElementsByTagName('RollDiameterAfterGrindingHeadstock')[0].childNodes[1].nodeValue[1:])
	except IndexError:RollDiameterAfterGrindingHeadstock = None

	ShapeRef = ''
	ShapeAfterGrinding = ''
	DeviationAfterGrinding = ''
	try:
		for num in [float(num) for num in xmldoc.getElementsByTagName('ShapeRef')[0].childNodes[1].nodeValue.replace('\n', '').split(',')]:
			ShapeRef += str(num) + ','
		ShapeRef = ShapeRef[:-1]	
	except IndexError: ShapeRef = None

	try: 
		for num in [float(num) for num in xmldoc.getElementsByTagName('ShapeAfterGrinding')[0].childNodes[1].nodeValue.replace('\n', '').split(',')]:
			ShapeAfterGrinding += str(num) + ','
		ShapeAfterGrinding = ShapeAfterGrinding[:-1]	
	except IndexError: ShapeAfterGrinding = None		

	try:
		for num in [float(num) for num in xmldoc.getElementsByTagName('DeviationAfterGrinding')[0].childNodes[1].nodeValue.replace('\n', '').split(',')]:
			DeviationAfterGrinding += str(num) + ','
		DeviationAfterGrinding = DeviationAfterGrinding[:-1]			
	except IndexError: DeviationAfterGrinding = None

	Bruise = '-'
	Crack = '-'
	Magnetism = '-'
	# CrackBeforeGrinding = []
	# for i in xmldoc.getElementsByTagName('CrackBeforeGrinding')[0].childNodes[1].nodeValue.replace('\n', '').split(','):
	# 	CrackBeforeGrinding.append(float(i))
	# CrackAfterGrinding = []
	# for i in xmldoc.getElementsByTagName('CrackAfterGrinding')[0].childNodes[1].nodeValue.replace('\n', '').split(','):
	# 	CrackAfterGrinding.append(float(i))
	with conn:
		with conn.cursor() as cur:
			cur.execute("""SELECT max(id) FROM test""")
			max_id = cur.fetchall()[0][0]
			if max_id is None:
				next_id = 1 
			else:
				next_id = max_id + 1	
			
			cur.execute("""
							INSERT INTO test
							values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
						""", (next_id, FileProperties[1][6:], FileProperties[3][13:], MeasuringLength, ProgramNo, StartGrindDate,
							  EndGrindDate, psycopg2.Time(GrindingTimeHours, GrindingTimeMinutes, GrindingTimeSeconds), 
							  MeasOffsetHeadstock, MeasOffsetTailstock, ShapeNo, GrindId, FormTolerance,
							  TargetDiameter, RollDiameterBeforeGrindingHeadstock, RollDiameterAfterGrindingHeadstock,
	                          RollDiameterBeforeGrindingMiddle, RollDiameterAfterGrindingMiddle, RollDiameterBeforeGrindingTailstock,
							  RollDiameterAfterGrindingTailstock, ShapeRef, ShapeAfterGrinding, DeviationAfterGrinding,
							  Bruise, Crack, Magnetism, datetime.now()
	                         ))


	
# нужно узнать номер станка и, в зависимости от того какую таблицу мы заполняем,
# нужно эту таблицу и проверять на наличие ее в базе.

# проверка на наличие файла
with conn:
	with conn.cursor() as cur:
		# делаем запрос на то есть ли данная таблица в базе
		table_name = 'test'  # название таблицы будет формироваться динамически в зависимости от номера станка
		# cur.execute("""DELETE FROM test""") # удаление таблицы для тестов
		cur.execute(f"""
						SELECT table_name
					    FROM information_schema.tables
					    WHERE table_schema='public'
					    AND table_type='BASE TABLE'
					    AND table_name='{table_name}';
					""")
		availability_query_table = cur.fetchall()
		if len(availability_query_table) == 0: # если таблицы нет, то создать ее
			create_hercules_table(table_name) 

		# делаем запрос на последнюю дату в базе	
		cur.execute(f"""
					 SELECT max(StartGrindDate) FROM {table_name} 
					 """)
		query_last_StartGrindDate = cur.fetchall()
		
		if query_last_StartGrindDate[0][0] != None:
			last_StartGrindDate	= query_last_StartGrindDate[0][0]
			print(last_StartGrindDate)
			for file in os.listdir('.'): # указаваем путь к папке . здесь означает текущую дирректорию
				if file.endswith('.xml'):
					"""отсекаем лишнее (ищем место первого вхождения _ это сделано на случай того, если номер станет 4х значным) и преобразуем в дату """
					date_current_file = datetime.strptime(file[file.find('_')+1:-4], '%d_%m_%Y_%H_%M_%S')
					# print(date_current_file, '-текущий')
					if date_current_file > last_StartGrindDate:
						parse_xml(file)
						print(file,'-добавлен т.к. данные более новые')
					else:
						print(file, '-старые данные')	

		else:
			last_StartGrindDate = None
			for file in os.listdir('.'): 
				if file.endswith('.xml'):
					print(file,'-добавлен т.к. данных вообще не было')
					parse_xml(file)
						
			# если в таблице нет последнего значения, то выгрузить в таблицу всю папку


		
"""
1. 
   а. Запрос в БД по файлу, который был обработан последним
   (в качестве даты преобразовать название файла), т.к. 
   файлы из папки могут записываться не по порядку.
   б. Будет две даты 1. дата записи в БД 2. Дата окончания 
   шлифовки, которая будет преобразовываться из имени файла.
   в. Если есть файлы в папке, которые позже последней даты
   сохраненной в бд, то они все записываются.
   г. Если база пустая, внести все данные, что есть в папке.

"""


