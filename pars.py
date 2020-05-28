
from xml.dom import minidom 
import os
import psycopg2, time
from datetime import datetime

conn = psycopg2.connect(database="roll_grinders",
                        user='postgres',
                        password='2378951',
                        host='localhost',
                        port=5432)

table_name = 'test'  # укажите название таблицы в которую будет происходить запись и которая будет создана если ее нет
parse_directory = '.'  # папка в которой лежат файлы xml

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
										       Operator varchar(10), 									  
											   Recording_date timestamp);""")
def parse_xml(filename):

	xmldoc = minidom.parse(filename)
	xmldoc.normalize()
	
	def xml_get_values(tag_name, child_index, node_start_index=None, node_finish_index=None):
		"""
			Если тег в документе один, то мы можем сразу обратится к
			нему по индексу [0], далее чтобы получить значение тега
			нужно обратится к его дочернему узлу через ChildNodes, да -
			здесь значения тега, это его дочерние узлы так называемые.
			И далее с помощью nodeValue мы получаем само значение дочернего
			узла.
		"""
		xml_doc_tag_child_select = xmldoc.getElementsByTagName(tag_name)[0].childNodes[child_index]
		if node_start_index is None:
			if node_finish_index is None:
				return xml_doc_tag_child_select.nodeValue
			return xml_doc_tag_child_select.nodeValue[:node_finish_index]
		else:
			if node_finish_index is None:
				return xml_doc_tag_child_select.nodeValue[node_start_index:]
			return xml_doc_tag_child_select.nodeValue[node_start_index:node_finish_index]		

	RollParameters = {}


	parameters = [	
					['FileProperties'],
					['StartGrind', 'EndGrind', 'GrindingTime'],
					['ProgramNo', 'ShapeNo', 'Operator', 'GrindId'],
					['MeasuringLength', 'MeasOffsetTailstock', 'MeasOffsetHeadstock'],
					['FormTolerance', 'TargetDiameter', 'RollDiameterBeforeGrindingTailstock',
					 'RollDiameterBeforeGrindingMiddle', 'RollDiameterBeforeGrindingHeadstock',
					 'RollDiameterAfterGrindingTailstock', 'RollDiameterAfterGrindingMiddle',
					 'RollDiameterAfterGrindingHeadstock'],
					['ShapeRef', 'ShapeAfterGrinding', 'DeviationAfterGrinding'],
					['Bruise', 'Crack', 'Magnetism']						
				]

	for i in range(7):
		for xml_tag in parameters[i]:
			try:
				if i == 0:
					FileProperties = xml_get_values('FileProperties', 0, node_start_index=1, node_finish_index=-1).split('\n')			
					RollParameters['Name'] = FileProperties[1][6:]
					RollParameters['Description'] = FileProperties[3][13:]
				elif i == 1:
					if parameters[i].index(xml_tag) < 2:
						RollParameters[xml_tag] = datetime.strptime(xml_get_values(xml_tag, 1), '%d_%m_%Y_%H_%M_%S')
					else:		
					 # после того как данные по дате начала и конца добавились в словарь
						GrindingTimeDelta = RollParameters['EndGrind'] - RollParameters['StartGrind']
						h = GrindingTimeDelta.seconds // 3600
						m = GrindingTimeDelta.seconds // 60
						s = GrindingTimeDelta.seconds - h * 3600 - m * 60
						RollParameters[xml_tag] = psycopg2.Time(h, m, s)
				elif i == 2:
					RollParameters[xml_tag] = xml_get_values(xml_tag, 1)
				elif i == 3:	
					RollParameters[xml_tag] = xml_get_values(xml_tag, 1, node_start_index=1)
				elif i == 4:	
					RollParameters[xml_tag] = float(xml_get_values(xml_tag, 1, node_start_index=1))
				elif i == 5:	
					teststr = ''
					for num in [float(num) for num in xml_get_values(xml_tag, 1).replace('\n', '').split(',')]:
						teststr += str(num) + ','
					RollParameters[xml_tag] = teststr[:-1]
				elif i == 6:
					RollParameters[xml_tag] = '-'			
			except:
				RollParameters[xml_tag] = None				
	# print(RollParameters)	
	
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
							values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
						""", (next_id,
							  RollParameters['Name'],
							  RollParameters['Description'],
							  RollParameters['MeasuringLength'],
							  RollParameters['ProgramNo'],
							  RollParameters['StartGrind'],
							  RollParameters['EndGrind'],
							  RollParameters['GrindingTime'],
							  RollParameters['MeasOffsetHeadstock'],
							  RollParameters['MeasOffsetTailstock'],
							  RollParameters['ShapeNo'],
							  RollParameters['GrindId'],
							  RollParameters['FormTolerance'],
							  RollParameters['TargetDiameter'],
							  RollParameters['RollDiameterBeforeGrindingHeadstock'],
							  RollParameters['RollDiameterAfterGrindingHeadstock'],
							  RollParameters['RollDiameterBeforeGrindingMiddle'],
							  RollParameters['RollDiameterAfterGrindingMiddle'],
							  RollParameters['RollDiameterBeforeGrindingTailstock'],
							  RollParameters['RollDiameterAfterGrindingTailstock'],
							  RollParameters['ShapeRef'],
							  RollParameters['ShapeAfterGrinding'],
							  RollParameters['DeviationAfterGrinding'],
							  RollParameters['Bruise'],
							  RollParameters['Crack'],
							  RollParameters['Magnetism'],
							  RollParameters['Operator'],
							  datetime.now()
	                         ))

with conn:
	with conn.cursor() as cur:
		# cur.execute(f"""DELETE FROM {table_name}""") # удаление таблицы для тестов
		# делаем запрос на то есть ли данная таблица в базе
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
			for file in os.listdir(parse_directory): # указаваем путь к папке . здесь означает текущую дирректорию
				if file.endswith('.xml'):
					"""отсекаем лишнее (ищем место первого вхождения _ это сделано на случай того, если номер станет 4х значным) и преобразуем в дату """
					date_current_file = datetime.strptime(file[file.find('_')+1:-4], '%d_%m_%Y_%H_%M_%S')
					if date_current_file > last_StartGrindDate:
						parse_xml(file)
						print(file,'-добавлен т.к. данные более новые')
					else:
						print(file, '-старые данные')	

		else: # если последней даты нет, то - добавим всю папку в бд
			last_StartGrindDate = None
			for file in os.listdir('.'): 
				if file.endswith('.xml'):
					parse_xml(file)
					print(file,'-добавлен т.к. данных вообще не было')


