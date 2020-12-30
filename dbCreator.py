from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, Float , String, Date, MetaData,exc
from sqlalchemy_utils import database_exists, create_database

import csv
from datetime import datetime

engine = create_engine("mysql+pymysql://root@localhost/statistics?charset=utf8",echo=False)
if not database_exists(engine.url):
    create_database(engine.url)

meta = MetaData()
clients = Table(
   'clients', meta, 
   Column('client_id', String(20), primary_key = True, nullable=True), 
   Column('title', String(20)), 
   Column('dob', Date),
   Column('store_id', String(10)), 
   Column('mem_start_date', Date), 
   Column('mem_date', Date), 
   Column('mem_end_date', Date), 
   Column('vip', Integer), 
   Column('insee_code', String(10)), 
   Column('country', String(3)), 
)


heads = Table(
   'heads', meta, 
   Column('ticket_id', BigInteger, autoincrement=False, primary_key=True ,  nullable=True), 
   Column('ticket_date', Date), 
   Column('store_id', String(10)),
   Column('client_id', String(20)), 
   Column('discount_volume', Integer), 
   Column('total_ticket_cost', Float), 
)


lines = Table(
   'lines', meta, 
   Column('ticket_id', BigInteger, nullable=True), 
   Column('ticket_line', Integer), 
   Column('item_id', String(10)),
   Column('quantity', Integer), 
   Column('discount_amount', Float), 
   Column('total', Float), 
   Column('output_margin', Float), 
)


items = Table(
   'items', meta, 
   Column('item_id', String(10), primary_key = True,nullable=True), 
   Column('universal_code', String(10)), 
   Column('family_code', String(10)),
   Column('sub_family_code', String(10)), 
)


stores = Table(
   'stores', meta, 
   Column('store_id', String(10), primary_key = True, autoincrement=False, nullable=True), 
   Column('city', String(30)), 
   Column('department_label', String(5)),
   Column('com_region_label', String(30)), 
)

def getDate(txt):
	try:
		return datetime.strptime(txt,'%d/%m/%Y %H:%M:%S') if txt else None
	except:
		return datetime.strptime(txt,'%Y-%m-%d %H:%M:%S') if txt else None

def uploadData():
	meta.create_all(engine)
	discounts = {}
	with engine.connect() as connection:
		with connection.begin() as transaction:
			with open('../CLIENT.CSV', "r") as csvfile:
				reader = csv.reader(csvfile,delimiter ='|')
				values = []
				for i, line in enumerate(reader):
					if i==0:
						continue
					line[2] = getDate(line[2])
					line[4] = getDate(line[4])
					line[5] = getDate(line[5])
					line[6] = getDate(line[6])
					line[7] = int(line[7])
					cols = ['client_id','title','dob','store_id','mem_start_date','mem_date','mem_end_date','vip','insee_code','country']
					values.append({col:val for col,val in zip(cols,line)})
					if i%10000==0:
						print("Done:",i)
						try:
							connection.execute(clients.insert(), values)
						except:
							transaction.rollback()
							raise
						values = []
				if values:
					print("Done:",i)
					connection.execute(clients.insert(), values)



			with open('../LIGNES_TICKET_V4.CSV', "r") as csvfile:
				reader = csv.reader(csvfile,delimiter ='|')
				values = []
				for i, line in enumerate(reader):
					if i==0:
						continue
					line[0] = int(line[0])
					line[1] = int(line[1])
					line[3] = float(line[3].replace(',','.'))
					line[4] = float(line[4].replace(',','.'))
					line[5] = float(line[5].replace(',','.'))
					line[6] = float(line[6].replace(',','.'))
					if line[0] in discounts:
						discounts[line[0]] += 1 if line[4] else 0
					else:
						discounts[line[0]] = 1 if line[4] else 0
					cols = ['ticket_id','ticket_line','item_id','quantity','discount_amount','total','output_margin']
					values.append({col:val for col,val in zip(cols,line)})
					if i%100000==0:
						print("Done:",i)
						try:
							connection.execute(lines.insert(), values)
						except:
							transaction.rollback()
							raise
						values = []
				if values:
					print("Done:",i)
					connection.execute(lines.insert(), values)



			with open('../ENTETES_TICKET_V4.CSV', "r") as csvfile:
				reader = csv.reader(csvfile,delimiter ='|')
				values = []
				for i, line in enumerate(reader):
					if i==0:
						continue
					line[0] = int(line[0])
					line[1] = getDate(line[1])
					line[4] = float(line[4].replace(',','.'))
					line[5] = discounts[line[0]]
					cols = ['ticket_id','ticket_date','store_id','client_id','total_ticket_cost','discount_volume']
					values.append({col:val for col,val in zip(cols,line)})
					if i%100000==0:
						print("Done:",i)
						try:
							connection.execute(heads.insert(), values)
						except:
							transaction.rollback()
							raise
						values = []
				if values:
					print("Done:",i)
					connection.execute(heads.insert(), values)






			with open('../REF_ARTICLE.CSV', "r") as csvfile:
				reader = csv.reader(csvfile,delimiter ='|')
				values = []
				for i, line in enumerate(reader):
					if i==0:
						continue
					cols = ['item_id','universal_code','family_code','sub_family_code']
					values.append({col:val for col,val in zip(cols,line)})
					if i%100000==0:
						print("Done:",i)
						try:
							connection.execute(items.insert(), values)
						except:
							transaction.rollback()
							raise
						values = []
				if values:
					print("Done:",i)
					connection.execute(items.insert(), values)



			with open('../REF_MAGASIN.CSV', "r") as csvfile:
				reader = csv.reader(csvfile,delimiter ='|')
				values = []
				for i, line in enumerate(reader):
					if i==0:
						continue
					cols = ['store_id','city','department_label','com_region_label']
					values.append({col:val for col,val in zip(cols,line)})
					if i%100000==0:
						print("Done:",i)
						try:
							connection.execute(stores.insert(), values)
						except:
							transaction.rollback()
							raise
						values = [] 
				if values:
					print("Done:",i)
					connection.execute(stores.insert(), values)
				transaction.commit()

if __name__ == '__main__':
	uploadData()