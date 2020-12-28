from dbCreator import *
from sqlalchemy.sql import select,and_, or_, not_
from sqlalchemy import func
from calendar import monthrange




"""

FUNCTIONS TO BE ADDED

"""


with engine.connect() as conn:
	for store in conn.execute(select([stores.c.store_id])):
		years = []
		quarters = {}
		print("[+] Store:",store[0])
		
		statement = select([func.count(func.distinct(heads.c.client_id)),func.date_format(heads.c.ticket_date,'%Y-%m').label('date_log'),func.quarter(heads.c.ticket_date)]).where(heads.c.store_id==store[0]).group_by('date_log')
		for count,date,quart in conn.execute(statement):
			d = datetime.strptime(date,'%Y-%m')
			y,m = d.year,d.month
			d1 = f'{y}-{m-2:02d}-01'
			years.append(y) if y not in years else None
			print('\t[+] Date:',date,'Count:',count)
			if d1 not in quarters and m%3==0:
				quarters[d1] = date+'-'+str(monthrange(y,m)[1])
		print()

		for q1,q2 in quarters.items():
			statement = select([func.count(func.distinct(heads.c.client_id)),func.quarter(heads.c.ticket_date)]).where(and_(heads.c.store_id==store[0],heads.c.ticket_date.between(q1,q2)))
			for count,quart in conn.execute(statement):
				print("\t[+] From:",q1,"To:",q2,"Count:",count)
		print()
		
		for year in years:
			statement = select([func.count(func.distinct(heads.c.client_id))]).where(and_(heads.c.store_id==store[0],func.year(heads.c.ticket_date)==year))
			for count in conn.execute(statement):
				print('\t[+] Year:',year,'Count:',count[0])
		print()



		statement = select([func.distinct(func.year(heads.c.ticket_date))])
		# years = [2016,2017]
		years = []
		for count in conn.execute(statement):
			years.append(count[0])
		sy,ey=years[0],years[-1]
		statement = select([func.count(func.distinct(heads.c.client_id))]).where(and_(heads.c.store_id==store[0],heads.c.ticket_date.between(f'{sy}/01/01',f'{ey}/12/31')))
		for count in conn.execute(statement):
			print(count)




		statement = select([func.distinct(func.year(heads.c.ticket_date))])
		# year = 2016
		year = conn.execute(statement).fetchone()[0]
		statement = select([func.count(func.distinct(heads.c.client_id))]).where(and_(heads.c.store_id==store[0],heads.c.ticket_date.between(f'{year}/01/01',f'{year}/12/31')))
		for count in conn.execute(statement):
			print(count)





		statement = select([func.distinct(func.year(heads.c.ticket_date))])
		years = []
		for count in conn.execute(statement):
			years.append(count[0])
		# year = 2017
		year=years[-1]
		statement = select([func.count(func.distinct(heads.c.client_id))]).where(and_(heads.c.store_id==store[0],heads.c.ticket_date.between(f'{year}/01/01',f'{year}/12/31')))
		for count in conn.execute(statement):
			print(count)



