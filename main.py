from dbCreator import *
from sqlalchemy.sql import select,and_, or_, not_
from sqlalchemy import func
from calendar import monthrange



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
			print('\t[+] Count:',count,'Date:',date)
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

		