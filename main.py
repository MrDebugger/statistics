from dbCreator import *
from sqlalchemy.sql import select,and_, or_, not_
from sqlalchemy import func
from calendar import monthrange
import time

"""

FUNCTIONS TO BE ADDED

"""

def getPerYear(conn,year,f):
	statement = select([	f , heads.c.store_id	]).\
				where(	func.year(heads.c.ticket_date) == year	).\
				group_by(heads.c.store_id)

	return conn.execute(statement).fetchall()

def getFromTo(conn,year1,year2,f):
	statement = select([	f,	heads.c.store_id	]).\
				where(	heads.c.ticket_date.between(year1,year2)	).\
				group_by(	heads.c.store_id	)

	return conn.execute(statement).fetchall()

def getPerStore(conn,f):
	statement = select([	f,	heads.c.store_id,	func.date_format(heads.c.ticket_date,'%Y-%m').label('date_log'),	func.quarter(heads.c.ticket_date)	]).\
				group_by(	heads.c.store_id	).\
				group_by(	'date_log'	)

	return conn.execute(statement).fetchall()

def getStores(conn):
	statement = select([stores.c.store_id])
	return conn.execute(statement).fetchall()


def getClientsCount(conn):
	years = []
	quarters = {}
	f = func.count( func.distinct(heads.c.client_id) ) 
	for count,store,date,quart in getClientsPerStore(conn,f):
		d = datetime.strptime(date,'%Y-%m')
		y,m = d.year,d.month
		d1 = f'{y}-{m-2:02d}-01'
		years.append(y) if y not in years else None
		print('[+] Date:',date,'Store:',store,'Count:',count)
		if d1 not in quarters and m%3==0:
			quarters[d1] = date+'-'+str(monthrange(y,m)[1])
	print()
			
	for q1,q2 in quarters.items():
		for count,store in getClientsFromTo(conn,q1,q2,f):
			print("[+] From:",q1,"To:",q2,'Store:',store,"Count:",count)
	print()
	
	for year in years:
		for count,store in getClientsPerYear(conn,year,f):
			print('[+] Year:',year,'Count:',count,'Store:',store)
	print()



def getLoyals(conn):
	statement = select([	func.count( func.distinct(heads.c.client_id) ) , heads.c.store_id	]).\
				where(	heads.c.ticket_date.between('2016-01-01','2017-12-31')	).\
				group_by(	heads.c.store_id	)
	 
	for count,store in conn.execute(statement).fetchall():
		print('Count:',count,'Store:',store)


def getAttritions(conn):
	for store in getStores(conn):
		store = store[0]
		statement = select([	 func.count(	func.distinct(heads.c.client_id) 	)	]).\
					where(	
						and_(
							heads.c.client_id.notin_(	
								select([	func.distinct(heads.c.client_id) ]).\
								where(
									and_(	func.year(heads.c.ticket_date) == 2017,	heads.c.store_id == store 	)
								)
							),

							func.year(heads.c.ticket_date) == 2016,
							heads.c.store_id == store 
						)	
					)

		 
		for count in conn.execute(statement).fetchall():
			print('Count:',count[0],'Store:',store)





def getNew(conn):
	for store in getStores(conn):
		store = store[0]
		statement = select([	 func.count(	func.distinct(heads.c.client_id) 	)	]).\
					where(	
						and_(
							heads.c.client_id.notin_(	
								select([	func.distinct(heads.c.client_id) ]).\
								where(
									and_(	func.year(heads.c.ticket_date) == 2016,	heads.c.store_id == store 	)
								)
							),
							func.year(heads.c.ticket_date) == 2017,
							heads.c.store_id == store 
						)	
					)
		for count in conn.execute(statement).fetchall():
			print('Count:',count[0],'Store:',store)



def getMax(conn):
	years = []
	f = func.max( heads.c.total_ticket_cost )
	quarters = {}
	for count,store,date,quart in getPerStore(conn, f ):
		d = datetime.strptime(date,'%Y-%m')
		y,m = d.year,d.month
		d1 = f'{y}-{m-2:02d}-01'
		years.append(y) if y not in years else None
		print('[+] Date:',date,'Store:',store,'Max:',count)
		if d1 not in quarters and m%3==0:
			quarters[d1] = date+'-'+str(monthrange(y,m)[1])
	print()
			
	for q1,q2 in quarters.items():
		for count,store in getFromTo(conn,q1,q2, f ):
			print("[+] From:",q1,"To:",q2,'Store:',store,"Max:",count)
	print()
	
	for year in years:
		for count,store in getPerYear(conn,year, f ):
			print('[+] Year:',year,'Max:',count,'Store:',store)
	print()


def getMin(conn):
	years = []
	f = func.min( heads.c.total_ticket_cost )
	quarters = {}
	for count,store,date,quart in getPerStore(conn, f ):
		d = datetime.strptime(date,'%Y-%m')
		y,m = d.year,d.month
		d1 = f'{y}-{m-2:02d}-01'
		years.append(y) if y not in years else None
		print('[+] Date:',date,'Store:',store,'Min:',count)
		if d1 not in quarters and m%3==0:
			quarters[d1] = date+'-'+str(monthrange(y,m)[1])
	print()
			
	for q1,q2 in quarters.items():
		for count,store in getFromTo(conn,q1,q2, f ):
			print("[+] From:",q1,"To:",q2,'Store:',store,"Min:",count)
	print()
	
	for year in years:
		for count,store in getPerYear(conn,year, f ):
			print('[+] Year:',year,'Min:',count,'Store:',store)
	print()


def getSales(conn):
	years = []
	f = func.sum( heads.c.total_ticket_cost )
	quarters = {}
	for count,store,date,quart in getPerStore(conn, f ):
		d = datetime.strptime(date,'%Y-%m')
		y,m = d.year,d.month
		d1 = f'{y}-{m-2:02d}-01'
		years.append(y) if y not in years else None
		print('[+] Date:',date,'Store:',store,'Sales:',count)
		if d1 not in quarters and m%3==0:
			quarters[d1] = date+'-'+str(monthrange(y,m)[1])
	print()
			
	for q1,q2 in quarters.items():
		for count,store in getFromTo(conn,q1,q2, f ):
			print("[+] From:",q1,"To:",q2,'Store:',store,"Sales:",count)
	print()
	
	for year in years:
		for count,store in getPerYear(conn,year, f ):
			print('[+] Year:',year,'Sales:',count,'Store:',store)
	print()





def getVipCustomers(conn):
	statement = select([	func.count(	func.distinct(heads.c.client_id) ), heads.c.store_id	]).\
				where(
					and_(
						heads.c.ticket_date.between('2016-01-01','2017-12-31'),
						heads.c.discount_volume	> 0
					)
				).\
				group_by(	heads.c.store_id	)

	return conn.execute(statement).fetchall()



def getVipDiscounts(conn):
	statement = select([	func.sum(	heads.c.discount_volume ), heads.c.store_id	]).\
				where(
					and_(
						heads.c.ticket_date.between('2016-01-01','2017-12-31'),
					)
				).\
				group_by(	heads.c.store_id	)

	return conn.execute(statement).fetchall()


now = time.time()
with engine.connect() as conn:
	# getClientsCount(conn)
	# getLoyals(conn)
	# getAttritions(conn)
	# getVipCustomers(conn)
	# getVipDiscounts(conn)
	# getNew(conn)
	# getMax(conn)
	# getMin(conn)
	# getSales(conn)	
	# # TO BE UPDATED getGrowthRate(conn)	
	# # TO BE UPDATED getAttritionRate(conn)	
	# # TO BE UPDATED getRatio(conn)	
	pass

print("Total time: %f" % (time.time() - now))
