from dbCreator import *
from sqlalchemy.sql import select,and_, or_, not_
from sqlalchemy import func
from calendar import monthrange
import time,threading



"""

FUNCTIONS TO BE ADDED

"""


def thread(y,store,array={},statement = None):
	if  statement is None:
		statement = select([	 func.distinct(heads.c.client_id)	]).\
					where(	
						and_(
							func.year(heads.c.ticket_date) == y,
							heads.c.store_id == store 
						)	
					)
	array[store] = []
	with engine.connect() as conn:
		[array[store].append(cid) for cid in conn.execute(statement).fetchall()]
	return array


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



def definingThread(conn,year,discount=False):
	array ={}
	threads = []
	print("Getting Stores\n")
	stores = getStores(conn)
	for store in stores:
		store = store[0]
		statement = None
		if discount:
			statement = select([	heads.c.client_id, func.sum(heads.c.discount_volume)	]).\
						where(	
							and_(
								func.year(heads.c.ticket_date) == year,
								heads.c.store_id == store ,
								heads.c.discount_volume > 0
							)	
						).group_by(heads.c.client_id)

		th = threading.Thread(target=thread,args=(year,store,array, statement))
		th.start()
		th.name = store
		print("Starting Thread for store:",th.name)
		threads.append(th)
		if len(threads) % 10 == 0:
			print("JOINING THREADS")
			while threads:
				threads.pop().join()
	if threads:
		print("JOINING THREADS")
		while threads:
			threads.pop().join()

	return array


def getLoyals(conn,getClients=False):
	lastYearClients = definingThread(conn,2016)
	newYearClients = definingThread(conn,2017)
	loyals = {}


	for store in newYearClients:
		print("Finding loyals clients for Store:",store)
		if not lastYearClients.get(store) or not newYearClients.get(store):
			loyals[store] = 0 if not getClients else []
			continue
		for cid in newYearClients.get(store,[]):
			if cid in lastYearClients.get(store):
				if loyals.get(store):
					loyals[store] += 1 if not getClients else [cid]
				else:
					loyals[store] = 1 if not getClients else [cid]

	return loyals


def getAttritions(conn,getClients=False):
	lastYearClients = definingThread(conn,2016)
	newYearClients = definingThread(conn,2017)
	attritions = {}


	for store in lastYearClients:
		print("Finding attritions clients for Store:",store)
		if not lastYearClients.get(store):
			attritions[store] = 0 if not getClients else []
			continue
		for cid in lastYearClients.get(store,[]):
			if cid not in newYearClients.get(store):
				if store in attritions:
					attritions[store] += 1 if not getClients else [cid]
				else:
					attritions[store] = 1 if not getClients else [cid]
	return attritions



def getNew(conn,getClients=False):
	lastYearClients = definingThread(conn,2016)
	newYearClients = definingThread(conn,2017)

	new = {}

	for store in newYearClients:
		print("Finding new clients for Store:",store)
		if not newYearClients.get(store):
			new[store] = 0 if not getClients else []
			continue
		for cid in newYearClients.get(store,[]):
			if cid not in lastYearClients.get(store):
				if store in new:
					new[store] += 1 if not getClients else [cid]
				else:
					new[store] = 1 if not getClients else [cid]
	return new



def getMQY(conn,f):
	yearlyData = {}
	quaterlyData = {}
	monthlyData = {}

	years = []
	quarters = {}
	for count,store,date,quart in getPerStore(conn,f):


		d = datetime.strptime(date,'%Y-%m')
		y,m = d.year,d.month
		d1 = f'{date}-01'
		
		if y not in yearlyData:
			yearlyData[y] = {store:0}
		else:
			yearlyData[y][store] = 0


		if date not in monthlyData:
			monthlyData[date] = {store:count}
		else:
			monthlyData[date][store] = count 


		years.append(y) if y not in years else None
		if d1 not in quarters and m%3==0:
			quarters[d1] = date+'-'+str(monthrange(y,m)[1])
			if d1 not in quaterlyData:
				quaterlyData[d1] = {store:0}
			else:
				quaterlyData[d1][store] = 0
			
	for q1,q2 in quarters.items():
		for count,store in getFromTo(conn,q1,q2,f):
			quaterlyData[q1][store] = count
	
	for year in years:
		for count,store in getPerYear(conn,year,f):
			yearlyData[year][store] = count

	return monthlyData,quaterlyData,yearlyData

def getClientsCount(conn):
	f = func.count( func.distinct(heads.c.client_id) ) 
	return getMQY(conn,f)

def getMax(conn):
	f = func.max( heads.c.total_ticket_cost )
	return getMQY(conn,f)
	
def getMin(conn):
	f = func.min( heads.c.total_ticket_cost )
	return getMQY(conn,f)
	
def getSales(conn):
	f = func.sum( heads.c.total_ticket_cost )
	return getMQY(conn,f)
	




def getVipCustomers(conn):
	
	lastYearClients = definingThread(conn,2016,discount=True)
	newYearClients = definingThread(conn,2017,discount=True)

	vip = {}

	for store in newYearClients:
		print("Finding vip clients for Store:",store)
		if not newYearClients.get(store) or not lastYearClients.get(store):
			vip[store] = 0 if not getClients else []
			continue
		for cid in newYearClients.get(store,[]):
			if cid in lastYearClients.get(store):
				if store in vip:
					vip[store] += 1 if not getClients else [cid]
				else:
					vip[store] = 1 if not getClients else [cid]
	return vip




def getVipDiscounts(conn):
	statement = select([	func.sum(	heads.c.discount_volume ), heads.c.store_id	]).\
				where(
					and_(
						func.year(heads.c.ticket_date).between(2016,2017),
					)
				).\
				group_by(	heads.c.store_id	)

	return {store:discount for discount,store in conn.execute(statement).fetchall()}



def getGrowthRate(conn):
	stores = getNew(conn)
	f = func.count( func.distinct(heads.c.client_id) ) 
	totalLastYear = {s:c for c,s in getPerYear(conn,2016,f)}
	growthRates = {}
	for store in stores:
		if not totalLastYear.get(store):
			growthRates[store] = 100.0
		else:
			growthRates[store] = round((stores[store]  /  totalLastYear[store]) * 100, 2)
	
	return growthRates




def getAttritionRate(conn):
	stores = getAttritions(conn)
	f = func.count( func.distinct(heads.c.client_id) ) 
	averageClients = {s:c/2 for c,s in getFromTo(conn,2016,2017,f)}
	attritionRates = {}
	for store in stores:
		if not averageClients.get(store):
			attritionRates[store] = 0.0
		else:
			attritionRates[store] = round((stores[store]  /  averageClients[store]) * 100, 2)
	return attritionRates


def getRatio(conn):
	statement = select([	func.sum( heads.c.total_ticket_cost ), func.count(func.distinct(heads.c.client_id)) , heads.c.store_id	]).\
				group_by(	heads.c.store_id	)

	return {store:round(cost/clients,2) for cost,clients,store in conn.execute(statement).fetchall()}

now = time.time()
with engine.connect() as conn:
	# print(getClientsCount(conn))
	# print(getLoyals(conn))
	# print(getAttritions(conn))
	# print(getNew(conn))
	# print(getVipCustomers(conn))
	# getVipDiscounts(conn)
	# getMax(conn)
	# getMin(conn)
	# getSales(conn)	
	print(getGrowthRate(conn))	
	# print(getAttritionRate(conn))	
	# print(getRatio(conn))	
	pass

print("Total time: %f" % (time.time() - now))
