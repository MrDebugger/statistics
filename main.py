from dbCreator import *
from sqlalchemy.sql import select,and_, or_, not_
from sqlalchemy import func
from calendar import monthrange
import time,threading



"""

1. MORE EXPLANATION TO BE ADDED
2. GRAPHICS TO BE ADDED
3. TO BE CONVERTED TO OOP

"""


def thread(y,store,array={},statement = None):
	"""
		Thread that finds all non duplicate clients of a specified store in a specified year, executing the following example query
		"SELECT DISTINCT(client_id) FROM heads WHERE YEAR(ticket_date) = 2016 AND store_id = 'ALB'"
	"""
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
		[array[store].append(cid[0]) for cid in conn.execute(statement).fetchall()]
	return array


def getPerYear(conn,year,f):
	"""
		Gets max/min/(total cost)/(number of customer) per store in selected year, Executing the following example query out of four
		"SELECT COUNT(DISTINCT(`client_id`)), `store_id` FROM `heads` WHERE YEAR(`ticket_date`) = 2017 GROUP BY `store_id`"
	"""
	statement = select([	f , heads.c.store_id	]).\
				where(	func.year(heads.c.ticket_date) == year	).\
				group_by(heads.c.store_id)

	return conn.execute(statement).fetchall()

def getFromTo(conn,year1,year2,f):
	"""
		Gets max/min/(total cost)/(number of customer) per store between two dates, Executing the following example query out of four
		"SELECT COUNT(DISTINCT(`client_id`)), `store_id` FROM `heads` WHERE `ticket_date` BETWEEN 2016 AND 2017 GROUP BY `store_id`"
	"""
	statement = select([	f,	heads.c.store_id	]).\
				where(	heads.c.ticket_date.between(year1,year2)	).\
				group_by(	heads.c.store_id	)

	return conn.execute(statement).fetchall()

def getPerStore(conn,f):
	"""	
		Gets max/min/(total cost)/(number of customer) per store by month, Executing the following example query out of four
		"SELECT COUNT(DISTINCT(`client_id`)), DATE_FORMAT(`ticket_date`, '%Y-%m') as date_log, QUARTER(`ticket_date`) FROM `heads` GROUP BY `store_id`, date_log"
	"""
	statement = select([	f,	heads.c.store_id,	func.date_format(heads.c.ticket_date,'%Y-%m').label('date_log'),	func.quarter(heads.c.ticket_date)	]).\
				group_by(	heads.c.store_id	).\
				group_by(	'date_log'	)
	return conn.execute(statement).fetchall()



def getStores(conn):
	"""
		Gets all stores by executing the following query
		"SELECT `store_id` FROM `stores`"
	"""

	statement = select([stores.c.store_id])
	return conn.execute(statement).fetchall()



def definingThread(conn,year,discount=False):
	"""
		Method that deals with creating threads and joining their data into one dictionary.
		If there is a discount value to find (VIP CUSTOMERS)
		the following example query is executed instead of the original.
		"SELECT client_id,SUM(discount_volume) from heads WHERE YEAR(ticket_date) = 2016 AND store_id = 'ALB' AND discount_volume > 0 GROUP BY client_id"
	"""

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
	"""
		Finds the clients/customers in 2016 and 2017. then check if customers/clients present in 2016 is present in 2017 or not. 
		If the client is available in both years then it is saved as loyal client
	"""

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
	"""
		Finds the clients/customers in 2016 and 2017. then check if customers/clients present in 2016 is present in 2017 or not. 
		If not present then saves it as attrition/lost client to the store
	"""

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
	"""
		Finds the clients/customers in 2016 and 2017. then check if customers/clients present in 2017 is present in 2016 or not. 
		If not present then saves it as new client to the store
	"""

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

	"""
	This function is divided into 3 parts.
	1. Finds (number of clients)/max/min/(total sales) for each store in a month. Saves the quater dates and years for remainig parts
	2. Finds (number of clients)/max/min/(total sales) for each store in a Quarter.
	3. Finds (number of clients)/max/min/(total sales) for each store in a year.

	"""

	yearlyData = {}
	quaterlyData = {}
	monthlyData = {}

	# part 1
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

	# part 2
	for q1,q2 in quarters.items():
		for count,store in getFromTo(conn,q1,q2,f):
			quaterlyData[q1][store] = count
	
	# part 3
	for year in years:
		for count,store in getPerYear(conn,year,f):
			yearlyData[year][store] = count

	return monthlyData,quaterlyData,yearlyData

def getClientsCount(conn):
	"""
		finds number of clients/customers for each store in month, quater and year
	"""
	f = func.count( func.distinct(heads.c.client_id) ) 
	return getMQY(conn,f)

def getMax(conn):
	"""
		finds Maximum sales done by each store in month, quater and year
	"""
	f = func.max( heads.c.total_ticket_cost )
	return getMQY(conn,f)
	
def getMin(conn):
	"""
		finds Minimum sales done by each store in month, quater and year
	"""
	f = func.min( heads.c.total_ticket_cost )
	return getMQY(conn,f)
	
def getSales(conn):
	"""
		finds total sales done by each store in month, quater and year
	"""
	f = func.sum( heads.c.total_ticket_cost )
	return getMQY(conn,f)
	




def getVipCustomers(conn):
	"""
		collects 2016 and 2017 clients and check if each clients done purchases in 2016 and 2017 and also granted the discounts
	"""

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
	"""
	Executes the following MySQL Query to get volume of discount coupons and grouping them by store_id
	"SELECT SUM(`discount_volume`), `store_id` from `heads` WHERE YEAR(`ticket_date`) BETWEEN 2016 and 2017 GROUP BY `store_id`"
	"""
	statement = select([	func.sum(	heads.c.discount_volume ), heads.c.store_id	]).\
				where(
					and_(
						func.year(heads.c.ticket_date).between(2016,2017),
					)
				).\
				group_by(	heads.c.store_id	)

	return {store:discount for discount,store in conn.execute(statement).fetchall()}



def getGrowthRate(conn):

	"""
		Gets clients done purchases only in 2017 (new clients) and divide them by total customers in previous year (2016)
		to find	Growth Rate of the stores
	"""

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
	"""
		Gets clients done purchases only in 2016 (old clients) and divide them by average clients of the store (average = total clients/2)
		to find	Attrition Rate of the stores
	"""

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

	"""
		Finds the ration between clients and revenues for each store by executing the following query
		"SELECT SUM(total_ticket_cost), COUNT(DISTINCT(client_id)), store_id from heads GROUP BY store_id"
	"""

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
	# print(getGrowthRate(conn))	
	# print(getAttritionRate(conn))	
	# print(getRatio(conn))	
	pass

print("Total time: %f" % (time.time() - now))
