from dbCreator import *
from sqlalchemy.sql import select,and_, or_, not_
from sqlalchemy import func
from calendar import monthrange
import time,threading
import plotly.graph_objects as go
from plotly import subplots

"""

1. MORE EXPLANATION TO BE ADDED
2. GRAPHICS TO BE ADDED
3. TO BE CONVERTED TO OOP

"""
class Statistics:

	def __init__(self):

		self.lastYearClients={}
		self.allData={}
		self.seqData=[]
		self.stores=[]
		self.newYearClients={}
		self.lastYearClientsWithDiscount={}
		self.newYearClientsWithDiscount={}
		self.attritions={}
		self.new={}
		self.loyals ={}



		now = time.time()
		with engine.connect() as self.conn:
			with open('figure.html','w') as f:

				f.write('<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>')

				fig = go.Figure()
				for func in [self.getLoyals,self.getAttritions,self.getNew,self.getVipCustomers]:
					func(fig)

				labels = ["Loyal","Attrition","New","VIP"]
				buttons = self.create_layout_buttons(labels)
				fig.update_layout(barmode='group',
							updatemenus=[go.layout.Updatemenu(
								buttons = buttons
							)]
						)
				f.write(fig.to_html(full_html=False))

				if not self.stores:
					self.stores = self.getStores()
				buttons = self.create_layout_buttons(self.stores)
				for func,name in zip([self.getClientsCount,self.getMax,self.getMin,self.getSales],
										["Number of Clients","Max Expenditure","Min Expenditure","Sale per store"]):
					fig = subplots.make_subplots(rows=2, cols=2,
									specs = [[{},{}],[{'colspan':2},None]],
									subplot_titles= ("Month","Quarter","Year")
								)
					func(fig)
					fig.update_layout(title_text=name,updatemenus=[
							go.layout.Updatemenu(buttons=buttons)
						])
					f.write(fig.to_html(full_html=False))

				

				fig = go.Figure()
				for func in  [self.getGrowthRate, self.getAttritionRate]:
					func(fig)
				labels = ['Growth Rate','Attrition Rate']
				buttons = self.create_layout_buttons(labels)
				fig.update_layout(updatemenus=[go.layout.Updatemenu(buttons=buttons)])
				f.write(fig.to_html(full_html=False))
				
				for func,name in zip([self.getRatio,self.getVipDiscounts],['Ratio','VIP Discounts']):
					fig = go.Figure()
					func(fig)
					fig.update_layout(title_text=name)	
					f.write(fig.to_html(full_html=False))
				
		print("Total time: %f" % (time.time() - now))

	def create_layout_buttons(self,labels,clients=False):
		if not clients:
			buttons =[dict( label = 'All',method='update',args = [{'visible': [True]*len(labels)}])]
			for i,label in enumerate(labels):
				visibility = [i==j for j in range(len(labels))]
				button = dict( label = label,method='update',args = [{'visible': visibility}])
				buttons.append(button)
		return buttons

	def create_button(self,title,x,y):
		return dict(label = 'All',
	                      method = 'update',
	                      args = [{'x': [[store for store in self.stores]] * len(self.seqData),
	                      			'y': self.seqData,
	                      			'text': self.seqData,
	                               'title': 'All',
	                               'showlegend':True}])

		



	def thread(self,y,store,array={},statement = None):
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


	def getPerYear(self,year,f):
		"""
			Gets max/min/(total cost)/(number of customer) per store in selected year, Executing the following example query out of four
			"SELECT COUNT(DISTINCT(`client_id`)), `store_id` FROM `heads` WHERE YEAR(`ticket_date`) = 2017 GROUP BY `store_id`"
		"""
		statement = select([	f , heads.c.store_id	]).\
					where(	func.year(heads.c.ticket_date) == year	).\
					group_by(heads.c.store_id)

		return self.conn.execute(statement).fetchall()

	def getFromTo(self,year1,year2,f):
		"""
			Gets max/min/(total cost)/(number of customer) per store between two dates, Executing the following example query out of four
			"SELECT COUNT(DISTINCT(`client_id`)), `store_id` FROM `heads` WHERE `ticket_date` BETWEEN 2016 AND 2017 GROUP BY `store_id`"
		"""
		statement = select([	f,	heads.c.store_id	]).\
					where(	heads.c.ticket_date.between(year1,year2)	).\
					group_by(	heads.c.store_id	)

		return self.conn.execute(statement).fetchall()

	def getPerStore(self,f):
		"""	
			Gets max/min/(total cost)/(number of customer) per store by month, Executing the following example query out of four
			"SELECT COUNT(DISTINCT(`client_id`)), DATE_FORMAT(`ticket_date`, '%Y-%m') as date_log, QUARTER(`ticket_date`) FROM `heads` GROUP BY `store_id`, date_log"
		"""
		statement = select([	f,	heads.c.store_id,	func.date_format(heads.c.ticket_date,'%Y-%m').label('date_log'),	func.quarter(heads.c.ticket_date)	]).\
					group_by(	heads.c.store_id	).\
					group_by(	'date_log'	)
		return self.conn.execute(statement).fetchall()



	def getStores(self):
		"""
			Gets all stores by executing the following query
			"SELECT `store_id` FROM `stores`"
		"""

		statement = select([stores.c.store_id])
		Stores = []
		for store in self.conn.execute(statement).fetchall():
			self.allData[store[0]] = []
			Stores.append(store[0])
		return Stores


	def definingThread(self,year,discount=False):
		"""
			Method that deals with creating threads and joining their data into one dictionary.
			If there is a discount value to find (VIP CUSTOMERS)
			the following example query is executed instead of the original.
			"SELECT client_id,SUM(discount_volume) from heads WHERE YEAR(ticket_date) = 2016 AND store_id = 'ALB' AND discount_volume > 0 GROUP BY client_id"
		"""

		array ={}
		threads = []
		print("Getting Stores\n")
		if not self.stores:
			self.stores = self.getStores()
		for store in self.stores:
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

			th = threading.Thread(target=self.thread,args=(year,store,array, statement))
			th.start()
			th.name = store
			threads.append(th)
			if len(threads) % 10 == 0:
				while threads:
					threads.pop().join()
		if threads:
			while threads:
				threads.pop().join()

		return array


	def getLoyals(self,fig=None,getClients=False):
		"""
			Finds the clients/customers in 2016 and 2017. then check if customers/clients present in 2016 is present in 2017 or not. 
			If the client is available in both years then it is saved as loyal client
		"""

		if not self.lastYearClients:
			self.lastYearClients = self.definingThread(2016)
		if not self.newYearClients:
			self.newYearClients = self.definingThread(2017)
		self.loyals = {}


		for store in self.newYearClients:
			print("Finding loyals clients for Store:",store)
			if not self.lastYearClients.get(store) or not self.newYearClients.get(store):
				self.loyals[store] = 0 if not getClients else []
				self.allData[store].append(0)
				continue
			for cid in self.newYearClients.get(store,[]):
				if cid in self.lastYearClients.get(store):
					if self.loyals.get(store):
						self.loyals[store] += 1 if not getClients else [cid]
					else:
						self.loyals[store] = 1 if not getClients else [cid]
			self.allData[store].append(self.loyals[store])
		self.seqData.append(list(self.loyals.values()))
		if fig:
			fig.add_trace(go.Bar(
				x = self.stores,
				y = list(self.loyals.values()),
				name = "Loyals",
				text = list(self.loyals.values()),
				textposition='outside',
				marker_color = "green"
			))	
		return self.loyals


	def getAttritions(self,fig=None,getClients=False):
		"""
			Finds the clients/customers in 2016 and 2017. then check if customers/clients present in 2016 is present in 2017 or not. 
			If not present then saves it as attrition/lost client to the store
		"""

		if not self.lastYearClients:
			self.lastYearClients = self.definingThread(2016)
		if not self.newYearClients:
			self.newYearClients = self.definingThread(2017)

		self.attritions = {}


		for store in self.lastYearClients:
			print("Finding attritions clients for Store:",store)
			if not self.lastYearClients.get(store):
				self.allData[store].append(0)
				self.attritions[store] = 0 if not getClients else []
				continue
			for cid in self.lastYearClients.get(store,[]):
				if cid not in self.newYearClients.get(store):
					if store in self.attritions:
						self.attritions[store] += 1 if not getClients else [cid]
					else:
						self.attritions[store] = 1 if not getClients else [cid]
			self.allData[store].append(self.attritions[store])
		self.seqData.append(list(self.attritions.values()))
		if fig:
			fig.add_trace(go.Bar(
				x = self.stores,
				y = list(self.attritions.values()),
				name = "Attritions",
				marker_color = "red"
			))	
		return self.attritions



	def getNew(self,fig=None,getClients=False):
		"""
			Finds the clients/customers in 2016 and 2017. then check if customers/clients present in 2017 is present in 2016 or not. 
			If not present then saves it as new client to the store
		"""

		if not self.lastYearClients:
			self.lastYearClients = self.definingThread(2016)
		if not self.newYearClients:
			self.newYearClients = self.definingThread(2017)

		self.new = {}

		for store in self.newYearClients:
			print("Finding new clients for Store:",store)
			if not self.newYearClients.get(store):
				self.new[store] = 0 if not getClients else []
				self.allData[store].append(0)
				continue
			for cid in self.newYearClients.get(store,[]):
				if cid not in self.lastYearClients.get(store):
					if store in self.new:
						self.new[store] += 1 if not getClients else [cid]
					else:
						self.new[store] = 1 if not getClients else [cid]
			self.allData[store].append(self.new[store])
		
		self.seqData.append(list(self.new.values()))
		if fig:
			fig.add_trace(go.Bar(
				x = self.stores,
				y = list(self.new.values()),
				name = "New",
				marker_color = "yellow"
			))	

		return self.new



	def getMQY(self,f,fig):

		"""
		This function is divided into 3 parts.
		1. Finds (number of clients)/max/min/(total sales) for each store in a month. Saves the quater dates and years for remainig parts
		2. Finds (number of clients)/max/min/(total sales) for each store in a Quarter.
		3. Finds (number of clients)/max/min/(total sales) for each store in a year.

		"""

		if not self.stores:
			self.stores = self.getStores()
		yearlyData = {}
		quaterlyData = {}
		monthlyData = {}
		for st in self.stores:
			yearlyData[st] = {}
			quaterlyData[st] = {}
			monthlyData[st] = {}
		# part 1
		years = []
		quarters = {}
		d1 =''
		for count,store,date,quart in self.getPerStore(f):
			d = datetime.strptime(date,'%Y-%m')
			y,m = d.year,d.month
			d1 = f'{date}-01'
			
			yearlyData[store][y]=0
			monthlyData[store][date]=count

			years.append(y) if y not in years else None
			if d1 not in quarters and m%3==0:
				quarters[d1] = date+'-'+str(monthrange(y,m)[1])
				quaterlyData[store][d1]=0

		for st in monthlyData:
			fig.append_trace(
				go.Scatter(
					x=list(monthlyData[st].keys()),
					y=list(monthlyData[st].values()),
					name=st
				),1,1
			)
		# part 2
		for q1,q2 in quarters.items():
			for count,store in self.getFromTo(q1,q2,f):
				quaterlyData[store][q1] = count
		
		for st in quaterlyData:
			fig.append_trace(
				go.Scatter(
					x=list(quaterlyData[st].keys()),
					y=list(quaterlyData[st].values()),
					name=st
				),1,2
			)
		# part 3
		for year in years:
			for count,store in self.getPerYear(year,f):
				yearlyData[store][str(year)] = count

		for st in yearlyData:
			fig.append_trace(
				go.Bar(
					x=list(yearlyData[st].keys()),
					y=list(yearlyData[st].values()),
					name=st,
				),2,1
			)
		return monthlyData,quaterlyData,yearlyData

	def getClientsCount(self,fig):
		"""
			finds number of clients/customers for each store in month, quater and year
		"""
		f = func.count( func.distinct(heads.c.client_id) ) 
		return self.getMQY(f,fig)

	def getMax(self,fig):
		"""
			finds Maximum sales done by each store in month, quater and year
		"""
		f = func.max( heads.c.total_ticket_cost )
		return self.getMQY(f,fig)
		
	def getMin(self,fig):
		"""
			finds Minimum sales done by each store in month, quater and year
		"""
		f = func.min( heads.c.total_ticket_cost )
		return self.getMQY(f,fig)
		
	def getSales(self,fig):
		"""
			finds total sales done by each store in month, quater and year
		"""
		f = func.sum( heads.c.total_ticket_cost )
		return self.getMQY(f,fig)
		




	def getVipCustomers(self,fig,getClients=False):
		"""
			collects 2016 and 2017 clients and check if each clients done purchases in 2016 and 2017 and also granted the discounts
		"""

		if not self.lastYearClientsWithDiscount:
			self.lastYearClientsWithDiscount = self.definingThread(2016,discount=True)
		if not self.newYearClientsWithDiscount:
			self.newYearClientsWithDiscount = self.definingThread(2017,discount=True)

		vip = {}

		for store in self.newYearClientsWithDiscount:
			print("Finding vip clients for Store:",store)
			if not self.newYearClientsWithDiscount.get(store) or not self.lastYearClientsWithDiscount.get(store):
				vip[store] = 0 if not getClients else []
				self.allData[store].append(0)
				continue
			for cid in self.newYearClientsWithDiscount.get(store,[]):
				if cid in self.lastYearClientsWithDiscount.get(store):
					if store in vip:
						vip[store] += 1 if not getClients else [cid]
					else:
						vip[store] = 1 if not getClients else [cid]
			self.allData[store].append(vip[store])
		self.seqData.append(list(vip.values()))
		fig.add_trace(go.Bar(
			x = list(vip.keys()),
			y = list(vip.values()),
			name = "VIP",
			marker_color = "blue"
		))	

		return vip


	def getVipDiscounts(self,fig):
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
		discounts = {}
		for discount,store in self.conn.execute(statement).fetchall():
			discounts[store]=discount 
		fig.add_trace(go.Bar(x=self.stores,y=list(discounts.values())))
		return discounts



	def getGrowthRate(self,fig):

		"""
			Gets clients done purchases only in 2017 (new clients) and divide them by total customers in previous year (2016)
			to find	Growth Rate of the stores
		"""

		if not self.new:
			self.new = self.getNew()

		if not self.lastYearClients:
			self.lastYearClients = self.definingThread(2016)
		growthRates = {}
		for store,count in self.new.items():
			if not self.lastYearClients.get(store):
				growthRates[store] = 100.0
			else:
				growthRates[store] = round((count  /  len(self.lastYearClients[store])) * 100, 2)
		
		fig.add_trace(go.Bar(x=self.stores,y=list(growthRates.values()),name="Growth Rate"))
		return growthRates




	def getAttritionRate(self,fig):
		"""
			Gets clients done purchases only in 2016 (old clients) and divide them by average clients of the store (average = total clients/2)
			to find	Attrition Rate of the stores
		"""

		if not self.attritions:
			self.attritions = self.getAttritions()

		averageClients = {}
		for store,count in self.lastYearClients.items():
			averageClients[store] = (len(count) + len(self.newYearClients.get(store,[]))) / 2

		attritionRates = {}
		for store,count in self.attritions.items():
			if not averageClients.get(store):
				attritionRates[store] = 0.0
			else:
				attritionRates[store] = round((count  /  averageClients[store]) * 100, 2)
		fig.add_trace(go.Bar(x=self.stores,y=list(attritionRates.values()),name="Attrition Rate"))
		return attritionRates


	def getRatio(self,fig):

		"""
			Finds the ration between clients and revenues for each store by executing the following query
			"SELECT SUM(total_ticket_cost), COUNT(DISTINCT(client_id)), store_id from heads GROUP BY store_id"
		"""

		statement = select([	func.sum( heads.c.total_ticket_cost ), func.count(func.distinct(heads.c.client_id)) , heads.c.store_id	]).\
					group_by(	heads.c.store_id	)
		if not self.stores:
			self.stores = self.getStores()
		ratios = {store:0.0 for store in self.stores}
		for cost,clients,store in self.conn.execute(statement).fetchall():
			ratios[store]=round(cost/clients,2)
		fig.add_trace(go.Bar(x=self.stores,y=list(ratios.values()),name="Ratio"))
		return ratios

s = Statistics()
