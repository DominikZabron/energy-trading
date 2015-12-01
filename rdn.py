import sqlite3
from numpy import *
from datetime import *

class RDN(object):

	def __init__(self, date, demand=[200 for x in range(24)], fixing=1, conf_level=0.90):
		self.date = date
		self.demand = demand
		self.fixing = fixing
		self.conf_level = conf_level
		self.daytype = self._retrieve('SELECT Daytype FROM Dates WHERE date = ?', (self.date,))
		self.safe = self.safe_price()
		self.min = self.min_price()
		self.est = self.est_demand()
		self.ref = []
		self.market_data = []
		self.purchase = [[0 for x in range(24)]]
		self.var = self.VaR()
		self.calls = []
		self.result = []
		self.rest = []
		
		
	def _retrieve(self, query, options):
		data = []
		connection = sqlite3.connect('Energy.db')
		
		with connection:
			current = connection.cursor()
			current.execute(query, options)
			
			while True:
				row = current.fetchone()
				if row == None: 
					break
				elif len(row) > 24:
					row = list(row)
					
					for i in range(len(row)):
						if row[i] != None:
							pass
						else:
							row[i] = nan
								
					data.append(list(row[1:25]))
								
				elif len(row) == 1:
					data = row[0]
		
		return data
	
	def safe_price(self, limit=360):
		query = "SELECT * FROM MarketData INNER JOIN Dates ON MarketData.Date = Dates.Date WHERE MarketData.Date < ? AND Dates.DayType = ? AND MarketData.Type = 0 ORDER BY Date DESC LIMIT ?"
		opt = (self.date, self.daytype, limit)
		prices = self._retrieve(query, opt)
		prices = transpose(prices)
		avg = mean(prices, axis = 1)
		sd = std(prices, axis = 1)
		safe = (avg + sd / 2)
		return safe

	def est_demand(self):
		query = "SELECT * FROM MarketData INNER JOIN Dates ON MarketData.Date = Dates.Date WHERE MarketData.Date < ? AND Dates.DayType = ? AND MarketData.Type IN (5, 6, 7) ORDER BY Date DESC LIMIT 3"
		opt = (self.date, self.daytype)
		volumes = self._retrieve(query, opt)
		sums = nansum(transpose(volumes), axis = 1)
		demand = divide(volumes, sums)
		demand = nan_to_num(demand)
		self.est = demand
		return demand
						
	def min_price(self, limit=20):
		query = "SELECT * FROM MarketData INNER JOIN Dates ON MarketData.Date = Dates.Date WHERE MarketData.Date < ? AND MarketData.Type IN (1, 2, 3) AND Dates.DayType = ? ORDER BY Date DESC LIMIT ?"
		opt = (self.date, self.daytype, limit)
		prices = self._retrieve(query, opt)
		price = transpose(prices)
		min = nanmin(price, axis = 1)
		return min
	
	def average(self, date, ext=3):
		query = "SELECT * FROM MarketData INNER JOIN Dates ON MarketData.Date = Dates.Date WHERE MarketData.Date = ? AND Dates.DayType = ? AND MarketData.Type IN (1, 2, ?) ORDER BY Date DESC LIMIT 3"
		opt = (date, self.daytype, ext)
		prices = self._retrieve(query, opt)
		mprices = ma.masked_array(prices, isnan(prices))
		if ext == 3:
			volumes = self.est_demand()
		else:
			volumes = self.est_demand()[:2]
		mvolumes = ma.masked_array(volumes, isnan(volumes)) 
		avg = ma.average(transpose(mprices), 1, transpose(mvolumes))
		return avg.filled(nan)

	def VaR(self, limit=360):
		query = "SELECT * FROM MarketData INNER JOIN Dates ON MarketData.Date = Dates.Date WHERE MarketData.Date < ? AND Dates.DayType = ? AND MarketData.Type = 0 ORDER BY Date DESC LIMIT ?"	
		opt = (self.date, self.daytype, limit)
		prices = self._retrieve(query, opt)
		i = int(len(prices)*(1 - self.conf_level))
		queue = sort(transpose(prices))
		var = [queue[x][-i] for x in range(len(queue))]
		return var
		
	def mitigate(self, x=0.3):
		f = self.fixing - 1
		for i in range(24):
			ex = ((self.ref[f][i] * sum(self.purchase, 0)[i]) + (self.rest[f-1][i] * self.var[i])) / (self.ref[f][i] * (sum(self.purchase, 0)[i] + self.rest[f-1][i]))
			if x + 1 < ex:
				self.ref[f][i] = self.var[i]
				
	def fix(self, freq=10):
		if self.fixing == 1:
			dt = self._retrieve('SELECT Date FROM Dates WHERE Date < ? and Daytype = ? ORDER BY Date DESC LIMIT 1', (self.date, self.daytype))	
			self.calls.append(self.demand * self.est[0])
			self.ref.append(self.average(dt))
		elif self.fixing == 2:
			self.rest.append(self.calls[0] - self.purchase[1])
			self.calls.append(self.demand * self.est[1] + self.rest[0])
			self.ref.append(array([p for p, v in self.market_data[0]]))
			self.mitigate()
		elif self.fixing == 3:
			self.rest.append(self.demand * sum(self.est[:2], 0) - sum(self.purchase[:3], 0))
			self.calls.append(self.demand * self.est[2] + self.rest[1])
			self.ref.append(self.average(self.date, None))
			self.mitigate()
			
		f = self.fixing - 1
		ref = self.ref[f]
		prices = []
		volumes = []
		
		for i in range(24):
			if ref[i] < self.safe[i]: 
				op = linspace(ref[i], self.safe[i], freq)
				bp = linspace(self.min[i], ref[i], freq)
				prices.append(list(op) + list(bp))
				ov = [self.calls[f][i]/freq for x in range(freq)]
				bv = [(self.demand[i] - self.calls[f][i] - sum(self.purchase, 0)[i])/freq for x in range(freq)]
				volumes.append(list(ov) + list(bv))
			elif ref[i] >= self.safe[i]:
				prices.append(list(linspace(self.min[i], self.safe[i], 2*freq)))
				volumes.append([(self.demand[i] - sum(self.purchase, 0)[i]) / (2*freq) for x in range(2*freq)])
		
		orders = []
		for i in range(len(prices)):
			orders.append(zip(prices[i], volumes[i]))

		return orders
	
	def performance(self):
		while self.fixing <= 3:
			query = "SELECT * FROM MarketData WHERE Date = ? AND Type IN (?, ?) ORDER BY Type"
			p, v = self.fixing, self.fixing + 4			
			opt = (self.date, p, v)
			market = (self._retrieve(query, opt))
			f = self.fixing - 1
			self.market_data.append(zip(market[0], market[1]))
			purchase = []
			orders = self.fix()

		
			for i in range(24):
				vol = 0
				
				for j in range(len(orders[i])):
					if orders[i][j][0] >= self.market_data[f][i][0]:
						vol += orders[i][j][1]
				
				if vol > self.market_data[f][i][1]:
					vol = self.market_data[f][i][1]
					
				purchase.append(vol)
			
			self.purchase.append(purchase)
			self.fixing += 1
			
		if self.fixing == 4:
			self.purchase.append(self.demand - sum(self.purchase, 0))
			opt = (self.date, self.daytype, 1)
			bm = self._retrieve('SELECT * FROM MarketData INNER JOIN Dates ON MarketData.Date = Dates.Date WHERE MarketData.Date = ? AND Dates.DayType = ? AND MarketData.Type = 0 LIMIT ?', opt)
			del self.purchase[0]
			md = []
			for i in range(3):
				md.append([p for p, v in nan_to_num(self.market_data[i])])
			md.append(nan_to_num(bm[0]))
			self.cost = (sum(self.purchase * array(md), 0)) / self.demand
			self.avg = self.average(self.date)
			self.res = (self.avg - self.cost) * self.demand
			
			return sum(self.res)

def results(start_date, end_date):
	dt = start_date
	delta = timedelta(days=1)
	time_change = [datetime(2012, 10, 28), datetime(2012, 3, 25), datetime(2011, 10, 30), datetime(2011, 3, 27), datetime(2010, 10, 31), datetime(2010, 3, 28)]
	while dt <= end_date:
		if dt in time_change: 
			dt += delta
		day = RDN(dt.__str__().split()[0])
		yield datetime.date(dt).isoformat(), round(day.performance(), 2)
		dt += delta
		
def backtesting(start_date, end_date):
	start = datetime.strptime(start_date, '%Y-%m-%d')
	end_date = datetime.strptime(end_date, '%Y-%m-%d')
	profit = 0
	for res in results(start, end_date):
		print res[0], res[1]
		profit += res[1]
	return profit


