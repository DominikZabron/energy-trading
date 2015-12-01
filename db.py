import sqlite3, calendar, datetime

years = [2010, 2012]
	
timezone_change = [
	[2012, 10, 28],
	[2012, 3, 25],
	[2011, 10, 30],
	[2011, 3, 27],
	[2010, 10, 31],
	[2010, 3, 28]]

moving_holidays = [
	[2012, 1, 6],
	[2012, 4, 8],
	[2012, 4, 9],
	[2012, 5, 27],
	[2012, 6, 7],
	[2011, 1, 6],
	[2011, 4, 4],
	[2011, 4, 25],
	[2011, 6, 12],
	[2011, 6, 23],
	[2010, 4, 4],
	[2010, 4, 5],
	[2010, 5, 23],
	[2010, 6, 3]]
		
fixed_holidays = [
	[1, 1],
	[5, 1],
	[5, 3],
	[8, 15],
	[11, 1],
	[11, 11],
	[12, 25],
	[12, 26]]

day_types = [
	'working day',
	'saturday',
	'sunday/holiday']

data_types = [
	'BalMPrice',
	'Fix1Price',
	'Fix2Price',
	'Fix3Price',
	'BalMVol',
	'Fix1Vol',
	'Fix2Vol',
	'Fix3Vol']
	
mdata = []
cal = calendar.Calendar()
holidays = []
dates = []
years[1] += 1
excluded = []
wrk = range(5)

daytp = zip(range(3), day_types)
datatp = zip(range(8), data_types)

for item in moving_holidays:
	holidays.append(datetime.date(item[0], item[1], item[2]).isoformat())
	
for year in range(2001, years[1]):
	for item in fixed_holidays:
		holidays.append(datetime.date(year, item[0], item[1]).isoformat())
		
for item in timezone_change:
	excluded.append(datetime.date(item[0], item[1], item[2]).isoformat())

	
for year in range(2001, years[1]):
	for month in range(1, 13):
		for day in cal.itermonthdays2(year, month):
			if day[0] != 0:
				line = [datetime.date(year, month, day[0]).isoformat()]
				
				if day[1] == 6 or line[0] in holidays:
					line.append(2)
				elif day[1] in wrk:
					line.append(0)
				elif day[1] == 5:
					line.append(1)
				
				if line[0] not in excluded:
					dates.append(line)
	

with open('RDN_20121011_1707.CSV') as file:
	file.readline()
	file.readline()
	file.readline()
	file.readline()
	
	for line in file:
		l = line.replace('\n', '').split(';')
		
		if l[3] == '' or l[4] != '':
			continue
			
		for i in xrange(2,77):
			try:
				l[i] = float(l[i])
			except ValueError:
				l[i] = None

		day = l[0].split()[0]
		
		if l[1] == 'Wolumen':
			fixtype = [5, 6, 7]
		elif l[1] == 'Cena':
			fixtype = [1, 2, 3]
			
		fixing = [l[2:27], l[27:52], l[52:77]]
		
		for i in range(len(fixing)):
			del fixing[i][2]
			fixing[i].insert(0, day)
			fixing[i].append(fixtype[i])
			mdata.append(fixing[i])

			
with open('daneRB.csv') as f:
	f.readline()
	
	for line in f:		
		l = line.replace('\n', '').split(',')
		
		try: p
		except NameError:
			p = [l[0]]
			v = [l[0]]
			
		if l[0] != p[0] and len(p) == 25:
			p.append(0)
			v.append(4)
			mdata.append(p)
			mdata.append(v)
			p = [l[0]]
			v = [l[0]]
		elif l[0] != p[0] and len(p) != 25:
			p = [l[0]]
			v = [l[0]]
		else:
			pass
				
		p.append(float(l[2]))
		v.append(int(l[5]))



connection = sqlite3.connect('Energy.db')

with connection:
	
	current = connection.cursor()
	
	current.execute("PRAGMA foreign_keys = ON")
	
	current.execute("DROP TABLE IF EXISTS MarketData")
	current.execute("DROP TABLE IF EXISTS Dates")
	current.execute("DROP TABLE IF EXISTS DayTypes")
	current.execute("DROP TABLE IF EXISTS DataTypes")
	
	current.execute("CREATE TABLE DayTypes(Id INTEGER PRIMARY KEY, Desc TEXT)")
	current.executemany("INSERT INTO DayTypes VALUES(?, ?)", daytp)
	
	current.execute("CREATE TABLE DataTypes(Id INTEGER PRIMARY KEY, Desc TEXT)")
	current.executemany("INSERT INTO DataTypes VALUES(?, ?)", datatp)

	current.execute("""CREATE TABLE Dates(Date TEXT PRIMARY KEY, DayType INT, 
	FOREIGN KEY(DayType) REFERENCES DayTypes(Id))""")
	current.executemany("INSERT INTO Dates VALUES(?, ?)", dates)	
	
	current.execute("""CREATE TABLE MarketData(Date TEXT, One REAL, Two REAL, 
	Three REAL, Four REAL, Five REAL, Six REAL, Seven REAL, Eight REAL, Nine REAL, Ten REAL, Eleven REAL, 
	Twelve REAL, Thirteen REAL, Fourteen REAL, Fifteen REAL, Sixteen REAL, Seventeen REAL, Eighteen REAL, 
	Nineteen REAL, Twenty REAL, TwentyOne REAL, TwentyTwo REAL, TwentyThree REAL, TwentyFour REAL,
	Type TEXT, FOREIGN KEY(Date) REFERENCES Dates(Date), FOREIGN KEY(Type) REFERENCES DataTypes(Id))""")
	current.executemany("""INSERT INTO MarketData VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
	?, ?, ?, ?, ?, ?, ?, ?)""", mdata)
	

	
