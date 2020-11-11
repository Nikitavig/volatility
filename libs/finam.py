import datetime
import urllib
import urllib.request
import pandas as pd
import time
import requests
# from fp.fp import FreeProxy



DIR_TEMP_FILE = "data"


def get_df(code, em, timeframe_p, date_start, date_end):
	# print(f"Запрос данных по активу: {code}")

	# Переименование колонок на нормальные
	def rename_df(df):
		res_df = pd.DataFrame()

		res_df["datetime"] = df["datetime"]

		res_df["open"] = df["<OPEN>"]
		res_df["high"] = df["<HIGH>"]
		res_df["low"] = df["<LOW>"]
		res_df["close"] = df["<CLOSE>"]
		res_df["vol"] = df["<VOL>"]

		return res_df

	# **********************************
	# *********** FINAM  ***************
	# **********************************

	# Выгрузка данных с finam и сохранение в файл
	def get_df_from_finam(date_start, date_end, timeframe_p, code="GDAX.BTC-USD", em="484429"):
		df = int(date_start.split(".")[0])
		mf = int(date_start.split(".")[1]) - 1
		yf = int(date_start.split(".")[2])

		dt = int(date_end.split(".")[0])
		mt = int(date_end.split(".")[1]) - 1
		yt = int(date_end.split(".")[2])



		# Минуты
		# url = f"http://export.finam.ru/export9.out?market=520&em={em}&code={code}&apply=0&df={df}&mf={mf}&yf={yf}&from={date_start}&dt={dt}&mt={mt}&yt={yt}&to={date_end}&p=2&f=GDAX.BCH-USD_190504_200504&e=.csv&cn={code}&dtf=4&tmf=3&MSOR=1&mstime=on&mstimever=1&sep=3&sep2=1&datf=1&at=1"

		# Час
		# url = f"http://export.finam.ru/export9.out?market=520&em={em}&code={code}&apply=0&df={df}&mf={mf}&yf={yf}&from={date_start}&dt={dt}&mt={mt}&yt={yt}&to={date_end}&p=7&f=GDAX.BCH-USD_190504_200504&e=.csv&cn={code}&dtf=4&tmf=3&MSOR=1&mstime=on&mstimever=1&sep=3&sep2=1&datf=1&at=1"
		# День
		url = f"http://export.finam.ru/export9.out?market=520&em={em}&code={code}&apply=0&df={df}&mf={mf}&yf={yf}&from={date_start}&dt={dt}&mt={mt}&yt={yt}&to={date_end}&p={timeframe_p}&f={code}&e=.csv&cn={code}&dtf=4&tmf=3&MSOR=1&mstime=on&mstimever=1&sep=3&sep2=1&datf=1&at=1"

		
		while True:
			try:
				# proxy = FreeProxy(rand=True).get().replace("http://", "").replace("https://", "")
				# print(f"proxy: {proxy}")
				headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

				# responce = requests.get(url, headers=headers, proxies={'http': proxy})
				responce = requests.get(url, headers=headers)
				if responce.status_code == 200:
					data = responce.text
					df = pd.DataFrame([x.split(';') for x in data.replace("\r", "").split('\n')])
					df = df[1:-1]

					df.columns = ['ticker', 'per', 'date', 'time', 'open', 'high', 'low', 'close', 'vol']

					return df
				
			except Exception as e:
				print(f"Error: {e}")
				print(f"ticker: {code}")



		
		
	

		



		# with open(f"{DIR_TEMP_FILE}/data.csv" , "w") as file:
		# 	file.write(data.replace("\n", ""))
	# **********************************
	# *********** FINAM END ************
	# **********************************



	# **********************************
	# ******* Пресчет времени **********
	# **********************************



	# Функция для пересчета времени.
	# Для синхронизации времени с TV
	# Нужно доделать ее
	def format_datetime(df, timeframe_p):
		date_ = df["date"].values.tolist()
		time_ = df["time"].values.tolist()

		res_datetime = []

		for i in range(len(date_)):
			date_time_ = f"{date_[i]} {time_[i]}"
			if str(timeframe_p) == "2":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(minutes=1)
			if str(timeframe_p) == "3":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(minutes=5)
			if str(timeframe_p) == "4":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(minutes=10)
			if str(timeframe_p) == "5":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(minutes=15)
			if str(timeframe_p) == "6":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(minutes=30)
			if str(timeframe_p) == "7":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(minutes=60)
			if str(timeframe_p) == "8":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(days=1)
			if str(timeframe_p) == "9":
				date_time_ = datetime.datetime.strptime(date_time_, "%d/%m/%y %H:%M:%S") - datetime.timedelta(weeks=1)
			res_datetime.append(date_time_)

		df["datetime"] = res_datetime


	df = get_df_from_finam(date_start=date_start, date_end=date_end, timeframe_p=timeframe_p, code=code, em=em)


	# df = pd.read_csv(data, sep=";")

	# Фомратирвоание даты
	format_datetime(df, timeframe_p)
	# df = calc_datetime_hours(df)

	# df = rename_df(df)

	# df.to_csv(f"{DIR_TEMP_FILE}/data_format_{code}_{date_start}_{date_end}.csv", sep=";", index=False)
	df = df[['datetime', 'open', 'high', 'low', 'close', 'vol']]
	# print(df.info())
	df["open"] = pd.to_numeric(df["open"])
	df["high"] = pd.to_numeric(df["high"])
	df["low"] = pd.to_numeric(df["low"])
	df["close"] = pd.to_numeric(df["close"])
	df["vol"] = pd.to_numeric(df["vol"])
	

	return df



def main():
	pass



if __name__ == '__main__':
	main()
	
