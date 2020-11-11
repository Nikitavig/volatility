import libs.finam as finam
import pandas as pd
import datetime




def volat(ticker, em, date_start=None, date_end=None):
	"""
		Функция для рассчета волатильности актива
		Args:
			ticker: типекр актива
			em: код с финам
			date_start: начало периода для рассчета волатильности
			date_end: конец периода для рассчета волатильности	
	"""
	if not date_start:
		date_start = (datetime.datetime.today() - datetime.timedelta(days=365*2)).strftime("%d.%m.%Y")
		date_end = datetime.datetime.today().strftime("%d.%m.%Y")

	timeframe="8"

	# Получаени списка тикеров и em по активам 
	df = finam.get_df(code=ticker, em=em, timeframe_p=timeframe, date_start=date_start, date_end=date_end)
	df["volatility"] = (df["high"] - df["low"]) / df["low"] *  100
	volatility_list = df["volatility"].values.tolist()
	volatility = sum(volatility_list) / len(volatility_list)

	print(f"{ticker}: {round(volatility, 2)}")

	return volatility

def main():

	# Входные параметры
	filename = input(f"Введите название файла: ")
	date_start = input(f"Введите date_start: ")
	date_end = input(f"Введите   date_end: ")

	# Загружаем необходимый файл
	df = pd.pandas.read_excel(filename, index_col="Акции №")

	# Расчет волатильности
	volatility_list = []
	for index, row in df.iterrows():
		ticker = row['code']
		em = row['em']
		volatility_list.append(volat(ticker, em, date_start, date_end))

	# Сохранение результат в файл
	df['volat'] = volatility_list
	df.to_excel(f"{filename.split('.')[0]}_volat.xlsx")



	



if __name__ == '__main__':
	main()
				

		


	
