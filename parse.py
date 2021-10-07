import re
from datetime import datetime
from datetime import timedelta
import base64
import requests
import dataset
import json
import hashlib 
import time
import traceback

db = dataset.connect('sqlite:///../data/db/data.sqlite3')
request_status_datatable = db["request_status_data"]
data_table = db["data"]

parse_row  = request_status_datatable.find_one(parsed=0, status_code=200)



while parse_row:
	print ("inside while loop")
	db.begin()
	try:
		print(str(parse_row))
		json_data = json.loads(parse_row['json_data'])

		data = json_data["data"]
		tabularData = data["tabularData"]
		bodyContent = tabularData["bodyContent"]

		for row in bodyContent:
			insert_row = {}
			insert_row["state"] = parse_row["state"]
			insert_row["city"] = parse_row["city"]
			insert_row["site"] = parse_row["site"]
			insert_row["site_name"] = parse_row["site_name"]
			insert_row["query_name"] = parse_row["query_name"]

			#dateformat : 14-Oct-2017 - 08:00"
			#print str(row)
			if row.__contains__("to date"):
				to_date = row["to date"]
				to_date_array = to_date.split(" - ")
				insert_row["to_date"] = to_date_array[0]
				insert_row["to_time"] = to_date_array[1]

			if row.__contains__("from date"):
				from_date = row["from date"]
				from_date_array = from_date.split(" - ")
				insert_row["from_date"] = from_date_array[0]
				insert_row["from_time"] = from_date_array[1]
				insert_row["year"] = datetime.strptime(to_date_array[0],"%d-%b-%Y").year

			if row.__contains__("PM2.5"):
				pm25 = row["PM2.5"]
				if pm25 and pm25 !="":
					insert_row["pm25"] = pm25

			if row.__contains__("PM10"):
				pm10 = row["PM10"]
				if pm10 and pm10 !="":
					insert_row["pm10"] = pm10

			if row.__contains__("NO"):
				no = row["NO"]
				if no and no !="":
					insert_row["NO"] = no
			if row.__contains__("NO2"):
				no2 = row["NO2"]
				if no2 and no2 !="":
					insert_row["NO2"] = no2

			if row.__contains__("NOx"):
				nox = row["NOx"]
				if nox and nox !="":
					insert_row["NOx"] = nox

			if row.__contains__("NH3"):
				nh3 = row["NH3"]
				if nh3 and nh3 !="":
					insert_row["NH3"] = nh3

			if row.__contains__("SO2"):
				so2 = row["SO2"]
				if so2 and so2 !="":
					insert_row["SO2"] = so2

			if row.__contains__("CO"):
				co = row["CO"]
				if co and co !="":
					insert_row["CO"] = co

			if row.__contains__("Ozone"):
				ozone = row["Ozone"]
				if ozone and ozone !="":
					insert_row["Ozone"] = ozone

			if row.__contains__("Benzene"):
				benzene = row["Benzene"]
				if benzene and benzene != "":
					insert_row["Benzene"] = benzene

			if row.__contains__("Toluene"):
				toluene = row["Toluene"]
				if toluene and toluene != "":
					insert_row["Toluene"] = toluene

			if row.__contains__("P-Xylene"):
				pxylene = row["P-Xylene"]
				if pxylene and pxylene != "":
					insert_row["PXylene"] = pxylene

			if row.__contains__("MP-Xylene"):
				mpxylene = row["MP-Xylene"]
				if mpxylene and mpxylene != "":
					insert_row["mpXylene"] = mpxylene

			if row.__contains__("Eth-Benzene"):
				ethbenzene = row["Eth-Benzene"]
				if ethbenzene and ethbenzene != "":
					insert_row["EthBenzene"] = ethbenzene

			if row.__contains__("Xylene"):
				xylene = row["Xylene"]
				if xylene and xylene != "":
					insert_row["Xylene"] = xylene


			if row.__contains__("O Xylene"):
				oxylene = row["O Xylene"]
				if oxylene and oxylene != "":
					insert_row["OXylene"] = oxylene


			if row.__contains__("Temp"):
				temp = row["Temp"]
				if temp and temp != "":
					insert_row["Temp"] = temp

			if row.__contains__("RH"):
				rh = row["RH"]
				if rh and rh != "":
					insert_row["RH"] = rh

			if row.__contains__("WS"):
				ws = row["WS"]
				if ws and ws != "":
					insert_row["WS"] = ws

			if row.__contains__("WD"):
				wd = row["WD"]
				if wd and wd != "":
					insert_row["WD"] = wd

			if row.__contains__("SR"):
				sr = row["SR"]
				if sr and sr != "":
					insert_row["SR"] = sr

			if row.__contains__("BP"):
				bp = row["BP"]
				if bp and bp != "":
					insert_row["BP"] = bp

			if row.__contains__("CO2"):
				co2 = row["CO2"]
				if co2 and co2 != "":
					insert_row["CO2"] = co2

			if row.__contains__("CH4"):
				ch4 = row["CH4"]
				if ch4 and ch4 != "":
					insert_row["CH4"] = ch4

			if row.__contains__("AT"):
				at = row["AT"]
				if at and at != "":
					insert_row["AT"] = at

			if row.__contains__("Black Carbon"):
				blackcarbon = row["Black Carbon"]
				if blackcarbon and blackcarbon != "":
					insert_row["BlackCarbon"] = blackcarbon

			if row.__contains__("Rack Temp"):
				racktemp = row["Rack Temp"]
				if racktemp and racktemp != "":
					insert_row["RackTemp"] = racktemp

			if row.__contains__("Gust"):
				gust = row["Gust"]
				if gust and gust != "":
					insert_row["Gust"] = gust

			if row.__contains__("Variance"):
				variance = row["Variance"]
				if variance and variance != "":
					insert_row["Variance"] = variance

			if row.__contains__("PP Accum"):
				ppaccum = row["PP Accum"]
				if ppaccum and ppaccum != "":
					insert_row["PPAccum"] = ppaccum

			if row.__contains__("VWS"):
				vws = row["VWS"]
				if vws and vws != "":
					insert_row["VWS"] = vws

			if row.__contains__("Power"):
				power = row["Power"]
				if power and power != "":
					insert_row["Power"] = power


			if row.__contains__("RF"):
				rf = row["RF"]
				if rf and rf != "":
					insert_row["RF"] = rf

			if 1 <= datetime.strptime(to_date_array[0],"%d-%b-%Y").month <= 2:
				insert_row["season"] = 1

			if 3 <= datetime.strptime(to_date_array[0],"%d-%b-%Y").month <= 5:
				insert_row["season"] = 2

			if 6 <= datetime.strptime(to_date_array[0],"%d-%b-%Y").month <= 9:
				insert_row["season"] = 3

			if 10 <= datetime.strptime(to_date_array[0],"%d-%b-%Y").month <= 12:
				insert_row["season"] = 4



			print (str(insert_row))
			data_table.insert(insert_row)
			#parsed
			parse_row["parsed"]=1

	except Exception:
		traceback.print_exc()
		#error in parsing
		parse_row["parsed"]=2

	#update row to parsed
	request_status_datatable.update(parse_row, ['id'])
	db.commit()

	parse_row  = request_status_datatable.find_one(parsed=0, status_code=200)
	if parse_row:
		pass
	else:
		break
