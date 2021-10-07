import matplotlib.pyplot as plt
import pandas as pd
import csv
import os

# coastal need to do

pd.set_option("display.max_columns", 50)
file2017181920 = pd.read_csv("D:\\articles work\\coast land and hill article\\datafrom2017to2021for531sites.csv")

print(file2017181920.columns)
###------#######

file2017181920two = file2017181920.rename(columns={'year':'years'})



#------converting column format into date-------
file2017181920['from_date'] = pd.to_datetime(file2017181920['from_date'])
file2017181920['to_date'] = pd.to_datetime(file2017181920['to_date'])
file2017181920_dataframe = pd.DataFrame(file2017181920)

#print(file2017181920.iloc[0])
list_of_sites = ['site_260','site_5105','site_5272','site_5276','site_5270','site_5334','site_5271',
                 'site-252','site_5331','site_293','site_5365','site_5364','site_5361',
                 'site_5363','site_5367','site_300','site_5120','site_5106','site_5071','site_5446',
                 'site_5417','site_5073','site_5424','site_5131','site_5346','site_5375','site_118',
                 'site_1423','site_5395','site_1427','site_1432','site_5047','site_5056','site_5039',
                 'site_5041','site_5247','site_5275','site_5269','site_1549','site_1437','site_199',
                 'site_294','site_5464','site_5465','site_5083','site_1541','site_5023','site_5474',
                 'site_297','site_5258','site_5475']

list_of_left_out_coastal_cities = ['site_288'] #### site name contains "." full stop need to remove site name from naming figures

list_of_coastal_sites = ['site_260','site_5105','site_5272','site_5276','site_5270','site_5334','site_5271',
                         'site_252','site_5331','site_293','site_5365','site_5364','site_5361',
                         'site_5363','site_5367','site_300','site_5120','site_5106','site_5071']

#'site_260','site_5105','site_5272','site_5276','site_5270','site_5334','site_5271','site_252','site_5331','site_293',


list_of_hill_sites = ['site_5446','site_5417','site_5073','site_5424','site_5131','site_5346','site_5375']


list_of_plain_sites = ['site_5269', 'site_1549', 'site_1437', 'site_199', 'site_294',
                       'site_5464', 'site_5465','site_5083', 'site_1541', 'site_5023',
                       'site_5474','site_297','site_5258','site_5475']

list_of_plain_sites1= ['site_118','site_1423','site_5395','site_1427','site_1432',
                       'site_5056','site_5039','site_5041','site_5247']


list_of_left_out_plain_sites =['site_5047','site_5275']#### site name contains "." full stop need to remove site name from naming figures








list_of_columns= ['pm25','pm10','no','no2','nox','nh3','so2','co','ozone',
                  'benzene','toluene','pxylene','rh','ws','wd','sr','bp',
                  'at','season','year','xylene','VWS','racktemp','rf','ppaccum',
                  'ch4','co2','gust','variance','temp','mpxylene','ethbenzene',
                  'power','blackcarbon','oxylene']




visakhapatnam = file2017181920[(file2017181920.year >= 2017) & (file2017181920.site == 'site_5483')]

# visakhapatnam['from_date'] = pd.to_datetime(visakhapatnam['from_date'])
#print(year_classification.iloc[0])

visakhapatnam_dataframe = pd.DataFrame(visakhapatnam)

######-----information about dataframe by df.info()-----
# visakhapatnam_dataframe.info()
#####------- plotting---------
# visakhapatnam_dataframe.plot(kind = 'line',
#         x = 'from_date',
#         y =['year']
#         )
# plt.show()







#####------- adding absolute path ---------

my_path= os.path.abspath("E:\\figures\\plain states1\\plain_")   ### last coastal or hill or plain will add as prefix for thefigure title


#####------- loop plotting---------
for j in list_of_plain_sites1:
    for i in list_of_columns:
        lucknow = file2017181920[(file2017181920.year >= 2017) & (file2017181920.site == j)]
        lucknow_dataframe = pd.DataFrame(lucknow)
        k = file2017181920[["site_name"]]
        ######----- to get corrending first column value at "site_name" w.r.t to j key in site column
        l = lucknow_dataframe.loc[lucknow_dataframe['site'] == j, 'site_name'].iloc[0]

        lucknow_dataframe.plot(kind='line',
                               x='from_date',
                               y=[i]
                               )
        plt.title(l + ', parameter: ' + i.upper())
        plt.xlabel('TIME')
        plt.ylabel(i.upper())
        plt.savefig(my_path + " parameter " + i.upper() + "_"+ l + "_" + j) #### save figures figures
        plt.close('all') # close all figures opened and reduce chance of bitmap error
        print(l)

        # print(lucknow_dataframe)

        # plt.show()







# print(visakhapatnam_dataframe.iloc[-5:])
# print(list_of_sites.info())

