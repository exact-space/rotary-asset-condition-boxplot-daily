import os,platform
# current_dir = os.getcwd()
# target_dir = '/Users/nijin/exsp'
# os.chdir(target_dir)
import app_config.app_config as cfg
config = cfg.getconfig()
import timeseries.timeseries as ts
qr = ts.timeseriesquery()
meta = ts.timeseriesmeta()
# os.chdir(current_dir)
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
# import seaborn as sns
import datetime as datetime
import calendar
from datetime import timedelta
import json, requests
from pprint import pprint
import time
from functools import reduce
from flask import Flask,jsonify,request
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings("ignore")
# from memory_profiler import profile
# from apscheduler.schedulers.blocking import BlockingScheduler
from logzero import logger

version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()

# base_url='https://data.exactspace.co/exactapi'
# config["api"]["meta"]=base_url
base_url = config["api"]["meta"]
# unitsId = os.environ.get("UNIT_ID") if os.environ.get("UNIT_ID")!=None else None
# # unitsId="628dd242c78e4c5d0f3b90cf"
# if unitsId==None:
#     print("no unit id passed")
#     exit()

# config = cfg.getconfig()[unitsId]
# print(config)

##################################get unitsId ###############################################################

def getUnitsId(base_url):
    url = base_url+ '/units?filter={"where":{"name":{"nlike":"test","options":"i"},"stackDeploy":true},"fields":["id"]}'
    resp = requests.get(url)
    jjson = json.loads(resp.content)
    unitsId = [i['id'] for i in jjson]
    return unitsId

unitsId=getUnitsId(base_url)
# sufix = unitsId_+"boxplot"
# metric_list = [prefix + str(ids) for ids in unitIds]
# print(metric_list)
########################################Fetching data#####################################################################################


def getData1(taglist,timeType,qr, key = None,unitId = None, aggregators = [{"name":"avg","sampling_value":1,"sampling_unit":"minutes"}]):

    qr.addMetrics(taglist)
    if(timeType["type"]=="date"):
        qr.chooseTimeType("date",{"start_absolute":timeType["start"], "end_absolute":timeType["end"]})

    elif(timeType["type"]=="relative"):
        qr.chooseTimeType("relative",{"start_unit":timeType["start"], "start_value":timeType["end"]})


    elif(timeType["type"]=="absolute"):
        qr.chooseTimeType("absolute",{"start_absolute":timeType["start"], "end_absolute":timeType["end"]})


    else:
        logger.info('Error')
        logger.info('Improper timetype[type]')

    if aggregators != None:
        qr.addAggregators(aggregators)

    if ((key) and (key == "simulation")):
        qr.submitQuery("simulation",unitId)
    else:
        key = None
        qr.submitQuery(key,unitId)


    qr.formatResultAsDF()
    try:

        df = qr.resultset["results"][0]["data"]
        return df
    except Exception as e:
        print('Data Not Found getData1 ', e)
        return pd.DataFrame()
    
    
    
    
###########################fetch all tags################################################################



def getallTags(unitsId,base_url):
    url=base_url +'/tagmeta?filter={"where":{"unitsId":"'+str(unitsId)+'"},"fields":["dataTagId"]}'
    # {"where":{"equipmentId":{"like":"63d2280bcf24c00007438ab9"},"unitsId":"61c1818371c20d4a206a2e35"}}
    res =requests.get(url)

    if res.status_code == 200:
        tags = json.loads(res.content)
#         print(tags)
        datatag=[]
        for i in tags:
#             print(i["dataTagId"])
            datatag.append(i["dataTagId"])
            

    print(len(set(datatag)))
    return list(set(datatag))



##########################fetch eqid#########################################################################
def fetchtagmeta(unitsId,tag,base_url):
    url = base_url +'/tagmeta?filter={"where": {"unitsId":"'+str(unitsId)+'","dataTagId":"'+str(tag)+'"},"fields":["equipmentId"]}'
    response = requests.get(url)
    #     print("response " ,response)
    # tagmeta = json.loads(response.content)
    # print(tagmeta)
    # return tagmeta[0]["equipmentId"]
    # #     return tagmeta
    if response.status_code == 200:
        # if tagmeta:
    #     print("response " ,response)
        tagmeta = json.loads(response.content)
        if tagmeta:
            if "equipmentId" in tagmeta[0]:

    #     if res.status_code == 200
            # print(tagmeta)
                return tagmeta[0]["equipmentId"]
            else:
                print("no equipmentId")
                pass
        #     return tagmeta
    else:
        return []
        pass
    
###############################box pl0t calculation###################################
def boxplot(df,tag,unitId,year):
    print("**********df*****************",df)
    if not df.empty:
        if tag in df.columns:
            df[tag] = pd.to_numeric(df[tag], errors='coerce')

#     Drop rows with NaN values (which were non-numeric)
            df = df.dropna(subset=[tag])
            print(tag)
            df=df[df[tag]<99999.0]
            if not df.empty:
                
                print("dataframe",df.head())
        #         df['year'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M').dt.year

            # Store the year in a variable (assuming all entries are from the same year)
        #         year = df['year'].iloc[0]

                print(df.head())
                lst=[]
                #     try:
                Min=round(float(df[tag].min()),2)
                q1 = round(float(np.quantile(df[tag], 0.25)),2)
                Med=round(float(df[tag].median()),2)
                q3=round(float(np.quantile(df[tag], 0.75)),2)
                Max=round(float(df[tag].max()),2)

                lst.append( {
                    "name":unitId+"_boxplot",
                    "datapoints":[[0,Min]],
        #             "timestamp":0,
        #             "value":Min,
                    "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"Min"}
                    })
                lst.append( {
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,q1]],
        #             "timestamp":0,
        #             "value":q1,
                    "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"q1"}
                    })
                lst.append({
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,Med]],
        #             "timestamp":0,
        #             "value":med ,
                     "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"Med"}
                    })
                lst.append({
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,q3]],
    #                 "timestamp":0,
    #                 "value":q3,
                     "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"q3"}
                    })
                lst.append({
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,Max]],
        #             "timestamp":0,
        #             "value":Max,
                     "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"Max"}

                    })
            else:    
                print("No Numeric Data")
            
                lst=[]

        #         postscylla(lst)
        else:
            print("No Tag ")
            
            lst=[]


    else:
        print("No Data")
        
        lst=[]




    #     lst=[Min,q1,med,q3,Max]
        #     except :
        #         lst=[0,0,0,0,0]
    return lst
##################################################fetchlimits###################################################################################

def fetchlimits(unitsId,tag,base_url):
    url = base_url +'/tagmeta?filter={"where": {"unitsId":"'+str(unitsId)+'","dataTagId":"'+tag+'"},"fields":["unitsId","dataTagId","equipmentId", "limRangeHi","limRangeLo","benchmark","benchmarkLoad"]}'
    response = requests.get(url)
    #     print("response " ,response)
    dct={}
    tagmeta = json.loads(response.content)
#     for i in btagmeta[0]['benchmarkLoad']
#     bk=pd.DataFrame(tagmeta[0]['benchmarkLoad'])
#     print(tagmeta[0]['benchmarkLoad'])
    loadbkt=[int(x) for x in tagmeta[0]['benchmarkLoad'].keys() if x not in["status",'end','start','lastRunHistory','lastRun'] and  tagmeta[0]['benchmarkLoad'][x]['status'] =="valid"]
    loadbkt.sort()
    try:
        q95_values = [tagmeta[0]['benchmarkLoad'][str(key)]['q95'] for key in  loadbkt]
        q005_values=[tagmeta[0]['benchmarkLoad'][str(key)]['q005'] for key in  loadbkt]
    #     print( q95_values)
    #     print(q005_values)
        min_q005 = min(q005_values)
        max_q95 = max(q95_values)
        # upperVlaue=max_q95
    #     print(min_q005,max_q95)
        rollingSD=round(tagmeta[0]["benchmark"]["rollingSd"],2)
    #     print(rollingSD)
       
        if "zeroLimit" in tagmeta[0]["benchmark"].keys():
            dct["zeroLimit"]=tagmeta[0]["benchmark"]["zeroLimit"]
        else:
            dct["zeroLimit"]="-"
       
            
        if "rollingSd" in tagmeta[0]["benchmark"].keys():
            upperValue=max_q95+ 50*rollingSD
            lowerValue=min_q005-50*rollingSD
            dct["upperValue"]=upperValue
            dct["lowerValue"]=lowerValue
            
        else:
            dct["upperValue"]="-"
            dct["lowerValue"]="-"
            
            
        if 'limRangeHi' in tagmeta[0]["benchmark"].keys():
            dct['limRangeHi']=tagmeta[0]['limRangeHi']
            dct['limRangeLo'] = tagmeta[0]['limRangeLo']
            
            
            
        else:
            dct['limRangeHi']="-"
            dct['limRangeLo'] = "-"
    except:
        print("No Tagmeta")
        pass
    print(dct)
    return dct



#
##################################Removing Outliers###################################################################
def removingOutliers(df,statetag,validload,unitsId,tag,base_url):
  
    try:
        df = df[(df["statetag"] == 1) & (df["validload"] == 1)]
    except:
        print("No stateTag validLoad")
      
    try:     
        lim=fetchlimits(unitsId,tag,base_url)

        limRangeHi=lim["limRangeHi"]
        limRangeLo=lim["limRangeLo"]
        zeroLimit=lim["zeroLimit"]
        upperValue=lim['upperValue']
        lowerValue=lim['lowerValue']

        if zeroLimit == "positive":
            df = df[df[tag] >= 0]
        elif  zeroLimit == "negative":
                df = df[df[tag]<= 0]

        else:
            pass

        if (limRangeHi=="-") and (limRangeLo=="-"):
            pass

        elif (limRangeHi=="-") or (limRangeLo=="-"):
            
            pass
        
        elif (limRangeHi== None) and (limRangeLo==None):
            pass
        
        elif (limRangeHi== None) or (limRangeLo==None):
            pass
        else:
            df=df[df[tag]<=limRangeHi]
            df=df[df[tag]>=limRangeLo]



        if (upperValue =="-") and (lowerValue=="-"):
            pass

        elif (upperValue =="-") or (lowerValue=="-"):
            pass

        else:


            df==df[df[tag]<=float(upperValue)]
            df==df[df[tag]>=float(lowerValue)]   
        #     except: 
    except:
        df=df
        
    return df     


#########################boxplot yrs############################################################################

def to_seconds(date):
    ftime = calendar.timegm(date.timetuple())
    return ftime

# @profile
def boxplot_yrs(unitsId, tag, base_url, eqid):
    validload = 'validload__' + tag

    # Determine taglist based on the presence of eqid
    if eqid:
        statetag = 'state__' + eqid
        taglist = [statetag, tag, validload]
    else:
        taglist = [tag, validload]

    # Get the current year and iterate over the past 5 full years, excluding the current year
    start_year = 2018
    end_year = datetime.datetime.now().year - 1  # Exclude the current year

    for year in range(start_year, end_year + 1):
        start_time = datetime.datetime(year, 1, 1).strftime("%d-%m-%Y %H:%M")
        end_time = datetime.datetime(year, 12, 31, 23, 59, 59).strftime("%d-%m-%Y %H:%M")

        print(f"Fetching data from {start_time} to {end_time}")
        
        # Fetch data for the entire year
        # df = getData1(taglist, {"type": 'date', "start": start_time, "end": end_time}, qr, key=None, unitId=None, aggregators=[{"name": "avg", "sampling_value": 1, "sampling_unit": "hours"}])
        count = 0
        while count < 5:
   
            try:
                df = getData1(taglist, {"type": 'date', "start": start_time, "end": end_time}, qr, key=None, unitId=None, aggregators=[{"name": "avg", "sampling_value": 1, "sampling_unit": "hours"}])
            except:
                count+=1
                time.sleep(5)
        if not df.empty:
            df.dropna(axis=1, how='all', inplace=True)

            # Replace Infinity and -Infinity with NaN and then drop them
            df.replace([float('inf'), float('-inf')], pd.NA, inplace=True)
            df.dropna(inplace=True)
            
            
            df["time"] = pd.to_datetime(df['time'] / 1000 + 3 * 60 * 60, unit='s')
            df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M')
            df['time'] = pd.to_datetime(df['time'])
            print("&&&&&&&&&&&&&&&&",df.head(),df.tail())
            df["year"] = df['time'].dt.year
            
            print(f"Processing data for the year: {year}")
            if "statetag__" in df.columns:

                bplot = removingOutliers(df, statetag, validload, unitsId, tag, base_url)
                bplot = boxplot(bplot, tag, unitsId, year)
            else:
                statetag=0
                bplot = removingOutliers(df, statetag, validload, unitsId, tag, base_url)
                bplot = boxplot(bplot, tag, unitsId, year)
            print(bplot)
            postscylla(bplot)
        else:
            print(f"No data available for the year: {year}")










##################################box plot for 1 yr ,1 month, 7 days ############################################
# @profile
def boxplot_oneyrs(unitsId,tag,base_url,eqid):
    # dct={}
    validload='validload__'+tag
    eqid=fetchtagmeta(unitsId,tag,base_url)
    
    if (eqid !=[]) & (eqid !=None):
        
        statetag='state__'+eqid
        taglist= [statetag,tag, validload]
        
    else:
        
        
        taglist= [tag,validload]
        
#    
    # Format datetime object to desired string format
    endtime = datetime.datetime.now()
   
    start_time="01-01-"+str(endtime.year)+ " 00:00"
    
#     s7d =endtime-datetime.timedelta(days=7)
#     sd1month=endtime-datetime.timedelta(days=30)

    endtime=endtime.strftime("%d-%m-%Y %H:%M")
    

    while count<5:
        try:
            df1yr=getData1(taglist,{"type":'date',"start":str(start_time),"end":str(endtime)},qr,key = None,unitId = None,aggregators = [{"name":"avg","sampling_value":5,"sampling_unit":"minutes"}])

        except:
            count+=1
            time.sleep(5)
    # df1yr=getData1(taglist,{"type":'date',"start":str(start_time),"end":str(endtime)},qr,key = None,unitId = None,aggregators = [{"name":"avg","sampling_value":5,"sampling_unit":"minutes"}])
    
    if not df1yr.empty:
        df1yr.dropna(inplace=True)
        df1yr["time"]=pd.to_datetime(df1yr['time']/1000+5.5*60*60, unit='s')
        df1yr['time'] =df1yr['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df1yr['time'] = pd.to_datetime(df1yr['time'])
        if "state__" in df1yr.columns:

            bplot1yr=removingOutliers(df1yr,statetag,validload,unitsId,tag,base_url)
            bplot1yr=boxplot(bplot1yr,tag,unitsId,"1Y")
        else:
            statetag=0
            bplot1yr=removingOutliers(df1yr,statetag,validload,unitsId,tag,base_url)
            bplot1yr=boxplot(bplot1yr,tag,unitsId,"1Y")
            print("no statetag")
            
        # print("bplot1yr",bplot1yr)

        if bplot1yr!=[]:

            postscylla(bplot1yr)
        else:
            pass
    
    else:
        print("No Data for 1 year")
        pass

#####################################one month, seven days ###########################################

# @profile
def boxplot_onemonth_sevendays(unitsId,tag,base_url,eqid):
#     eqid=fetchtagmeta(unitsId,tag,base_url)
#     statetag='state__'+eqid
#     validload='validload__'+tag
#     taglist= [statetag,tag, validload]
    print(tag)
    validload='validload__'+tag
#     eqid=fetchtagmeta(unitsId,tag,base_url)
    
    if (eqid !=[]) & (eqid !=None):
        
        statetag='state__'+eqid
        taglist= [statetag,tag, validload]
    else:
        
        
        taglist= [tag,validload]
    
#    
    # Format datetime object to desired string format
    endtime = datetime.datetime.now()
   

    
    s7d =endtime-datetime.timedelta(days=7)
    print("s7d",s7d)
    sd1month=endtime-datetime.timedelta(days=30)

    endtime=endtime.strftime("%d-%m-%Y %H:%M")
    
    sd1month=sd1month.strftime("%d-%m-%Y %H:%M")
    s7d=s7d.strftime("%d-%m-%Y %H:%M")
    

    print("endtime",endtime)
    print("sd1month",sd1month)
    print("s7d",s7d)
   
    count=0
    while count<5:
        try:
           df1M=getData1(taglist,{"type":'date',"start":str(sd1month),"end":str(endtime)},qr,key = None,unitId = None,aggregators = [{"name":"avg","sampling_value":1,"sampling_unit":"minutes"}])
        except:
            count+=1
            time.sleep(5)
   
    # df1M=getData1(taglist,{"type":'date',"start":str(sd1month),"end":str(endtime)},qr,key = None,unitId = None,aggregators = [{"name":"avg","sampling_value":1,"sampling_unit":"minutes"}])
    if not df1M.empty:
        df1M.dropna(inplace=True)
        df1M["time"]=pd.to_datetime(df1M['time']/1000+5.5*60*60, unit='s')
        df1M['time'] =df1M['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df1M['time'] = pd.to_datetime(df1M['time'])
        if "state__" in df1M.columns:
        
            df7d=df1M[(df1M["time"]>=s7d) & (df1M["time"]<=endtime)]

            

            bplot7d=removingOutliers(df7d, statetag,validload,unitsId,tag,base_url)
            bplot7d=boxplot(bplot7d,tag,unitsId,"7d")
        
            bplot1M=removingOutliers(df1M, statetag,validload,unitsId,tag,base_url)
            bplot1M=boxplot(bplot1M,tag,unitsId,"1M")
        else:
            statetag=0

            df7d=df1M[(df1M["time"]>=s7d) & (df1M["time"]<=endtime)]

            

            bplot7d=removingOutliers(df7d, statetag,validload,unitsId,tag,base_url)
            bplot7d=boxplot(bplot7d,tag,unitsId,"7d")
        
            bplot1M=removingOutliers(df1M, statetag,validload,unitsId,tag,base_url)
            bplot1M=boxplot(bplot1M,tag,unitsId,"1M")

        if bplot1M!=[]:

            postscylla(bplot1M)
        else:
            pass
        if bplot7d!=[]:

            postscylla(bplot7d)
        else:
            pass

    else:
        print("No Data for 1 month and Seven Days ")
        pass

##################################fetching from kairos############################################




def fetch_boxplot(unitsId,tag): 
    url=config["api"]["query"]
    # url="https://data.exactspace.co/api/v1/datapoints/query"
    dd={
      "metrics": [
        {
          "tags": {
#             "period": [
#               period
#             ],
            "dataTagId": [
              tag
            ]
          },
          "name": unitsId+"_boxplot",
          "group_by": [
            {
              "name": "tag",
              "tags": [
                "period",
                "calculationType",
                "dataTagId"
              ]
            }
          ]
        }
      ],
      "plugins": [],
      "cache_time": 0,
        
      "start_absolute": 0,
      "end_absolute": 0 
#       "start_relative": {
#         "value": "100",
#         "unit": "years"
#       }
    }
    print(url)
    print(dd)
    try:
        res = requests.post(url=url, json=dd)
        print(res)
        if res.status_code == 200:
            resultset = json.loads(res.content)
        #     if resultset:
                
        #         return resultset 
            return resultset["queries"][0]["results"][0]["tags"]
            
    except:
        pass





#######################################################################################################
def boxplot_main_fun(unitsId,base_url):
    print("Running main function")
    for unit in unitsId:
        tagList=getallTags(unit,base_url)
#     tagList=['LPG_3LAV20CY101_XQ07.OUT']
    
#     tg=get_boxplot(unitsId,tag)
        for tag in tagList:
        
            res=fetch_boxplot(unit,tag)
    #         print(res)
            eqid=fetchtagmeta(unit,tag,base_url)
            if res=={}:
            
            
                boxplot_yrs(unit,tag,base_url,eqid) 
                print("**********************end of years function*********************")
                boxplot_oneyrs(unit,tag,base_url,eqid)
                print("****************end of one year**********************************")
                boxplot_onemonth_sevendays(unit,tag,base_url,eqid)
                print("**************end of seven days**************************************")
            
            
            else:
                
                boxplot_onemonth_sevendays(unit,tag,base_url,eqid)
                print("**************end of one month seven days**************************************")
            
                
            print("done posting for unitsId :",unit)
   
    time.sleep(5)
    print("Main function execution complete")


    

    
###############################posting to kairos###################################################    
    

def postscylla(body):
    print(body)
    url = config["api"]["datapoints"]
    # url="http://data.exactspace.co/kairos/api/v1/datapoints"   #pointing to scylla
    # url="https://data.exactspace.co/exactdata/api/v1/datapoints"
    
    res = requests.post(url = url,json = body)
    print(res,body)
    return res 


boxplot_main_fun(unitsId,base_url)

