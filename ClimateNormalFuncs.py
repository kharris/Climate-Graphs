import matplotlib.colors as mplc, numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sbn, sqlite3 as sqlite
from datetime import datetime as dtm
from pathlib import Path
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


def ReadClimateDB(staid,yr,fields=None):
    engine = create_engine(fr"")
    conn = engine.connect()

    select_fields = r'*' if not fields else ', '.join(fields)
    sql = fr"SELECT {select_fields} FROM climate.noaa_hlytemp_{yr} WHERE ghcn_id = '{staid}'"
    try:
        df = pd.read_sql_query(text(sql),conn)
        df['date'] = pd.to_datetime(df['date'],format=r'%Y-%m-%d %H:%M:%S')
        df.set_index('date',inplace=True)
        st_name = conn.execute(text(fr"select name from climate.noaa_stationlocs where ghcn_id = '{staid}'")).fetchall()[0][0]
    except:
        print('Whoops, something broke. Returning empty dataframe.')
        df = pd.DataFrame()
        st_name = 'error'

    conn.close()

    return st_name, df


def GetDataDict(input_cities=[]):
    #Pull both 2010 and 2020 data for both station IDs listed in arguments
    citydict = {}
    for city in input_cities:
        dfdict = {}
        for yr in [2010,2020]:
            dfdict['sta_name'], dfdict[f'{yr}'] = ReadClimateDB(city,yr)

        citydict[city] = dfdict
    
    return citydict


def CreateHeatmap(datadf,colors='abs',title='',valDesc='',outpath=r""):
    max_ext, min_ext = int(round(datadf.max().max(),0)), int(round(datadf.min().min(),0))
    val_max = max([abs(max_ext), abs(min_ext)]) 

    #Create color map of temperatures and color boundaries of temperature cutoffs using matplotlib
    if colors == 'abs':
        colorList = ['#725ba2','#9c87c0','#c2acde','#67053d','#9c2d8b','#c96ece','#bbd7f0','#6f94ca','#1f259e','#524a6f','#d6d52f','#d78602','#d72c06','#920200','#e569a7','#d4ccd1','#eceebb']
        colorBoundaries = [n for n in range(-40,125,5)]
        vmax, vmin = 125, -40

    if colors == 'chng':
        colorList = ['#2222dd','#ffffff','#dd2222']
        colorBoundaries = [n for n in range(-1*val_max,val_max+1)]
        vmax, vmin = val_max, -1*val_max

    cmap = mplc.LinearSegmentedColormap.from_list("custom",colorList,N=1000)
    norm = mplc.BoundaryNorm(colorBoundaries,len(colorBoundaries),clip=True)

    #Define the tick mark labels for the x and y axes
    xticklabels = []
    xticklist = []
    for m in range(1,13):
        xticklabels.append(dtm(2024,m,1).strftime(r'%b'))
        xticklist.append(dtm(2024,m,1).timetuple().tm_yday)
        
    yticklabels = []
    for h in range(24):
        if h%6 == 0:
            yticklabels.append(f'h{h}')
        else:
            yticklabels.append('')
    yticklabels = yticklabels[::-1]

    #Declare the figure and add the seaborn heatmap
    fig, ax = plt.subplots(figsize=(50,18))
    ax.xaxis.label.set_fontsize(40)
    ax.yaxis.label.set_fontsize(40)
    ax.xaxis.label.set_fontweight('bold')
    ax.yaxis.label.set_fontweight('bold')
    ax.tick_params(axis='x',labelsize=35)
    ax.tick_params(axis='y',labelsize=35)
    hm = sbn.heatmap(data=datadf,
                    vmin=vmin,
                    vmax=vmax,
                    cmap=cmap,
                    ax=ax)
    hm.set(xlabel=None,ylabel='Hour of the Day')
    hm.set_xticks(xticklist)
    hm.set_xticklabels(xticklabels)
    hm.set_yticklabels(yticklabels,rotation=0)

    hm.set_title(title,pad=20,size=50,weight='bold')

    #Customize the coloramp labels
    cbar = ax.collections[0].colorbar
    cbar.set_ticks(colorBoundaries[::2])
    cbar.ax.tick_params(labelsize=35)
    cbar.set_label(valDesc,size=40,weight='bold')

    #Save output image if output filepath provided, otherwise show image
    if outpath:
        plt.savefig(outpath)
    else:
        plt.show()


def RenderCityNormals(input_city,yr='2010',avgdays=7):
    citydict = GetDataDict([input_city])

    #Loop through dict of cities and data
    for city, dfdict in citydict.items():
        if city == 'sta_name':
            continue
        normalsdf = dfdict[yr]
        normalsdf['date'] = normalsdf.index.date

        mat = pd.crosstab(normalsdf.hour,normalsdf.date,normalsdf.hly_temp_normal,aggfunc='mean').fillna(0)
        sta_name = ''.join([w.capitalize() for w in dfdict['sta_name'].split(' ')])

    CreateHeatmap(mat,
                  colors='abs',
                  title = fr"",
                  valDesc = fr"",
                  outpath=fr"")


def RenderCityChange(input_city,avgdays=7):
    citydict = GetDataDict([input_city])

    #Loop through dict of cities and data
    for city, dfdict in citydict.items():
        if city == 'sta_name':
            continue
        dfjoin = dfdict['2010'].join(dfdict['2020'],rsuffix='_20')
        dfjoin['deltaTemp'] = dfjoin['hly_temp_normal_20'] - dfjoin['hly_temp_normal']
        dfjoin['date'] = dfjoin.index.date

        mat = pd.crosstab(dfjoin.hour,dfjoin.date,dfjoin.deltaTemp,aggfunc='mean').fillna(0)
        sta_name = ''.join([w.capitalize() for w in dfdict['sta_name'].split(' ')])

    CreateHeatmap(mat,
                  colors='chng',
                  title=fr"",
                  valDesc=fr"Difference between Decades",
                  outpath=fr"")


def RenderCityComparison(input_city1,input_city2,year='both'):
    citydict = GetDataDict([input_city1,input_city2])
    sta_name1 = ''.join([w.capitalize() for w in citydict[input_city1]['sta_name'].split(' ')])
    sta_name2 = ''.join([w.capitalize() for w in citydict[input_city2]['sta_name'].split(' ')])

    for yr in ['2010','2020']:
        dfjoin = citydict[input_city1][yr].join(citydict[input_city2][yr],rsuffix=f'_{input_city2}')
        dfjoin['deltaTemp'] = dfjoin[f'hly_temp_normal_{input_city2}'] - dfjoin['hly_temp_normal']
        dfjoin['date'] = dfjoin.index.date

        if year in [yr,'both']:
            mat = pd.crosstab(dfjoin.hour,dfjoin.date,dfjoin.deltaTemp,aggfunc='mean').fillna(0)
            
            CreateHeatmap(mat,
                          'chng',
                          title = fr"",
                          valDesc = "Difference between Cities",
                          outpath = fr"")


#Accepts file path for folder of climate data CSVs and a list of variables in the CSVs to combine into one table for all weather stations
def CollectVarFromStationCSVs(folderpath,varlist):
    folderpath = Path(folderpath)
    colList = ['ghcn_id','DATE']
    colList.extend(varlist)

    #Loop through all CSVs in the provided folder, select the variables named in the variable list, and add to the list of dataframes to consolidate
    dfList = []
    for i,f in enumerate(folderpath.glob(r'USW*.csv')):
        print(i, f.stem)
        #if i == 5:
            #break
        thisdf = pd.read_csv(f).rename(columns={'STATION':'ghcn_id'})
        thisdf = thisdf[colList]
        thisdf.rename(columns={c:c.lower().replace('-','_') for c in thisdf.columns},inplace=True)

        #Convert date column to datetime type and derive month, day, and year
        thisdf['date'] = thisdf.apply(lambda row: pd.to_datetime(row['date'],format=r"%m-%dT%H:%M:%S"), axis=1)
        thisdf['month'] = thisdf.apply(lambda row: row['date'].month, axis=1)
        thisdf['day'] = thisdf.apply(lambda row: row['date'].day, axis=1)
        thisdf['hour'] = thisdf.apply(lambda row: row['date'].hour, axis=1)

        #Rearrange columns so all datetime columns are together
        newcols = list(thisdf.columns)
        for n in range(3):
            newcols.insert(2,newcols.pop())
        thisdf = thisdf[newcols]

        dfList.append(thisdf)
        print(SaveToClimateDB(thisdf,'noaa_hlyTemp_2020','append'))

    #Consolidate all processed dataframes into one table to return
    bigdf = pd.concat(dfList)

    return bigdf


#Accepts Pandas DF of climate data to save as new table in the Postgres Climate DB
def SaveToClimateDB(df,tblname,if_exists='replace'):
    engine = create_engine("")

    try:
        df.to_sql(tblname,engine,'climate',if_exists=if_exists)
    except Exception as e:
        print(e)
        return 0
    else:
        return 1