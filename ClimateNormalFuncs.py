import matplotlib.colors as mplc, numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sbn
from calendar import monthrange
from datetime import datetime as dtm
from pathlib import Path
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

pd.set_option('display.max_columns',10)


def ReadClimateDB(staid,yr,fields=None):
    engine = create_engine(fr"postgresql+psycopg2://postgres:%s@localhost/postgres" % quote_plus("Fsga@1313"))
    conn = engine.connect()

    select_fields = r'*' if not fields else ', '.join(fields)
    sql = fr"SELECT {select_fields} FROM climate.noaa_hlytemp_{yr} WHERE ghcn_id = '{staid}'"
    try:
        df = pd.read_sql_query(text(sql),conn)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'],format=r'%Y-%m-%d %H:%M:%S')
            df.set_index('date',inplace=True)
    except:
        print('Whoops, something broke. Returning empty dataframe.')
        df = pd.DataFrame()

    return df


def GetDataDict(input_cities=[]):
    #Pull both 2010 and 2020 data for both station IDs listed in arguments
    citydict = {}
    for city in input_cities:
        dfdict = {}
        for yr in [2010,2020]:
            dfdict[f'{yr}'] = ReadClimateDB(city,yr)

        citydict[city] = dfdict
    
    return citydict


def CreateHeatmap(datadf,outpath=r"C:\Users\kdh13\OneDrive\Documents\Python\Climate Graphs\thisimage_ATLdelta10d.png"):
    datadf = pd.DataFrame(datadf,index=[f'h{h}' for h in range(datadf.shape[0])],columns=[c for c in range(datadf.shape[1])])

    #Create color map of temperatures and color boundaries of temperature cutoffs using matplotlib
    absColors = ['#725ba2','#9c87c0','#c2acde','#67053d','#9c2d8b','#c96ece','#bbd7f0','#6f94ca','#1f259e','#524a6f','#d6d52f','#d78602','#d72c06','#920200','#e569a7','#d4ccd1','#eceebb']
    absBoundaries = [n for n in range(-40,125,5)]

    chngColors = ['#2222dd','#ffffff','#dd2222']
    chngBoundaries = [n for n in range(-20,20)]

    cmap = mplc.LinearSegmentedColormap.from_list("custom",chngColors,N=1000)
    norm = mplc.BoundaryNorm(chngBoundaries,len(chngBoundaries),clip=True)

    #Define the tick mark labels for the x and y axes
    xticklabels = []
    for m in range(1,13):
        xticklabels.append(dtm(2024,m,1).strftime(r'%b'))
        xticklabels.extend(['']*(monthrange(2024,m)[1]-1))
        
    yticklabels = []
    for h in range(24):
        if h%6 == 0:
            yticklabels.append(f'h{h}')
        else:
            yticklabels.append('')

    #Declare the figure and add the seaborn heatmap
    fig, ax = plt.subplots(figsize=(72,18))
    hm = sbn.heatmap(data=datadf,
                    vmin=-20,
                    vmax=20,
                    cmap=cmap,
                    center=0,
                    xticklabels=xticklabels,
                    yticklabels=yticklabels,
                    ax=ax)

    #Customize the coloramp labels
    cbar = ax.collections[0].colorbar
    cbar.set_ticks(chngBoundaries[::2])

    #Save output image if output filepath provided, otherwise show image
    if outpath:
        plt.savefig(outpath)
    else:
        plt.show()


def RenderCityChange(input_cities=[],avgdays=7):
    citydict = GetDataDict(input_cities)

    #Loop through dict of cities and data
    for city, dfdict in citydict.items():
        dfjoin = dfdict['2010'].join(dfdict['2020'],rsuffix='_20')
        dfjoin['deltaTemp'] = dfjoin['hly_temp_normal_20'] - dfjoin['hly_temp_normal']
        dfjoin['date'] = dfjoin.index.date

        dftemps = dfjoin[['hly_temp_normal','hly_temp_normal_20']]
        mat = pd.crosstab(dfjoin.hour,dfjoin.date,dfjoin.deltaTemp,aggfunc='mean').to_numpy()
        #mat = pd.crosstab(dfjoin.hour,dfjoin.date,dfjoin.hly_temp_normal,aggfunc='mean').to_numpy()
        dfdict['dftemps'] = dftemps
        dfdict['deltamat'] = mat
        print(mat.shape,mat.min(),mat.max())

    CreateHeatmap(mat)


def RenderCityComparison(input_cities,year='both'):
    citydict = GetDataDict(input_cities)
    c1, c2 = 'New York', 'Atlanta'

    for yr in ['2010','2020']:
        dfjoin = citydict[input_cities[0]][yr].join(citydict[input_cities[1]][yr],rsuffix=f'_{input_cities[1]}')
        dfjoin['deltaTemp'] = dfjoin[f'hly_temp_normal_{input_cities[1]}'] - dfjoin['hly_temp_normal']
        dfjoin['date'] = dfjoin.index.date

        if year in [yr,'both']:
            mat = pd.crosstab(dfjoin.hour,dfjoin.date,dfjoin.deltaTemp,aggfunc='mean').to_numpy()
            print(yr,mat.shape,mat.min(),mat.max())
            CreateHeatmap(mat,fr"C:\Users\kdh13\OneDrive\Documents\Python\Climate Graphs\cityComp_{c2}Minus{c1}_{yr}.png")


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
    engine = create_engine("postgresql+psycopg2://postgres:%s@localhost/postgres" % quote_plus("Fsga@1313"))

    try:
        df.to_sql(tblname,engine,'climate',if_exists=if_exists)
    except Exception as e:
        print(e)
        return 0
    else:
        return 1
    

if __name__ == "__main__":
    datadf = CollectVarFromStationCSVs(r"C:\Users\kdh13\Downloads\us-climate-normals_1991-2020",['HLY-TEMP-10PCTL','HLY-TEMP-10PCTL_ATTRIBUTES','HLY-TEMP-90PCTL',
                                                                                                 'HLY-TEMP-90PCTL_ATTRIBUTES','HLY-TEMP-NORMAL',
                                                                                                 'HLY-TEMP-NORMAL_ATTRIBUTES'])
    #print(SaveToClimateDB(datadf,'noaa_hlytemp_2010'))
    datadf.to_csv(r"C:\Users\kdh13\Downloads\us-climate-normals_1991-2020\data_for_postgres.csv")