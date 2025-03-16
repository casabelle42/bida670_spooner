import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

def parse_event(event):
    state = event[:2]
    climateRegion = event[2]
    season = event[3]
    buildingNo = int(event[4:6])
    startDate = pd.to_datetime(event[6:]).strftime('%Y-%m-%d')

    return state, climateRegion, season, buildingNo, startDate

def fix_event(df):
    newdf = df.copy()
    event0 = []
    event1 = []
    eventList = df['EVENT'].tolist()
    for event in eventList:
        stringsplit = event.split('-')
        event0.append(stringsplit[0])
        event1.append(stringsplit[1])
    #print(event0)
    newdf['EVENT'] = event0
    newdf['EVENT1'] = event1
    return newdf
    
def change_to_UTF8(df):
  for col in df.columns:
    if df[col].dtype == 'object':
      df[col] = df[col].astype(str).apply(lambda x: x.encode('utf-8').decode('utf-8'))
      #df[col] = df[col].apply(lambda x: x.str.decode('utf-8'))
  return df
  
'''def change_to_UTF8(df):
  bytes_cols = df.applymap(lambda col: isinstance(col, bytes)).all(0)
  bytes_cols = df.columns[bytes_cols]
  df.loc[:, bytes_cols] = df[bytes_cols].applymap(lambda col: col.decode("utf-8", errors="ignore"))
  return df'''
  
rootdir = r"E:\bida_670\BASE CD\DATA\SAS"

mtr_datadir = os.path.join(rootdir, "MTR")
qsn_datadir = os.path.join(rootdir, "QSN")
svy_datadir = os.path.join(rootdir, "SVY")


files = ["bactair", "fungair", "voc", "weather", "filea1", "survey"]





for afile in files:
  print(afile)
  if afile == "bactair":
    fn = os.path.join(mtr_datadir, f"{afile}.sas7bdat")
    df = pd.read_sas(fn, format = 'sas7bdat')
    change_to_UTF8(df)
    # Get the index of rows concentration is less than 0
    index_to_drop = df[df['CONCENTR'] < 0].index

    # Remove rows with the specified index
    subset = df.drop(index_to_drop)

    subset = subset.loc[:, ['CONCENTR', 'BACT_GRP', 'EVENT']]
    subset = subset.rename(columns={'CONCENTR': 'BACT_CONCENTR'})
    subset.reset_index(inplace=True, drop=True)

  if afile == "fungair":
    fn = os.path.join(mtr_datadir, f"{afile}.sas7bdat")
    df = pd.read_sas(fn, format = 'sas7bdat')
    change_to_UTF8(df)
    # Get the index of rows where 'col2' is 'B'
    index_to_drop = df[df['CONCENTR'] < 0].index

    # Remove rows with the specified index
    subset = df.drop(index_to_drop)

    subset = subset.loc[:, ['CONCENTR', 'FUNG_GRP', 'EVENT']]
    subset = subset.rename(columns={'CONCENTR': 'FUNG_CONCENTR'})
    subset.reset_index(inplace=True, drop=True)
    
  elif afile == "voc":
    fn = os.path.join(mtr_datadir, f"{afile}.sas7bdat")
    df = pd.read_sas(fn, format = 'sas7bdat')
    change_to_UTF8(df)
    #Get the index of rows concentration is less than 0
    index_to_drop = df[df['CONCENT1'] < 0].index

    # Remove rows with the specified index
    subset = df.drop(index_to_drop)
    
    subset = subset.loc[:, ['COMPOUND', 'CONCENT1', 'CONCENT2', 'EVENT']]
    subset = subset.rename(columns={'CONCENT1': 'VOC_CONCENT1', 'CONCENT2': 'VOC_CONCENT2'})
    subset.dropna(axis=0, inplace=True)
    subset.reset_index(inplace=True, drop=True)
    
  elif afile == "weather":
    fn = os.path.join(mtr_datadir, f"{afile}.sas7bdat")
    df = pd.read_sas(fn, format = 'sas7bdat')
    change_to_UTF8(df)
    subset = df.dropna(axis=0)
    subset.reset_index(inplace=True, drop=True)
  
  elif afile == "filea1":
    fn = os.path.join(svy_datadir, f"{afile}.sas7bdat")
    df = pd.read_sas(fn, format = 'sas7bdat')
    change_to_UTF8(df)
    subset = df.loc[:, ['A1SITE1', 'A1SITE2', 'A1FLR1A', 'A1YEAR', 'EVENT']]
    
  elif afile == "survey":
    fn = os.path.join(qsn_datadir, f"{afile}.sas7bdat")
    df = pd.read_sas(fn, format = 'sas7bdat')
    change_to_UTF8(df)
    survdf = fix_event(df)
    
    subset = df.loc[:, ['CHESTTI', 'FATIGUE', 'HEADACH', 'EVENT']]
    # Columns to check for NaN values
    cols_to_check = ['CHESTTI', 'FATIGUE', 'HEADACH']

    # Drop rows where all specified columns have NaN values
    subset = subset.dropna(subset=cols_to_check, how='all')

    subset.reset_index(inplace=True, drop=True)
  
  subset.to_csv(f'SUBSETS-{afile}.csv', index=False)

  
  
    