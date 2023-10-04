import os
import pandas as pd
files = os.listdir('datafiles/KGfiles')

df = pd.DataFrame()
for nf in files:
	df2 = pd.read_csv('datafiles/KGfiles/'+nf)
	df = pd.concat([df,df2])

df = df[['WikidataID','relntype','relnID']]
df = df.rename(columns={"WikidataID":"head","relntype":"type","relnID":"tail"})
df = df.drop_duplicates()
df.to_csv('/mnt/d/Documents/kbHL_reln2.csv', index=False)
