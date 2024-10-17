import numpy as np
import pandas as pd
import os
import time
import datetime
from datetime import datetime as dtm
from datetime import timedelta
from tqdm import tqdm
from tqdm.notebook import trange

class T1(object):
    def __init__(self, url):
        megasub = '/Макроэкономика и банки/Банк России/Статистика национальной платежной системы (Y)'
        sub = '/1. Институциональная обеспеченность платежными услугами'
        link = url+megasub+sub
        os.chdir(link)
        names = pd.Series(os.listdir('.'))
        for i in trange(len(names), leave = False):
            a = names[i]
            xlsx = pd.ExcelFile(a)
            data = {}
            for sheet_name in xlsx.sheet_names:
                data[sheet_name] = xlsx.parse(sheet_name)
            for q in range(len(xlsx.sheet_names)):
                d = data[xlsx.sheet_names[q]]
                d = d.iloc[6:len(d)-1,1:9].reset_index(drop=True)
                d.columns = np.arange(1, len(d.columns)+1, 1)
                d = d.loc[d[1].notna()]
                date = pd.to_datetime(xlsx.sheet_names[q].replace('_', '.'), dayfirst=True)
                d['date'] = date
                if i == 0 and q == 0: 
                    result = d
                else: 
                    result = pd.concat([result, d], axis=0)
        self.step1 = result
    def table(self):
        return self.step1
