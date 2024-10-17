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
            date = a[a.find("_")+1:a.find(".")]
            b = pd.read_excel(a, skiprows=6, skipfooter=1, usecols= lambda x: x not in [1])
            b['Дата'] = date
            ### b['Дата'] = pd.to_datetime(b['Дата'])
            if i == 0: 
                self.result = b
            else: 
                self.result = pd.concat([self.result, b], axis=0)
    def table(self):
        return self.result
