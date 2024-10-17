import numpy as np
import pandas as pd
import os
import time
import datetime
from datetime import datetime as dtm
from datetime import timedelta
from tqdm import tqdm
from tqdm.notebook import trange

class Create_Class(object):
    def __init__(self, url):
        megasub = '/Макроэкономика и банки/Банк России/Статистика национальной платежной системы (Y)'
        sub = '/1. Институциональная обеспеченность платежными услугами'
        link = url+megasub+sub
        names = pd.Series(os.listdir('.'))
        for i in trange(len(names), leave = False):
            a = names[i]
            date = a.replace('.xls', '')
            b = pd.read_excel(a)
            b['Дата'] = date
            ### b['Дата'] = pd.to_datetime(b['Дата'])
            if i == 0: 
                self.result = b
            else: 
                self.result = pd.concat([self.result, b], axis=0)
    def table(self):
        return self.result
