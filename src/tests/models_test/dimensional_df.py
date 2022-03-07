from src.dimensional.dimen_resize import ReSize
import pandas as pd
from src.mafs.models.Prophet import MAFSProphetModel

if __name__ == '__main__':
    df = pd.read_csv('../../../data/01_raw/cpu_idle__0d156ba9_f0b3_4c79_8956_2ec9629237ba', sep='\t')
    df = df[['endpoint', 'time', 'value']]
    df = df.groupby(['endpoint'])

    for x in df:

        import time

        start_time = time.time()

        a = x[1][['time', 'value']]
        a['time_stamp'] = a['time'].apply(lambda y: pd.Timestamp(y))
        a.drop('time', axis=1, inplace=True)
        MAFSProphetModel(ReSize(a, step=300, method='PAA').get_data()).predict(step=300)

        print('消耗时间:', time.time() - start_time, '秒')



