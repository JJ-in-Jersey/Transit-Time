from os.path import exists
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter

from project_globals import seconds, timestep

class TransitTimeJob:

    def __init__(self, route, speed, environ, chart_yr, intro=''):
        self.__speed = speed
        self.__intro = intro
        self.__output_file = Path(str(environ.transit_time_folder())+'/TT_'+str(self.__speed)+'.csv')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_one()
        self.__seconds = seconds(self.__start, self.__end)
        self.__no_timesteps = int(self.__seconds / timestep)
        self.__speed_df = pd.DataFrame()
        for re in route.route_edges():
            col_name = re.name() + ' ' + str(self.__speed)
            self.__speed_df[col_name] = re.elapsed_time_dataframe()[col_name]

    def execute(self):
        if exists(self.__output_file):
            print(f'+     {self.__intro} Transit time ({self.__speed}) reading data file', flush=True)
            tt_minima_df = pd.read_csv(self.__output_file, header='infer')
            return tuple([self.__speed, tt_minima_df])
        else:
            print(f'+     {self.__intro} Transit time ({self.__speed}) calculation starting', flush=True)
            tt_minima_df = minima_table(np.fromiter([total_transit_time(row, self.__speed_df) for row in range(0, self.__no_timesteps)], dtype=int))

            tt_minima_df.to_csv(self.__output_file)
            return tuple([self.__speed, tt_minima_df])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__speed} {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"} {self.__no_timesteps}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__speed} process has raised an error: {result}', flush=True)

def total_transit_time(init_row, d_frame):
    row = init_row
    tt = 0
    for col in d_frame.columns:
        val = d_frame.loc[row, col]
        tt += val
        row += val
    return tt

def minima_table(transit_time_array):
    minima_threshold = 0.125
    savgol = savgol_filter(transit_time_array, 100, 1)
    tt_median = np.median(savgol)
    tt_df = pd.DataFrame()
    tt_df['tt'] = transit_time_array
    tt_df['Savitzky-Golay'] = (savgol)
    tt_df['gradient'] = np.gradient(savgol)
    tt_df['gradient'] = tt_df['gradient'].abs()
    tt_df['zero_ish'] = tt_df['gradient'].apply(lambda x: True if x < minima_threshold else False)
    tt_df['minima'] = False
    tt_df.to_csv('C:\\users\\bronfelj\\downloads\\ER\\'+str(tt_median)+'.csv')

    # convert clumps into single best estimate of minima
    clump = []
    minima_clumps = []
    zero_ish = tt_df['zero_ish'].to_numpy()
    print(len(zero_ish), len(savgol))

    for index, tt in enumerate(savgol):
        if zero_ish[index] and tt > tt_median:
            print(f'index: {index} clump len: {len(clump)}')
            if len(clump):
                print(np.median(clump))
                minima_clumps.append(np.median(clump))
                clump = []
        elif zero_ish[index] and tt < tt_median:
            clump.append(index)
    for index in [np.median(c) for c in minima_clumps]:
        tt_df.loc[index,'minima'] = True
    return tt_df.drop(columns=['gradient', 'zero_ish'])





