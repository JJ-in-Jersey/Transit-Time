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
        self.__tt_output_file = Path(str(environ.transit_time_folder())+'/TT_'+str(self.__speed)+'_array.npy')
        self.__min_output_file = Path(str(environ.transit_time_folder())+'MIN_'+str(self.__speed)+'_array.npy')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_one()
        self.__seconds = seconds(self.__start, self.__end)
        self.__no_timesteps = int(self.__seconds / timestep)
        self.__speed_df = pd.DataFrame()
        for re in route.route_edges():
            col_name = re.name() + ' ' + str(self.__speed)
            self.__speed_df[col_name] = re.elapsed_time_dataframe()[col_name]

    def execute(self):
        if exists(self.__tt_output_file) and exists(self.__min_output_file):
            print(f'+     {self.__intro} Transit time ({self.__speed}) reading data file', flush=True)
            # noinspection PyTypeChecker
            tt = np.load(self.__tt_output_file)
            # noinspection PyTypeChecker
            tt_minima = np.load(self.__min_output_file)
            return tuple([self.__speed, tuple([tt, tt_minima])])
        else:
            print(f'+     {self.__intro} Transit time ({self.__speed}) calculation starting', flush=True)
            tt = np.fromiter([total_transit_time(row, self.__speed_df) for row in range(0, self.__no_timesteps)], dtype=int)
            tt_minima = minima(tt)
            # noinspection PyTypeChecker
            np.save(self.__tt_output_file, tt)
            # noinspection PyTypeChecker
            np.save(self.__min_output_file, tt_minima)
            return tuple([self.__speed, tuple([tt, tt_minima])])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__speed} {"SUCCESSFUL" if isinstance(result[1], np.ndarray) else "FAILED"} {self.__no_timesteps}', flush=True)
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

def minima(transit_time_array):
    minima_threshold = 0.01
    tt_median = np.median(transit_time_array)
    tt_df = pd.DataFrame()
    tt_df['gradient'] = np.gradient(savgol_filter(transit_time_array, 100, 1))
    tt_df['zero_ish'] = True if abs(tt_df['gradient']) < minima_threshold else False
    zero_ish = tt_df['zero_ish'].to_numpy()
    minima_clumps = []
    clump = []
    for x in zero_ish:
        if x[1] > tt_median:
            if len(clump):
                minima_clumps.append(clump)
                clump = []
        else:
            clump.append(x[0])
    return [np.median(c) for c in minima_clumps]





