from os.path import exists
from pathlib import Path
import numpy as np
import pandas as pd

from project_globals import seconds, timestep

class TransitTimeJob:

    def __init__(self, route, speed, d_dir, chart_yr, intro=''):
        self.__speed = speed
        self.__intro = intro
        self.__output_file = Path(str(d_dir.transit_time_folder())+'/TT_'+str(self.__speed)+'_array.npy')
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
            # noinspection PyTypeChecker
            return tuple([self.__speed, np.load(self.__output_file)])
        else:
            print(f'+     {self.__intro} Transit time ({self.__speed}) calculation starting', flush=True)
            transit_time_df = pd.DataFrame()
            # noinspection PyTypeChecker
            result = np.fromiter([total_transit_time(row, self.__speed_df) for row in range(0, self.__no_timesteps)], dtype=int)
            # noinspection PyTypeChecker
            np.save(self.__output_file, result)
            return tuple([self.__speed, result])

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
