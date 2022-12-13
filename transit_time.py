from os.path import exists
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter

from project_globals import seconds, timestep

class TransitTimeMinimaJob:

    def __init__(self, route, speed, environ, chart_yr, intro=''):
        self.__speed = speed
        self.__intro = intro
        self.__output_file = Path(str(environ.transit_time_folder()) + '/TT_' + str(self.__speed) + '.csv')
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
            transit_times = np.fromiter([total_transit_time(row, self.__speed_df) for row in range(0, self.__no_timesteps)], dtype=int)
            tt_minima_df = minima_table(transit_times)
            tt_minima_df.to_csv(self.__output_file)
            return tuple([self.__speed, tt_minima_df])

    def execute_callback(self, result):
        print(
            f'-     {self.__intro} {self.__speed} {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"} {self.__no_timesteps}',
            flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__speed} process has raised an error: {result}', flush=True)

def total_transit_time(init_row, d_frame):
    row = init_row
    tt = 0
    for col in d_frame.columns:
        val = d_frame.at[row, col]
        tt += val
        row += val
    return tt

def median_index(index_array, tt_series):
    segment = tt_series[index_array[0]: index_array[-1]]
    indices_of_minima = segment[segment == segment.min()].index
    # noinspection PyTypeChecker
    return round(np.median(indices_of_minima),0)

def minima_table(transit_time_array):
    tt_df = pd.DataFrame()
    tt_df['minima'] = 0
    tt_df['tt'] = transit_time_array
    tt_df['midline'] = savgol_filter(transit_time_array, 50000, 1)
    tt_df['min_segments'] = tt_df.apply(lambda row: True if row.tt < row.midline else False, axis=1)

    clump = []
    mlv = tt_df['min_segments'].to_numpy()
    for index, val in enumerate(mlv):
        if val: 
            clump.append(index)
        elif len(clump) > 1:
            tt_df.at[median_index(clump,tt_df['tt']), 'minima'] = 1
            clump = []

    # tt_df['start'] = tt_df.apply(lambda row: row.gradient_change if row.Savitzky_Golay < row.midline else 0, axis=1)
    # tt_df['end'] = tt_df.apply(lambda row: row.gradient_change if row.Savitzky_Golay < row.midline else 0, axis=1)

    return tt_df