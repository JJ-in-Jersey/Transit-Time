from os.path import exists
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
from warnings import filterwarnings as fw

from project_globals import seconds, timestep, window_size

fw("ignore", message="FutureWarning: iteritems is deprecated and will be removed in a future version. Use .items instead.",)

class TransitTimeMinimaJob:

    def __init__(self, route, speed, environ, chart_yr, intro=''):
        self.__speed = speed
        self.__intro = intro
        self.__output_file = Path(str(environ.transit_time_folder()) + '/TT_' + str(self.__speed) + '.csv')
        self.__debug_file = Path(str(environ.transit_time_folder()) + '/TT_' + str(self.__speed) + '_debug.csv')
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
            tt_minima_df.to_csv(self.__debug_file)
            tt_minima_df.drop(columns=['tt', 'midline', 'min_segments', 'plot'], inplace=True)
            tt_minima_df.drop(columns=tt_minima_df.columns[0], inplace=True)
            tt_minima_df.dropna(inplace=True)
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
        val = d_frame.at[row, col]
        tt += val
        row += val
    return tt

def median_index(index_array, tt_series):
    segment = tt_series[index_array[0]: index_array[-1]]
    indices_of_minima = segment[segment == segment.min()].index
    # noinspection PyTypeChecker
    return round(np.median(indices_of_minima), 0)

def minima_table(transit_time_array):
    tt_df = pd.DataFrame()
    tt_df['tt'] = transit_time_array
    tt_df['midline'] = savgol_filter(transit_time_array, 50000, 1)
    tt_df['min_segments'] = tt_df.apply(lambda row: True if row.tt < row.midline else False, axis=1)

    clump = []
    mlv = tt_df['min_segments'].to_numpy()
    for index, val in enumerate(mlv):
        if val: 
            clump.append(index)
        elif len(clump) > 1:
            segment = tt_df['tt'][clump[0]: clump[-1]]
            indices_of_minima = segment[segment == segment.min()].index
            # noinspection PyTypeChecker
            minimum_index = round(np.median(indices_of_minima), 0)
            if minimum_index != clump[0] and minimum_index != clump[-1]:
                start_segment = segment.loc[segment.index[0]:minimum_index]
                end_segment = segment.loc[minimum_index:segment.index[-1]]
                minimum_tt = tt_df.at[minimum_index, 'tt']
                window_offset = minimum_tt + window_size*60/timestep
                start_segment = start_segment[start_segment <= window_offset]
                end_segment = end_segment[end_segment >= window_offset]
                # print(f'seg[0]={segment.index[0]} min_TT={minimum_tt} win_offset={window_offset} seg[-1]={segment.index[-1]}')
                # print(f'start[0]={start_segment.index[0]} start[-1]={start_segment.index[-1]} end[0]={end_segment.index[0]} end[-1]={end_segment.index[-1]}')
                tt_df.at[minimum_index, 'start'] = start_segment.index[0]
                tt_df.at[minimum_index, 'minima'] = minimum_index
                tt_df.at[minimum_index, 'end'] = end_segment.index[0]
                tt_df.at[minimum_index, 'start_tt'] = start_segment.loc[start_segment.index[0]]
                tt_df.at[minimum_index, 'minima_tt'] = tt_df.at[minimum_index, 'tt']
                tt_df.at[minimum_index, 'end_tt'] = end_segment.loc[end_segment.index[0]]
                tt_df.at[start_segment.index[0], 'plot'] = 'S'
                tt_df.at[minimum_index, 'plot'] = 'M'
                tt_df.at[end_segment.index[0], 'plot'] = 'E'
            clump = []

    return tt_df
