from os.path import exists
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
from warnings import filterwarnings as fw

from project_globals import TIMESTEP, WINDOW_SIZE, seconds, rounded_to_minutes

fw("ignore", message="FutureWarning: iteritems is deprecated and will be removed in a future version. Use .items instead.",)

class TransitTimeMinimaJob:

    def __init__(self, route, speed, environ, chart_yr, intro=''):
        self.__speed = speed
        self.__chart_yr = chart_yr
        self.__intro = intro
        self.__minima__index_table_file = Path(str(environ.transit_time_folder()) + '/TT_' + str(self.__speed) + '_minima_index.csv')
        self.__output_file = Path(str(environ.transit_time_folder()) + '/TT_' + str(self.__speed) + '.csv')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_one()
        self.__seconds = seconds(self.__start, self.__end)
        self.__no_timesteps = int(self.__seconds / TIMESTEP)
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
            print(f'+     {self.__intro} Transit time ({self.__speed}) transit time (1st day - 1, last day + 1)', flush=True)
            transit_times = np.fromiter([TransitTimeMinimaJob.total_transit_time(row, self.__speed_df) for row in range(0, self.__no_timesteps)], dtype=int)
            minima_index_table_df = self.minima_table(transit_times)
            minima_index_table_df.to_csv(self.__minima__index_table_file)
            minima_time_table_df = self.start_min_end(minima_index_table_df)
            minima_time_table_df.to_csv(self.__output_file)
            return tuple([self.__speed, minima_index_table_df])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__speed} {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"} {self.__no_timesteps}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__speed} process has raised an error: {result}', flush=True)

    @staticmethod
    def total_transit_time(init_row, d_frame):
        row = init_row
        tt = 0
        for col in d_frame.columns:
            val = d_frame.at[row, col]
            tt += val
            row += val
        return tt

    def start_min_end(self, minima_table_df):
        minima_table_df = minima_table_df.dropna()
        minima_table_df.reset_index(inplace=True)
        minima_table_df = minima_table_df.assign(start_time=[self.__chart_yr.index_to_time(minima_table_df.at[index, 'start_index']*TIMESTEP) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(minima_time=[self.__chart_yr.index_to_time(minima_table_df.at[index, 'minima_index']*TIMESTEP) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(end_time=[self.__chart_yr.index_to_time(minima_table_df.at[index, 'end_index']*TIMESTEP) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(start_rounded=[rounded_to_minutes(minima_table_df.at[index, 'start_time'], WINDOW_SIZE) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(minima_rounded=[rounded_to_minutes(minima_table_df.at[index, 'minima_time'], WINDOW_SIZE) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(end_rounded=[rounded_to_minutes(minima_table_df.at[index, 'end_time'], WINDOW_SIZE) for index in range(0, len(minima_table_df))])
        return minima_table_df

    def minima_table(self, transit_time_array):
        tt_df = pd.DataFrame()
        tt_df['tt'] = transit_time_array
        tt_df['date'] = [self.__chart_yr.index_to_time(index*TIMESTEP) for index in range(0, len(transit_time_array))]
        tt_df['midline'] = savgol_filter(transit_time_array, 50000, 1)
        tt_df['min_segments'] = tt_df.apply(lambda row: True if row.tt < row.midline else False, axis=1)
        clump = []
        mlv = tt_df['min_segments'].to_numpy()
        for index, val in enumerate(mlv):
            if val:
                clump.append(index)
            elif len(clump) > 1:  # ignore zero length clumps
                segment = tt_df['tt'][clump[0]: clump[-1]]
                indices = segment[segment == segment.min()].index
                # noinspection PyTypeChecker
                minimum_index = round(np.median(indices), 0)
                if minimum_index != clump[0] and minimum_index != clump[-1]:  # ignore minima at edges
                    offset = tt_df.at[minimum_index, 'tt'] + WINDOW_SIZE * 60 / TIMESTEP
                    start_segment = segment.loc[segment.index[0]:minimum_index]
                    start_index = start_segment[start_segment <= offset].index[0]
                    end_segment = segment.loc[minimum_index:segment.index[-1]]
                    end_index = end_segment[end_segment >= offset].index[0]
                    tt_df.at[minimum_index, 'start_index'] = start_index
                    tt_df.at[minimum_index, 'minima_index'] = minimum_index
                    tt_df.at[minimum_index, 'end_index'] = end_index
                    tt_df.at[start_index, 'plot'] = 'S'
                    tt_df.at[minimum_index, 'plot'] = 'M'
                    tt_df.at[end_index, 'plot'] = 'E'
                clump = []
        tt_df.drop(columns=['midline', 'min_segments'], inplace=True)
        return tt_df
