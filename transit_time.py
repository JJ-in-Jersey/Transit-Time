from os.path import exists
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
from functools import reduce
from warnings import filterwarnings as fw

from project_globals import TIMESTEP, WINDOW_MARGIN, seconds, rounded_to_minutes, write_df_pkl

fw("ignore", message="FutureWarning: iteritems is deprecated and will be removed in a future version. Use .items instead.",)

class TransitTimeMinimaJob:

    def __init__(self, route, speed, environ, chart_yr, intro=''):
        self.speed = speed
        self.env = environ
        self.date = chart_yr
        self.intro = intro
        self.minima_index_table_file = environ.transit_time_folder().joinpath('tt_' + str(self.speed) + '_minima_index.csv')
        self.elapsed_time_table = environ.transit_time_folder().joinpath('et_' + str(self.speed) + '.csv')
        self.output_file = environ.transit_time_folder().joinpath('/t_' + str(self.speed) + '.csv')
        self.start = chart_yr.first_day_minus_one()
        self.end = chart_yr.last_day_plus_one()
        self.seconds = seconds(self.start, self.end)
        self.no_timesteps = int(self.seconds / TIMESTEP)
        self.shared_columns = ['departure_index', 'departure_time']
        self.speed_columns = [edge.name() + ' ' + str(speed) for edge in route.route_edges()]
        self.elapsed_time_df = reduce(lambda left, right: pd.merge(left, right, on=self.shared_columns), [edge.elapsed_time_df() for edge in route.route_edges()])
        write_df_pkl(self.elapsed_time_df, self.elapsed_time_table)

    def execute(self):
        if exists(self.output_file):
            print(f'+     {self.intro} Transit time ({self.speed}) reading data file', flush=True)
            tt_minima_df = pd.read_csv(self.output_file, header='infer')
            return tuple([self.speed, tt_minima_df])
        else:
            print(f'+     {self.intro} Transit time ({self.speed}) transit time', flush=True)
            transit_times = [TransitTimeMinimaJob.total_transit_time(row, self.elapsed_time_df, self.speed_columns) for row in range(0, self.no_timesteps)]
            minima_index_table_df = self.minima_table(transit_times)
            minima_index_table_df.to_csv(self.minima_index_table_file)
            minima_time_table_df = self.start_min_end(minima_index_table_df)
            write_df_pkl(minima_time_table_df, self.output_file)
            #minima_time_table_df.to_csv(self.output_file)
            return tuple([self.speed, minima_index_table_df])

    def execute_callback(self, result):
        print(f'-     {self.intro} {self.speed} {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"} {self.no_timesteps}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} {self.speed} process has raised an error: {result}', flush=True)

    @staticmethod
    def total_transit_time(init_row, d_frame, cols):
        row = init_row
        tt = 0
        for col in cols:
            val = d_frame.at[row, col]
            tt += val
            row += val
        return tt

    def start_min_end(self, minima_table_df):
        minima_table_df = minima_table_df.dropna()
        minima_table_df.reset_index(inplace=True)
        minima_table_df['start_time'] = pd.to_timedelta(minima_table_df['start_index'], unit='seconds')*TIMESTEP + self.date.index_basis()*TIMESTEP
        minima_table_df = minima_table_df.assign(start_time=[self.date.index_to_time(minima_table_df.at[index, 'start_index']*TIMESTEP) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(minima_time=[self.date.index_to_time(minima_table_df.at[index, 'minima_index']*TIMESTEP) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(end_time=[self.date.index_to_time(minima_table_df.at[index, 'end_index']*TIMESTEP) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(start_rounded=[rounded_to_minutes(minima_table_df.at[index, 'start_time'], WINDOW_MARGIN) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(minima_rounded=[rounded_to_minutes(minima_table_df.at[index, 'minima_time'], WINDOW_MARGIN) for index in range(0, len(minima_table_df))])
        minima_table_df = minima_table_df.assign(end_rounded=[rounded_to_minutes(minima_table_df.at[index, 'end_time'], WINDOW_MARGIN) for index in range(0, len(minima_table_df))])
        return minima_table_df

    def minima_table(self, transit_time_array):
        tt_df = self.elapsed_time_df[[self.shared_columns[0]]]
        tt_df[self.shared_columns[1]] = pd.to_timedelta(tt_df[self.shared_columns[0]], unit='seconds')
        tt_df[self.shared_columns[1]] = tt_df[self.shared_columns[1]] + self.date.index_basis()
        tt_df = tt_df[tt_df[self.shared_columns[1]] < self.end]
        tt_df.assign(tt = transit_time_array)
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
                    offset = tt_df.at[minimum_index, 'tt'] + WINDOW_MARGIN * 60 / TIMESTEP
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
