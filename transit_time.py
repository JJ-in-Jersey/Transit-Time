import pandas as pd
from scipy.signal import savgol_filter
from functools import reduce
from warnings import filterwarnings as fw

from project_globals import TIMESTEP, TIMESTEP_MARGIN, seconds, rounded_to_minutes, output_file_exists, hours_min
# noinspection PyUnresolvedReferences
from project_globals import write_df_pkl, read_df, write_df_csv

fw("ignore", message="FutureWarning: iteritems is deprecated and will be removed in a future version. Use .items instead.",)

class ElapsedTimeReduce:

    def __init__(self, route, speed, env, chart_yr, intro=''):
        self.speed = speed
        self.date = chart_yr
        self.intro = intro
        self.route = route
        self.elapsed_times_path = env.transit_time_folder().joinpath('et_reduce_' + str(speed))
        self.shared_columns = ['departure_index', 'departure_time']
        self.elapsed_time_tables = [read_df(segment.elapsed_time_table_path()) for segment in self.route.route_segments()]

    def execute(self):
        if output_file_exists(self.elapsed_times_path):
            print(f'+     {self.intro} Elapsed Time Reduce ({self.speed}) reading data file', flush=True)
            return tuple([self.speed, read_df(self.elapsed_times_path)])
        else:
            print(f'+     {self.intro} Elapsed Time Reduce ({self.speed})', flush=True)
            et_reduce_df = reduce(lambda left, right: pd.merge(left, right, on=self.shared_columns), self.elapsed_time_tables)
            write_df_pkl(et_reduce_df, self.elapsed_times_path)
        return tuple([self.speed, et_reduce_df])

    def execute_callback(self, result):
        print(f'-     {self.intro} Elapsed Time Reduce ({self.speed}) {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} Elapsed Time Reduce ({self.speed}) process has raised an error: {result}', flush=True)


class TransitTimeMinimaJob:

    def __init__(self, route, speed, environ, chart_yr, intro=''):
        self.speed = speed
        self.env = environ
        self.date = chart_yr
        self.intro = intro
        self.plotting_table = environ.transit_time_folder().joinpath('tt_' + str(speed) + '_plotting_table')
        self.elapsed_time_table = environ.transit_time_folder().joinpath('et_' + str(speed))
        self.output_file = environ.transit_time_folder().joinpath('transit_time_' + str(speed))
        self.start = chart_yr.first_day_minus_one()
        self.end = chart_yr.last_day_plus_one()
        self.no_timesteps = int(seconds(self.start, self.end) / TIMESTEP)
        self.shared_columns = ['departure_index', 'departure_time']
        self.speed_columns = [segment.name() + ' ' + str(speed) for segment in route.route_segments()]
        self.elapsed_time_df = reduce(lambda left, right: pd.merge(left, right, on=self.shared_columns), [segment.elapsed_time_df() for segment in route.route_segments()])
        write_df_pkl(self.elapsed_time_df, self.elapsed_time_table)

    # def __del__(self):
    #     print(f'Deleting Transit Time Job', flush=True)

    def execute(self):
        if output_file_exists(self.output_file):
            print(f'+     {self.intro} Transit time ({self.speed}) reading data file', flush=True)
            tt_minima_df = read_df(self.output_file)
            return tuple([self.speed, tt_minima_df])
        else:
            print(f'+     {self.intro} Transit time ({self.speed}) transit time', flush=True)
            transit_timesteps = [TransitTimeMinimaJob.total_transit_time(row, self.elapsed_time_df, self.speed_columns) for row in range(0, self.no_timesteps)]  # in timesteps
            minima_table_df = self.minima_table(transit_timesteps)
            write_df_csv(minima_table_df, self.plotting_table)
            minima_time_table_df = self.start_min_end(minima_table_df)
            write_df_csv(minima_time_table_df, self.output_file)
            return tuple([self.speed, minima_time_table_df])

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
        minima_table_df.reset_index(inplace=True, drop=True)
        minima_table_df['start_time'] = pd.to_timedelta(minima_table_df['start_index'], unit='seconds') + self.date.index_basis()
        minima_table_df['min_time'] = pd.to_timedelta(minima_table_df['min_index'], unit='seconds') + self.date.index_basis()
        minima_table_df['end_time'] = pd.to_timedelta(minima_table_df['end_index'], unit='seconds') + self.date.index_basis()
        minima_table_df['window_time'] = (minima_table_df['end_time'] - minima_table_df['start_time']).apply(hours_min)
        minima_table_df['start_rounded'] = minima_table_df['start_time'].apply(rounded_to_minutes)
        minima_table_df['min_rounded'] = (minima_table_df['min_time'].apply(rounded_to_minutes))
        minima_table_df['end_rounded'] = (minima_table_df['end_time'].apply(rounded_to_minutes))
        minima_table_df['window_rounded'] = (minima_table_df['end_rounded'] - minima_table_df['start_rounded']).apply(hours_min)
        # minima_table_df.drop(columns=['midline'], inplace=True)
        return minima_table_df

    def minima_table(self, transit_array):
        tt_df = pd.DataFrame(columns=['departure_index', 'departure_time', 'tts', 'midline', 'min_segments', 'start_index', 'min_index', 'end_index', 'plot'])
        tt_df['departure_index'] = self.elapsed_time_df[['departure_index']]
        tt_df['departure_time'] = pd.to_datetime(self.elapsed_time_df['departure_time'])
        tt_df = tt_df[tt_df['departure_time'].lt(self.end)]  # trim to lenth of transit array
        tt_df['tts'] = pd.Series(transit_array)
        tt_df['midline'] = savgol_filter(transit_array, 50000, 1)
        tt_df['min_segments'] = tt_df['tts'].lt(tt_df['midline'])

        clump = []
        min_segs = tt_df['min_segments'].to_list()  # list of True or False the same length as tt_df
        for row, val in enumerate(min_segs):  # rows in tt_df, not the departure index
            if val:
                clump.append(row)  # list of the rows within the clump of True min_segs
            elif len(clump) > 1:  # ignore zero length clumps
                segment_df = tt_df[clump[0]:clump[-1]]  # subset of tt_df from first True to last True in clump
                seg_min_df = segment_df[segment_df['tts'] == segment_df.min()['tts']]  # segment rows equal to the minimum
                median_index = seg_min_df['departure_index'].median()  # median departure index of segment minima
                abs_diff = segment_df['departure_index'].sub(median_index).abs()  # abs of difference between actual index and median index
                min_index = seg_min_df.at[abs_diff[abs_diff == abs_diff.min()].index[0], 'departure_index']  # actual index closest to median index
                offset = segment_df['tts'].min() + TIMESTEP_MARGIN  # minimum tss + margin for window
                if min_index != clump[0] and min_index != clump[-1]:  # ignore minima at edges
                    start_segment = segment_df[segment_df['departure_index'].le(min_index)]  # portion of segment from start to minimum
                    end_segment = segment_df[segment_df['departure_index'].ge(min_index)]  # portion of segment from minimum to end
                    start_row = start_segment[start_segment['tts'].le(offset)].index[0]
                    end_row = end_segment[end_segment['tts'].ge(offset)].index[0]
                    start_index = start_segment.at[start_row, 'departure_index']
                    end_index = end_segment.at[end_row, 'departure_index']
                    tt_df_start_row = tt_df[tt_df['departure_index'] == start_index].index[0]
                    tt_df_min_row = tt_df[tt_df['departure_index'] == min_index].index[0]
                    tt_df_end_row = tt_df[tt_df['departure_index'] == end_index].index[0]
                    tt_df.at[tt_df_min_row, 'start_index'] = start_index
                    tt_df.at[tt_df_min_row, 'min_index'] = min_index
                    tt_df.at[tt_df_min_row, 'end_index'] = end_index
                    tt_df.at[tt_df_start_row, 'plot'] = 'S'
                    tt_df.at[tt_df_min_row, 'plot'] = 'M'
                    tt_df.at[tt_df_end_row, 'plot'] = 'E'
                clump = []
        tt_df.drop(columns=['min_segments'], inplace=True)
        return tt_df
