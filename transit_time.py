import pandas as pd
from scipy.signal import savgol_filter
from time import perf_counter

from project_globals import TIMESTEP, TIMESTEP_MARGIN, FIVE_HOURS_OF_TIMESTEPS, seconds, rounded_to_minutes, output_file_exists, hours_min, min_sec
from project_globals import read_df, read_df_hdf, write_df, write_df_csv, write_df_hdf, shared_columns, read_list, write_list

df_type = 'csv'

def total_transit_time(init_row, d_frame, cols):
    row = init_row
    tt = 0
    for col in cols:
        val = d_frame.at[row, col]
        tt += val
        row += val
    return tt

class TransitTimeMinimaJob:

    def __init__(self, route, speed, env, chart_yr, intro=''):
        self.speed = speed
        self.date = chart_yr
        self.intro = intro
        self.start = chart_yr.first_day_minus_one()
        self.end = chart_yr.last_day_plus_one()
        self.elapsed_time_df = route.elapsed_times()
        self.speed_columns = [segment.name() + ' ' + str(speed) for segment in route.route_segments()]
        self.no_timesteps = int(seconds(self.start, self.end) / TIMESTEP)
        self.plotting_table = env.transit_time_folder().joinpath('tt_' + str(speed) + '_plotting_table')
        self.savgol = env.transit_time_folder().joinpath('tt_' + str(speed) + '_savgol')
        self.transit_timesteps = env.transit_time_folder().joinpath('tt_' + str(speed) + '_timesteps')
        self.elapsed_time_table = env.transit_time_folder().joinpath('et_' + str(speed))
        self.transit_time = env.transit_time_folder().joinpath('transit_time_' + str(speed))

    # def __del__(self):
    #     print(f'Deleting Transit Time Job', flush=True)

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self.transit_time):
            print(f'+     {self.intro} Transit time ({self.speed}) reading data file', flush=True)
            tt_minima_df = read_df(self.transit_time)
            return tuple([self.speed, tt_minima_df, init_time])
        else:
            print(f'+     {self.intro} Transit time ({self.speed})', flush=True)
            if output_file_exists(self.transit_timesteps):
                transit_timesteps = read_list(self.transit_timesteps)
            else:
                transit_timesteps = [total_transit_time(row, self.elapsed_time_df, self.speed_columns) for row in range(0, self.no_timesteps)]  # in timesteps
                write_list(transit_timesteps, self.transit_timesteps)
            minima_table_df = self.minima_table(transit_timesteps)
            write_df_csv(minima_table_df, self.plotting_table)
            minima_time_table_df = self.start_min_end(minima_table_df)
            write_df(minima_time_table_df, self.transit_time, df_type)
            return tuple([self.speed, minima_time_table_df, init_time])

    def execute_callback(self, result):
        print(f'-     {self.intro} Transit time ({self.speed}) {min_sec(perf_counter() - result[2])} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} Transit time ({self.speed}) process has raised an error: {result}', flush=True)

    def start_min_end(self, minima_table_df):
        minima_table_df = minima_table_df.dropna()
        minima_table_df.drop(columns=['midline'], inplace=True)
        minima_table_df.reset_index(inplace=True, drop=True)
        minima_table_df = minima_table_df.assign(start_time = pd.to_timedelta(minima_table_df['start_index'], unit='seconds') + self.date.index_basis(),
                                                 min_time = pd.to_timedelta(minima_table_df['min_index'], unit='seconds') + self.date.index_basis(),
                                                 end_time = pd.to_timedelta(minima_table_df['end_index'], unit='seconds') + self.date.index_basis())
        minima_table_df = minima_table_df.assign(start_rounded = minima_table_df['start_time'].apply(rounded_to_minutes),
                                                 min_rounded = minima_table_df['min_time'].apply(rounded_to_minutes),
                                                 end_rounded = minima_table_df['end_time'].apply(rounded_to_minutes))
        minima_table_df = minima_table_df.assign(window_time = (minima_table_df['end_time'] - minima_table_df['start_time']),
                                                 window_rounded = minima_table_df['end_rounded'] - minima_table_df['start_rounded'])
        minima_table_df['window_time'] = minima_table_df['window_time'].apply(hours_min)
        minima_table_df['window_rounded'] = minima_table_df['window_rounded'].apply(hours_min)
        return minima_table_df

    def minima_table(self, transit_array):
        tt_df = pd.DataFrame(columns=shared_columns+['tts', 'midline', 'start_index', 'min_index', 'end_index', 'plot'])
        tt_df[shared_columns[0]] = self.elapsed_time_df[[shared_columns[0]]]
        tt_df[shared_columns[1]] = pd.to_datetime(self.elapsed_time_df[shared_columns[1]])
        tt_df = tt_df[tt_df[shared_columns[1]].lt(self.end)]  # trim to lenth of transit array
        tt_df = tt_df.assign(tts = transit_array)
        if output_file_exists(self.savgol):
            tt_df['midline'] = read_df_hdf(self.savgol)
        else:
            tt_df['midline'] = savgol_filter(transit_array, 50000, 1)
            write_df_hdf(tt_df['midline'], self.savgol)
        min_segs = tt_df['tts'].lt(tt_df['midline']).to_list()  # list of True or False the same length as tt_df
        # self.seg_check(tt_df['min_segments'])
        clump = []
        for row, val in enumerate(min_segs):  # rows in tt_df, not the departure index
            if val:
                clump.append(row)  # list of the rows within the clump of True min_segs
            elif len(clump) > FIVE_HOURS_OF_TIMESTEPS:  # ignore clumps caused by small flucuations in midline or end conditions ( ~ 5 hour tide window )
                segment_df = tt_df[clump[0]:clump[-1]]  # subset of tt_df from first True to last True in clump
                seg_min_df = segment_df[segment_df['tts'] == segment_df.min()['tts']]  # segment rows equal to the minimum
                median_index = seg_min_df['departure_index'].median()  # median departure index of segment minima
                abs_diff = segment_df['departure_index'].sub(median_index).abs()  # abs of difference between actual index and median index
                min_index = segment_df.at[abs_diff[abs_diff == abs_diff.min()].index[0], 'departure_index']  # actual index closest to median index, may not be minimum tts
                offset = segment_df['tts'].min() + TIMESTEP_MARGIN  # minimum tss + margin for window
                if min_index != clump[0] and min_index != clump[-1]:  # ignore minima at edges
                    start_segment = segment_df[segment_df['departure_index'].le(min_index)]  # portion of segment from start to minimum
                    end_segment = segment_df[segment_df['departure_index'].ge(min_index)]  # portion of segment from minimum to end
                    start_row = start_segment[start_segment['tts'].le(offset)].index[0]
                    end_row = end_segment[end_segment['tts'].ge(offset)].index[0]
                    # if len(start_segment['tts'].le(offset)):
                    #     start_row = start_segment[start_segment['tts'].le(offset)].index[0]
                    # else:
                    #     start_row = start_segment[0]
                    # if len(end_segment['tts'].ge(offset)):
                    #     end_row = end_segment[end_segment['tts'].ge(offset)].index[0]
                    # else:
                    #     end_row = end_segment[-1]
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
        return tt_df

def seg_check(col):
        segs = col.to_list()
        checks = []
        tuples = []
        for i in range(0,len(segs)-1):
            if segs[i] != segs[i+1]: checks.append(i)
        for j in range(0,len(checks)-1):
            tuples.append(tuple([checks[j], checks[j+1], checks[j+1]-checks[j]]))
        tuples.sort(key = lambda x: x[2])
        print(tuples)

