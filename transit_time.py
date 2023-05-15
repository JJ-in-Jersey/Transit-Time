import pandas as pd
from scipy.signal import savgol_filter
from time import perf_counter
from num2words import num2words

from project_globals import TIMESTEP, TIMESTEP_MARGIN, FIVE_HOURS_OF_TIMESTEPS, file_exists, hours_mins, mins_secs
from ReadWrite import ReadWrite as rw
from MemoryHelper import MemoryHelper as mh

def total_transit_time(init_row, d_frame, cols):
    row = init_row
    tt = 0
    for col in cols:
        val = d_frame.at[row, col]
        tt += val
        row += val
    return tt

class TransitTimeMinimaJob:

    def __init__(self, env, cy, route, speed):
        self.speed = speed
        self._first_day_index = cy.first_day_index()
        self._last_day_index = cy.last_day_index()
        self._start_index = cy.transit_start_index()
        self._end_index = cy.transit_end_index()
        self._transit_range = cy.transit_range()
        self._elapsed_times_df = route.elapsed_time_lookup(speed)
        self._elapsed_time_table = env.transit_time_folder().joinpath('et_' + str(speed))  # elapsed times in table, sorted by speed
        self.speed_folder = env.speed_folder(num2words(speed))
        self._plotting_table = self.speed_folder.joinpath('tt_' + str(speed) + '_plotting_table')  # output of minima_table, npy, can be used for plotting/checking
        self.savgol = self.speed_folder.joinpath('tt_' + str(speed) + 'savgol')  # savgol column
        self._transit_timesteps = self.speed_folder.joinpath('tt_' + str(speed) + '_timesteps')  # transit times column
        self._transit_time = env.transit_time_folder().joinpath('transit_time_' + str(speed))  # final results tabl

    def execute(self):
        init_time = perf_counter()
        if file_exists(self._transit_time):
            print(f'+     Transit time ({self.speed}) reading data file', flush=True)
            tt_minima_df = rw.read_df(self._transit_time)
            return tuple([self.speed, tt_minima_df, init_time])

        print(f'+     Transit time ({self.speed})', flush=True)
        if file_exists(self._transit_timesteps):
            transit_timesteps = rw.read_arr(self._transit_timesteps)
        else:
            row_range = range(len(self._transit_range))
            transit_timesteps = [total_transit_time(row, self._elapsed_times_df, self._elapsed_times_df.columns.to_list()) for row in row_range]  # in timesteps
            # transit_timesteps = []
            # for row in row_range:
            #     result = total_transit_time(row, self._elapsed_times_df, self._elapsed_times_df.columns.to_list())
            #     transit_timesteps.append(result)
            rw.write_list(transit_timesteps, self._transit_timesteps)

        if file_exists(self._plotting_table):
            minima_table_df = rw.read_df(self._plotting_table)
        else:
            minima_table_df = self.minima_table(transit_timesteps)
            rw.write_df(minima_table_df, self._plotting_table)
        minima_table_df.drop(['midline', 'plot'], axis=1, inplace=True)

        minima_time_table_df = self.start_min_end(minima_table_df)
        minima_time_table_df.drop(['min_index'], axis=1, inplace=True)

        final_df = self.trim_to_year(minima_time_table_df)
        rw.write_df(final_df, self._transit_time)
        return tuple([self.speed, minima_time_table_df, init_time])

    def execute_callback(self, result):
        print(f'-     Transit time ({self.speed}) {mins_secs(perf_counter() - result[2])} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     Transit time ({self.speed}) process has raised an error: {result}', flush=True)

    # noinspection PyMethodMayBeStatic
    def start_min_end(self, minima_df):
        minima_df.dropna(axis=0, inplace=True)
        minima_df['transit_time'] = (minima_df['tts']*TIMESTEP).apply(lambda x: hours_mins(x))
        minima_df['transit_time_new'] = pd.to_datetime(minima_df['tts']*TIMESTEP, unit='s').round('min')
        minima_df.drop(['tts'], axis=1, inplace=True)
        minima_df['start_time'] = pd.to_datetime(minima_df['start_index'], unit='s').round('min')
        minima_df['min_time'] = pd.to_datetime(minima_df['min_index'], unit='s').round('min')
        minima_df['end_time'] = pd.to_datetime(minima_df['end_index'], unit='s').round('min')
        minima_df['start_rounded'] = pd.to_datetime(minima_df['start_index'], unit='s').round('min')
        minima_df['min_rounded'] = pd.to_datetime(minima_df['min_index'], unit='s').round('min')
        minima_df['end_rounded'] = pd.to_datetime(minima_df['end_index'], unit='s').round('min')
        minima_df['window_time'] = pd.to_timedelta(minima_df['end_index'] - minima_df['start_index'], unit='s').round('min')
        minima_df['window_rounded'] = minima_df['window_time']
        minima_df = mh.shrink_dataframe(minima_df)
        return minima_df

    def minima_table(self, transit_array):
        tt_df = pd.DataFrame(columns=['departure_index', 'tts', 'midline', 'start_index', 'min_index', 'end_index', 'plot'])
        tt_df['departure_index'] = self._transit_range
        tt_df['plot'] = 0
        tt_df = tt_df.assign(tts=transit_array)
        if file_exists(self.savgol):
            tt_df['midline'] = rw.read_df(self.savgol)
        else:
            tt_df['midline'] = savgol_filter(transit_array, 50000, 1)
            rw.write_df(tt_df['midline'], self.savgol)
        min_segs = tt_df['tts'].lt(tt_df['midline']).to_list()  # list of True or False the same length as tt_df
        clump = []
        for row, val in enumerate(min_segs):  # rows in tt_df, not the departure index
            if val:
                clump.append(row)  # list of the rows within the clump of True min_segs
            elif len(clump) > FIVE_HOURS_OF_TIMESTEPS:  # ignore clumps caused by small fluctuations in midline or end conditions ( ~ 5-hour tide window )
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
                    start_index = start_segment.at[start_row, 'departure_index']
                    end_index = end_segment.at[end_row, 'departure_index']
                    tt_df_start_row = tt_df[tt_df['departure_index'] == start_index].index[0]
                    tt_df_min_row = tt_df[tt_df['departure_index'] == min_index].index[0]
                    tt_df_end_row = tt_df[tt_df['departure_index'] == end_index].index[0]
                    tt_df.at[tt_df_min_row, 'start_index'] = start_index
                    tt_df.at[tt_df_min_row, 'min_index'] = min_index
                    tt_df.at[tt_df_min_row, 'end_index'] = end_index
                    tt_df.at[tt_df_start_row, 'plot'] = max(transit_array)
                    tt_df.at[tt_df_min_row, 'plot'] = min(transit_array)
                    tt_df.at[tt_df_end_row, 'plot'] = max(transit_array)
                clump = []
        return tt_df

    def trim_to_year(self, final_df):
        final_df['departure_time'] = pd.to_datetime(final_df['departure_index'], unit='s').round('min')
        final_df = final_df[final_df['end_index'] > self._first_day_index]
        final_df = final_df[final_df['start_index'] < self._last_day_index]
        final_df.drop(['departure_index', 'start_index', 'end_index'], axis=1, inplace=True)
        return final_df
