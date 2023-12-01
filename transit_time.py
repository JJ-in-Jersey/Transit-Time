import pandas as pd
from scipy.signal import savgol_filter
from num2words import num2words

from tt_file_tools import file_tools as ft
from tt_geometry.geometry import Arc, RoundedArc, FractionalArcStartDay, FractionalArcEndDay
from tt_job_manager.job_manager import Job

from project_globals import TIMESTEP, TIMESTEP_MARGIN, FIVE_HOURS_OF_TIMESTEPS


def none_row(row, df):
    for c in range(len(df.columns)):
        df.iloc[row, c] = None


def total_transit_time(init_row, d_frame, cols):
    row = init_row
    tt = 0
    for col in cols:
        val = d_frame.at[row, col]
        tt += val
        row += val
    return tt


def minima_table(transit_array, tt_range, savgol_path):
    tt_df = pd.DataFrame()
    tt_df['departure_index'] = tt_range
    tt_df['plot'] = 0
    tt_df = tt_df.assign(tts=transit_array)
    if ft.csv_npy_file_exists(savgol_path):
        tt_df['midline'] = ft.read_arr(savgol_path)
    else:
        tt_df['midline'] = savgol_filter(transit_array, 50000, 1)
        ft.write_arr(tt_df['midline'], savgol_path)
    min_segs = tt_df['tts'].lt(tt_df['midline']).to_list()  # list of True or False the same length as tt_df
    clump = []
    for row, val in enumerate(min_segs):  # rows in tt_df, not the departure index
        if val:
            clump.append(row)  # list of the rows within the clump of True min_segs
        elif len(clump) > FIVE_HOURS_OF_TIMESTEPS:  # ignore clumps caused by small fluctuations in midline or end conditions ( ~ 5-hour tide window )
            segment_df = tt_df[clump[0]:clump[-1]]  # subset of tt_df from first True to last True in clump
            seg_min_df = segment_df[segment_df['tts'] == segment_df.min()['tts']]  # segment rows equal to the minimum
            median_index = seg_min_df['departure_index'].median()  # median departure index of segment minima rows
            abs_diff = segment_df['departure_index'].sub(median_index).abs()  # absolute value of difference between actual index and median index
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
                tt_df.at[tt_df_min_row, 'start_index'] = int(start_index)
                tt_df.at[tt_df_min_row, 'min_index'] = int(min_index)
                tt_df.at[tt_df_min_row, 'end_index'] = int(end_index)
                tt_df.at[tt_df_start_row, 'plot'] = max(transit_array)
                tt_df.at[tt_df_min_row, 'plot'] = min(transit_array)
                tt_df.at[tt_df_end_row, 'plot'] = max(transit_array)
            clump = []
    tt_df['transit_time'] = pd.to_timedelta(tt_df['tts']*TIMESTEP, unit='s').round('min')
    return tt_df


def create_arcs(minima_df, shape_name, f_date, l_date):

    arc_frame = minima_df[['tts', 'start_index', 'min_index', 'end_index']]
    Arc.name = shape_name

    arcs = [RoundedArc(*row.values.tolist()) for i, row in arc_frame.iterrows()]
    fractured_arc_list = list(filter(lambda n: n.fractured, arcs))
    arc_list = list(filter(lambda n: not n.fractured, arcs))

    for arc in fractured_arc_list:
        arc_list.append(FractionalArcStartDay(*arc.fractional_arc_args()))
        arc_list.append(FractionalArcEndDay(*arc.fractional_arc_args()))

    arc_df = pd.DataFrame([arc.df_angles() for arc in arc_list])
    arc_df.columns = Arc.columns
    arc_df.sort_values(['date'], ignore_index=True, inplace=True)

    # add shape counts to shapes, last row doesn't matter, it will be trimmed away
    count = 1
    for row in arc_df.index[:-1]:
        arc_df.loc[row, 'name'] = arc_df.loc[row, 'name'] + ' ' + str(count)
        if arc_df.loc[row, 'date'] == arc_df.loc[row + 1, 'date']:
            count += 1
        else:
            count = 1

    arc_df = arc_df[arc_df['date'] >= f_date]
    arc_df = arc_df[arc_df['date'] <= l_date]

    return arc_df


class ArcsDataframe:

    def __init__(self, speed, year, f_date, l_date, tt_range, et_df, tt_folder):

        self.dataframe = None
        sign = '+' if speed / abs(speed) > 0 else '-'
        boat_speed = sign + str(abs(speed))
        shape_name = 'arc ' + boat_speed
        file_header = str(year) + '_' + boat_speed
        speed_folder = tt_folder.joinpath(num2words(speed))

        transit_timesteps_path = speed_folder.joinpath(file_header + '_timesteps')
        savgol_path = speed_folder.joinpath(file_header + '_savgol_data')
        minima_path = speed_folder.joinpath(file_header + '_minima_data')
        arcs_path = speed_folder.joinpath(file_header + '_arcs')

        if ft.csv_npy_file_exists(arcs_path):
            self.dataframe = ft.read_df(arcs_path)
        else:

            if ft.csv_npy_file_exists(transit_timesteps_path):
                transit_timesteps_arr = ft.read_arr(transit_timesteps_path)
            else:
                row_range = range(len(tt_range))
                transit_timesteps_arr = [total_transit_time(row, et_df, et_df.columns.to_list()) for row in row_range]
                ft.write_arr(transit_timesteps_arr, transit_timesteps_path)

            if ft.csv_npy_file_exists(minima_path):
                minima_df = ft.read_df(minima_path)
            else:
                minima_df = minima_table(transit_timesteps_arr, tt_range, savgol_path)  # call minima_table
                ft.write_df(minima_df, minima_path)

            if ft.csv_npy_file_exists(arcs_path):
                self.dataframe = ft.read_df(arcs_path)
            else:
                minima_df = minima_df.dropna(axis=0).sort_index().reset_index(drop=True)
                minima_df = minima_df.astype({'departure_index': int, 'start_index': int, 'min_index': int, 'end_index': int})
                self.dataframe = create_arcs(minima_df, shape_name, f_date, l_date)
                ft.write_df(self.dataframe, arcs_path)


class TransitTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, speed, year, f_date, l_date, t_range, et_df, tt_folder):
        job_name = 'transit_time' + ' ' + str(speed)
        result_key = speed
        arguments = tuple([speed, year, f_date, l_date, t_range, et_df, tt_folder])
        super().__init__(job_name, result_key, ArcsDataframe, arguments)
