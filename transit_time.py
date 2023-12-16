import pandas as pd
from scipy.signal import savgol_filter
from num2words import num2words

from tt_file_tools import file_tools as ft
from tt_geometry.geometry import Arc
from tt_job_manager.job_manager import Job
from tt_date_time_tools import date_time_tools as dtt

from project_globals import TIMESTEP, TIMESTEP_MARGIN, FIVE_HOURS_OF_TIMESTEPS, BOAT_SPEEDS


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
    tt_df['departure_index'].astype('int')
    tt_df['plot'] = 0
    tt_df = tt_df.assign(tts=transit_array)
    if savgol_path.exists():
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
                start_row = start_segment[start_segment['tts'].le(offset)].index[0]
                start_index = start_segment.at[start_row, 'departure_index']
                end_segment = segment_df[segment_df['departure_index'].ge(min_index)]  # portion of segment from minimum to end
                end_row = end_segment[end_segment['tts'].ge(offset)].index[0]
                end_index = end_segment.at[end_row, 'departure_index']
                tt_df_start_row = tt_df[tt_df['departure_index'] == start_index].index[0]
                tt_df_min_row = tt_df[tt_df['departure_index'] == min_index].index[0]
                tt_df_end_row = tt_df[tt_df['departure_index'] == end_index].index[0]
                tt_df.at[tt_df_min_row, 'start_index'] = start_index
                tt_df.at[tt_df_min_row, 'start_datetime'] = dtt.datetime(start_index)
                tt_df.at[tt_df_min_row, 'min_index'] = min_index
                tt_df.at[tt_df_min_row, 'min_datetime'] = dtt.datetime(min_index)
                tt_df.at[tt_df_min_row, 'end_index'] = end_index
                tt_df.at[tt_df_min_row, 'end_datetime'] = dtt.datetime(end_index)
                tt_df.at[tt_df_start_row, 'plot'] = max(transit_array)
                tt_df.at[tt_df_min_row, 'plot'] = min(transit_array)
                tt_df.at[tt_df_end_row, 'plot'] = max(transit_array)
            clump = []
    tt_df['transit_time'] = pd.to_timedelta(tt_df['tts']*TIMESTEP, unit='s').round('min')
    tt_df = tt_df.dropna(axis=0).sort_index().reset_index(drop=True)  # make minima_df easier to write
    tt_df.drop(['tts', 'departure_index', 'plot', 'midline'], axis=1, inplace=True)  # make minima_df easier to write
    return tt_df


def index_arc_df(frame):
    # columns = ['name', 'start_date', 'start_time', 'start_angle', 'min_time', 'min_angle', 'end_time', 'end_angle', 'elapsed_time']

    date_keys = [key for key in sorted(list(set(frame['start_date'])))]
    name_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    start_time_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    start_angle_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    min_time_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    min_angle_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    end_time_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    end_angle_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    elapsed_time_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    columns = frame.columns.to_list()

    for i, row in frame.iterrows():
        name_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('name')])
        start_time_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('start_time')])
        start_angle_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('start_angle')])
        min_time_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('min_time')])
        min_angle_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('min_angle')])
        end_time_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('end_time')])
        end_angle_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('end_angle')])
        elapsed_time_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('elapsed_time')])

    arc_frame = pd.DataFrame(columns=Arc.columns)
    for date in date_keys:
        names = name_dict[date]
        start_times = start_time_dict[date]
        start_angles = start_angle_dict[date]
        min_times = min_time_dict[date]
        min_angles = min_angle_dict[date]
        end_times = end_time_dict[date]
        end_angels = end_angle_dict[date]
        elapsed_times = elapsed_time_dict[date]

        for i in range(len(names)):
            arc_frame.loc[len(arc_frame)] = [names[i] + ' ' + str(i + 1), date,
                                             start_times[i], start_angles[i], min_times[i], min_angles[i],
                                             end_times[i], end_angels[i], elapsed_times[i]]
    return arc_frame


def create_arcs(arc_frame, shape_name, f_date, l_date):

    # arc_frame = minima_df.drop(['departure_index', 'plot', 'tts', 'midline'], axis=1)
    Arc.name = shape_name

    arcs = [Arc(*row.values.tolist()) for i, row in arc_frame.iterrows()]

    whole_arc_rows = [a.info() for a in filter(lambda a: not a.fractured, arcs)]
    start_day_rows = [a.start_day_arc.info() for a in filter(lambda a: a.fractured, arcs)]
    end_day_rows = [a.end_day_arc.info() for a in filter(lambda a: a.fractured, arcs)]

    arcs_df = pd.DataFrame(whole_arc_rows + start_day_rows + end_day_rows)
    arcs_df.columns = Arc.columns
    arcs_df = index_arc_df(arcs_df)
    arcs_df = arcs_df[arcs_df['start_date'] >= f_date]
    arcs_df = arcs_df[arcs_df['start_date'] <= l_date]

    return arcs_df


class ArcsDataframe:

    def __init__(self, speed, year, f_date, l_date, tt_range, et_df, tt_folder):

        self.frame = None
        sign = '+' if speed / abs(speed) > 0 else '-'
        boat_speed = sign + str(abs(speed))
        shape_name = 'arc ' + boat_speed
        file_header = str(year) + '_' + boat_speed
        speed_folder = tt_folder.joinpath(num2words(speed))

        transit_timesteps_path = speed_folder.joinpath(file_header + '_timesteps')
        savgol_path = speed_folder.joinpath(file_header + '_savgol')
        minima_path = speed_folder.joinpath(file_header + '_minima')
        arcs_path = speed_folder.joinpath(file_header + '_arcs')

        if arcs_path.exists():
            self.frame = ft.read_df(arcs_path)
        else:
            if transit_timesteps_path.exists():
                transit_timesteps_arr = ft.read_arr(transit_timesteps_path)
            else:
                row_range = range(len(tt_range))
                transit_timesteps_arr = [total_transit_time(row, et_df, et_df.columns.to_list()) for row in row_range]
                ft.write_arr(transit_timesteps_arr, transit_timesteps_path)

            if minima_path.exists():
                minima_df = ft.read_df(minima_path)
            else:
                minima_df = minima_table(transit_timesteps_arr, tt_range, savgol_path)
                ft.write_df(minima_df, minima_path)

            self.frame = create_arcs(minima_df, shape_name, f_date, l_date)
            ft.write_df(self.frame, arcs_path)


class TransitTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, speed, year, f_date, l_date, t_range, et_df, tt_folder):
        job_name = 'transit_time' + ' ' + str(speed)
        result_key = speed
        arguments = tuple([speed, year, f_date, l_date, t_range, et_df, tt_folder])
        super().__init__(job_name, result_key, ArcsDataframe, arguments)


def check_arcs(env, year):

    edge_processing_required = False
    for speed in BOAT_SPEEDS:

        sign = '+' if speed / abs(speed) > 0 else '-'
        boat_speed = sign + str(abs(speed))

        speed_folder = env.transit_time_folder.joinpath(num2words(boat_speed))
        arcs_path = speed_folder.joinpath(str(year) + '_' + str(boat_speed) + '_arcs')

        if not arcs_path.exists():
            edge_processing_required = True

    return edge_processing_required
