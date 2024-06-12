import pandas as pd
from scipy.signal import savgol_filter
from num2words import num2words
from pathlib import Path
import numpy as np

from tt_file_tools.file_tools import read_df, write_df
from tt_geometry.geometry import Arc
from tt_job_manager.job_manager import Job
from tt_date_time_tools.date_time_tools import index_to_date, round_time
from tt_globals.globals import Globals


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
    noise_size = 100
    min_df = pd.DataFrame()
    min_df['departure_index'] = tt_range
    min_df['departure_index'].astype('int')
    min_df = min_df.assign(tts=transit_array)
    if savgol_path.exists():
        min_df['midline'] = np.load(savgol_path)
    else:
        min_df['midline'] = savgol_filter(transit_array, 50000, 1)
        np.save(savgol_path, min_df['midline'])

    min_df['TF'] = min_df['tts'].lt(min_df['midline'])  # Above midline = False,  below midline = True
    min_df['block'] = (min_df['TF'] != min_df['TF'].shift(1)).cumsum()  # index the blocks of True and False
    clump_lookup = {index: df for index, df in min_df.groupby('block') if df['TF'].any()}  # select only the True blocks
    clump_lookup = {index: df.drop(['TF', 'block', 'midline'], axis=1).reset_index() for index, df in clump_lookup.items() if len(df) > noise_size}  # remove the tiny blocks caused by noise at the inflections

    for index, clump in clump_lookup.items():
        median_departure_index = clump[clump['tts'] == clump.min()['tts']]['departure_index'].median()  # median of the departure indices among the tts minimum values
        abs_diff = clump['departure_index'].sub(median_departure_index).abs()  # series of abs differences
        minimum_index = abs_diff[abs_diff == abs_diff.min()].index[0]  # row closest to median departure index
        minimum_tts = clump.at[minimum_index, 'tts']

        minimum_row = clump.iloc[minimum_index]
        minimum_departure = minimum_row['departure_index']  # departure index at row closest to median departure index

        offset = minimum_tts + Globals.TIMESTEP_MARGIN
        start_row = clump.iloc[0] if clump.iloc[0]['tts'] < offset else clump[clump['departure_index'].le(minimum_departure) & clump['tts'].ge(offset)].iloc[-1]
        start_departure = start_row['departure_index']
        end_row = clump.iloc[-1] if clump.iloc[-1]['tts'] < offset else clump[clump['departure_index'].ge(minimum_departure) & clump['tts'].ge(offset)].iloc[0]
        end_departure = end_row['departure_index']

        min_df.at[minimum_row['index'], 'start_index'] = start_departure
        min_df.at[minimum_row['index'], 'start_datetime'] = index_to_date(start_departure)
        min_df.at[minimum_row['index'], 'min_index'] = minimum_departure
        min_df.at[minimum_row['index'], 'min_datetime'] = index_to_date(minimum_departure)
        min_df.at[minimum_row['index'], 'end_index'] = end_departure
        min_df.at[minimum_row['index'], 'end_datetime'] = index_to_date(end_departure)

    min_df['transit_time'] = pd.to_timedelta(min_df['tts']*Globals.TIMESTEP, unit='s').round('min')

    min_df = min_df.dropna(axis=0).sort_index().reset_index(drop=True)  # remove lines with NA
    min_df.drop(['tts', 'departure_index', 'midline', 'block', 'TF'], axis=1, inplace=True)  # delete unwanted columns
    min_df['round_start'] = min_df['start_datetime'].apply(round_time)
    min_df['round_min'] = min_df['min_datetime'].apply(round_time)
    min_df['round_end'] = min_df['end_datetime'].apply(round_time)
    return min_df


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
    # arc_frame.insert(loc=1, column='index', value=None)
    arc_frame.insert(loc=1, column='index', value='NaN')
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
            arc_frame.loc[len(arc_frame)] = [names[i] + ' ' + str(i + 1), i+1, date,
                                             start_times[i], start_angles[i], min_times[i], min_angles[i],
                                             end_times[i], end_angels[i], elapsed_times[i]]
    return arc_frame


def create_arcs(f_day, l_day, arc_frame, shape_name):

    Arc.name = shape_name

    arcs = [Arc(*row.values.tolist()) for i, row in arc_frame.iterrows()]

    whole_arc_rows = [a.info() for a in filter(lambda a: not a.fractured, arcs)]
    start_day_rows = [a.start_day_arc.info() for a in filter(lambda a: a.fractured and a.start_day_arc, arcs)]
    end_day_rows = [a.end_day_arc.info() for a in filter(lambda a: a.fractured and a.end_day_arc, arcs)]

    arcs_df = pd.DataFrame(whole_arc_rows + start_day_rows + end_day_rows)
    arcs_df.columns = Arc.columns
    arcs_df.sort_values(by=['start_date', 'start_time'], inplace=True)
    arcs_df = index_arc_df(arcs_df)
    arcs_df = arcs_df[arcs_df['start_date'] <= l_day.date()]
    arcs_df = arcs_df[arcs_df['start_date'] >= f_day.date()]

    return arcs_df


class ArcsDataframe:

    def __init__(self, speed, tt_range, et_file, tt_folder, f_day, l_day):

        et_df = read_df(et_file)

        self.frame = None
        sign = '+' if speed / abs(speed) > 0 else '-'
        boat_speed = sign + str(abs(speed))
        shape_name = 'arc ' + boat_speed
        file_header = str(f_day.date().year) + '_' + boat_speed
        speed_folder = tt_folder.joinpath(num2words(speed))

        transit_timesteps_path = speed_folder.joinpath(file_header + '_timesteps.npy')
        savgol_path = speed_folder.joinpath(file_header + '_savgol.npy')
        minima_path = speed_folder.joinpath(file_header + '_minima.csv')
        self.filepath = speed_folder.joinpath(file_header + '_arcs.csv')

        if not self.filepath.exists():
            if transit_timesteps_path.exists():
                transit_timesteps_arr = np.load(transit_timesteps_path)
            else:
                row_range = range(len(tt_range))
                transit_timesteps_arr = [total_transit_time(row, et_df, et_df.columns.to_list()) for row in row_range]
                np.save(transit_timesteps_path, transit_timesteps_arr)

            if minima_path.exists():
                minima_df = read_df(minima_path)
            else:
                minima_df = minima_table(transit_timesteps_arr, tt_range, savgol_path)
                write_df(minima_df, minima_path)

            frame = create_arcs(f_day, l_day, minima_df, shape_name)
            frame['speed'] = speed
            write_df(frame, self.filepath)


class TransitTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, speed, et_file: Path, tt_folder: Path):

        job_name = 'transit_time' + ' ' + str(speed)
        result_key = speed
        arguments = tuple([speed, Globals.TRANSIT_TIME_INDEX_RANGE, et_file, tt_folder, Globals.FIRST_DAY, Globals.LAST_DAY])
        super().__init__(job_name, result_key, ArcsDataframe, arguments)
