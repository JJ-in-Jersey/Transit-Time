import pandas as pd
from scipy.signal import savgol_filter
from num2words import num2words
from pathlib import Path
import datetime

from tt_file_tools.file_tools import read_df, write_df, print_file_exists
from tt_geometry.geometry import Arc
from tt_job_manager.job_manager import Job
from tt_date_time_tools.date_time_tools import index_to_date, round_datetime, timedelta_hours_mins
from tt_globals.globals import Globals
from tt_gpx.gpx import Route

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)


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


class MinimaFrame:
    col_types = {
        'start_datetime': 'DT', 'min_datetime': 'DT', 'end_datetime': 'DT',
        'start_et': 'TD', 'min_et': 'TD', 'end_et': 'TD',
        'start_round_datetime': 'DT', 'min_round_datetime': 'DT', 'end_round_datetime': 'DT'
    }

    def __init__(self, transit_array, template_df, savgol_path, minima_path):

        self.frame = None
        if print_file_exists(minima_path):
            self.frame = read_df(minima_path)
            for column in self.frame.columns:
                if MinimaFrame.col_types[column] == 'DT':
                    self.frame[column] = pd.to_datetime(self.frame[column])
                elif MinimaFrame.col_types[column] == 'TD':
                    self.frame[column] = pd.to_timedelta(self.frame[column])
        else:
            noise_size = 100
            self.frame = template_df.copy(deep=True)
            self.frame = self.frame.assign(tts=transit_array)
            self.frame.drop(['date_time'], axis=1, inplace=True)  # only needed for debugging
            if savgol_path.exists():
                self.frame['midline'] = read_df(savgol_path)['midline']
            else:
                # noinspection PyUnresolvedReferences
                self.frame['midline'] = savgol_filter(transit_array, 50000, 1).round()
                write_df(self.frame, savgol_path)
            print_file_exists(savgol_path)

            self.frame['TF'] = self.frame['tts'].lt(
                self.frame['midline'])  # Above midline = False,  below midline = True
            self.frame = self.frame.drop(self.frame[self.frame['tts'] == self.frame['midline']].index).reset_index(
                drop=True)  # remove values that equal midline
            # noinspection PyUnresolvedReferences
            self.frame['block'] = (
                        self.frame['TF'] != self.frame['TF'].shift(1)).cumsum()  # index the blocks of True and False
            clump_lookup = {index: df for index, df in self.frame.groupby('block') if
                            df['TF'].any()}  # select only the True blocks
            clump_lookup = {index: df.drop(['TF', 'block', 'midline'], axis=1).reset_index() for index, df in
                            clump_lookup.items() if
                            len(df) > noise_size}  # remove the tiny blocks caused by noise at the inflections

            for index, clump in clump_lookup.items():
                # for this clump get the row with the minimum transit time
                # median of the departure indices among the tts minimum values
                median_departure_index = clump[clump['tts'] == clump.min()['tts']]['departure_index'].median()
                abs_diff = clump['departure_index'].sub(median_departure_index).abs()  # series of abs differences
                minimum_index = abs_diff[abs_diff == abs_diff.min()].index[0]  # row closest to median departure index
                minimum_row = clump.iloc[minimum_index]

                # find the upper and lower limits of the time window
                offset = int(
                    minimum_row['tts'] * Globals.TIME_WINDOW_SCALE_FACTOR)  # offset is transit time steps (tts)

                sr_range = clump[clump['departure_index'].lt(
                    minimum_row['departure_index'])]  # range of valid starting points less than minimum
                if len(sr_range[sr_range['tts'].gt(
                        offset)]):  # there is a valid starting point between the beginning and minimum
                    sr = sr_range[sr_range['tts'].gt(offset)].iloc[-1]  # pick the one closest to minimum
                else:
                    sr = clump.iloc[0]

                er_range = clump[clump['departure_index'].gt(
                    minimum_row['departure_index'])]  # range of valid ending points greater than minimum
                if len(er_range[
                           er_range['tts'].gt(offset)]):  # there is a valid starting point between the minimum and end
                    er = er_range[er_range['tts'].gt(offset)].iloc[0]  # pick the one closest to minimum
                else:
                    er = clump.iloc[-1]

                start_row = sr
                end_row = er

                self.frame.at[minimum_row['index'], 'start_datetime'] = index_to_date(
                    start_row['departure_index'])  # datetime.timestamp ('<M8[ns]') (datetime64[ns])
                self.frame.at[minimum_row['index'], 'min_datetime'] = index_to_date(minimum_row['departure_index'])
                self.frame.at[minimum_row['index'], 'end_datetime'] = index_to_date(end_row['departure_index'])
                self.frame.at[minimum_row['index'], 'start_et'] = datetime.timedelta(
                    seconds=int(start_row['tts']) * Globals.TIMESTEP)  # datetime.timedelta (timedelta64[us])
                self.frame.at[minimum_row['index'], 'min_et'] = datetime.timedelta(
                    seconds=int(minimum_row['tts']) * Globals.TIMESTEP)
                self.frame.at[minimum_row['index'], 'end_et'] = datetime.timedelta(
                    seconds=int(end_row['tts']) * Globals.TIMESTEP)

            self.frame = self.frame.dropna(axis=0).sort_index().reset_index(drop=True)  # remove lines with NA
            self.frame['start_round_datetime'] = self.frame['start_datetime'].apply(round_datetime)  # datetime.timestamp ('<M8[ns]') (datetime64[ns])
            self.frame['min_round_datetime'] = self.frame['min_datetime'].apply(round_datetime)
            self.frame['end_round_datetime'] = self.frame['end_datetime'].apply(round_datetime)

            self.frame.drop(['tts', 'departure_index', 'midline', 'block', 'TF'], axis=1, inplace=True)
            write_df(self.frame, minima_path)
            print_file_exists(minima_path)


def index_arc_df(frame):
    output_frame = pd.DataFrame(columns=['idx'] + frame.columns.to_list())

    date_arr_dict = {key: [] for key in sorted(list(set(frame['date'])))}

    for i, row in frame.iterrows():
        date_arr_dict[row['date']].append(row)

    for key in date_arr_dict.keys():
        for i in range(len(date_arr_dict[key])):
            output_frame.loc[len(output_frame)] = [i + 1] + date_arr_dict[key][i].tolist()
        if len(date_arr_dict[key]) == 2:
            last_row_dict = output_frame.loc[len(output_frame) - 1].to_dict()
            new_dict = {key: None for key in last_row_dict}
            new_dict.update({'idx': 3, 'date': last_row_dict['date']})
            output_frame.loc[len(output_frame)] = new_dict

    return output_frame


def create_arcs(f_day, l_day, minima_frame):
    arcs = [Arc(row.to_dict()) for i, row in minima_frame.iterrows()]
    next_day_arcs = [arc.next_day_arc for arc in arcs if arc.next_day_arc]
    all_arcs = arcs + next_day_arcs
    all_good_arcs = [arc for arc in all_arcs if not arc.zero_angle]

    arcs_df = pd.DataFrame(columns=Arc.columns)
    for d in [a.arc_dict for a in all_good_arcs]:
        arcs_df.loc[len(arcs_df)] = d

    # noinspection PyTypeChecker
    arcs_df.insert(loc=0, column='date', value=None)
    arcs_df['date'] = arcs_df['start_datetime'].dt.date

    arcs_df.sort_values(by=['start_datetime'], inplace=True)
    arcs_df = index_arc_df(arcs_df)
    arcs_df = arcs_df[arcs_df['date'] <= l_day.date()]
    arcs_df = arcs_df[arcs_df['date'] >= f_day.date()]
    arcs_df['start_et'] = arcs_df['start_et'].apply(timedelta_hours_mins)
    arcs_df['min_et'] = arcs_df['min_et'].apply(timedelta_hours_mins)
    arcs_df['end_et'] = arcs_df['end_et'].apply(timedelta_hours_mins)

    return arcs_df


class TransitTimeDataframe:

    def __init__(self, speed, template_df: pd.DataFrame, et_file: Path, tt_folder, f_day, l_day):

        self.transit_time_path = None
        self.rounded_transit_time_path = None
        folder = tt_folder.joinpath(num2words(speed))
        transit_times_path = folder.joinpath('transit_times.csv')
        rounded_transit_times_path = folder.joinpath('rounded_transit_times.csv')

        timesteps_path = folder.joinpath('timesteps.csv')
        savgol_path = folder.joinpath('savgol.csv')
        minima_path = folder.joinpath('minima.csv')
        row_range = range(len(template_df))

        if print_file_exists(transit_times_path):
            self.transit_time_path = transit_times_path
            if print_file_exists(rounded_transit_times_path):
                self.rounded_transit_time_path = rounded_transit_times_path
            else:
                frame = read_df(transit_times_path)
                rounded_frame = frame.drop(['start_datetime', 'min_datetime', 'end_datetime',
                                            'start_et', 'min_et', 'end_et',
                                            'start_angle', 'min_angle', 'end_angle'], axis=1, inplace=True)
                self.rounded_transit_time_path = write_df(rounded_frame, rounded_transit_times_path)
        else:
            et_df = read_df(et_file)
            if print_file_exists(timesteps_path):
                transit_timesteps_arr = list(read_df(timesteps_path)['0'].to_numpy())
            else:
                col_list = et_df.columns.to_list()
                col_list.remove('departure_index')
                col_list.remove('date_time')

                transit_timesteps_arr = [total_transit_time(row, et_df, col_list) for row in row_range]
                write_df(pd.concat([template_df, pd.DataFrame(transit_timesteps_arr)], axis=1), timesteps_path)
                print_file_exists(timesteps_path)

            minima_df = MinimaFrame(transit_timesteps_arr, template_df, savgol_path, minima_path).frame

            frame = create_arcs(f_day, l_day, minima_df)
            if frame.duplicated().any():
                print(f'Duplicates in {speed}')
            frame['speed'] = speed

            self.transit_time_path = write_df(frame, transit_times_path)
            print_file_exists(self.transit_time_path)

            rounded_frame = frame.drop(['start_datetime', 'min_datetime', 'end_datetime',
                                        'start_angle', 'min_angle', 'end_angle', 'arc_angle'], axis=1)

            self.rounded_transit_time_path = write_df(rounded_frame, rounded_transit_times_path)
            print_file_exists(self.rounded_transit_time_path)


class TransitTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()

    def execute_callback(self, result): return super().execute_callback(result)

    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, speed, et_frame: pd.DataFrame, tt_folder: Path):
        job_name = 'transit_time' + ' ' + str(speed)
        result_key = speed
        arguments = tuple(
            [speed, Globals.TEMPLATE_TRANSIT_TIME_DATAFRAME, et_frame, tt_folder, Globals.FIRST_DAY, Globals.LAST_DAY])
        super().__init__(job_name, result_key, TransitTimeDataframe, arguments)


def transit_time_processing(job_manager, route: Route):
    print(f'\nCalculating transit timesteps')
    keys = [job_manager.put(TransitTimeJob(speed, route.elapsed_time_csv_to_speed[speed], Globals.TRANSIT_TIMES_FOLDER))
            for speed in Globals.BOAT_SPEEDS]
    # for speed in Globals.BOAT_SPEEDS:
    #     job = TransitTimeJob(speed, route.elapsed_time_csv_to_speed[speed], Globals.TRANSIT_TIMES_FOLDER)
    #     result = job.execute()
    job_manager.wait()

    for key in keys:
        print(f'Posting transit times paths to route for speed {key}')
        result = job_manager.get(key)
        route.transit_time_csv_to_speed[key] = result.transit_time_path
        route.rounded_transit_time_csv_to_speed[key] = result.rounded_transit_time_path

    print(f'\nAggregating transit times', flush=True)

    aggregate_transit_time_df = pd.concat([read_df(route.rounded_transit_time_csv_to_speed[key]) for key in
                                           route.rounded_transit_time_csv_to_speed.keys()])
    transit_time_df = (aggregate_transit_time_df.
        drop(['idx', 'start_round_angle', 'min_round_angle', 'end_round_angle', 'arc_round_angle', 'start_et', 'min_et', 'end_et'], axis=1)
        .rename(columns={'start_round_datetime': 'start', 'min_round_datetime': 'best', 'end_round_datetime': 'end'})
        .set_index(['date', 'speed']).add_prefix(route.location_code + " ").reset_index())
    transit_time_df = transit_time_df.fillna("-")
    arc_df = (aggregate_transit_time_df
              .drop(['start_round_datetime', 'min_round_datetime', 'end_round_datetime', 'arc_round_angle'], axis=1)
              .rename(columns={'start_round_angle': 'start', 'min_round_angle': 'best', 'end_round_angle': 'end',
                               'min_et': 'best_et'}))

    row_counts = arc_df.count(axis=1)
    arc_df = arc_df[row_counts > 3]

    print_file_exists(
        write_df(transit_time_df, Globals.TRANSIT_TIMES_FOLDER.joinpath(route.location_code + '_transit_times.csv')))
    print_file_exists(write_df(arc_df, Globals.TRANSIT_TIMES_FOLDER.joinpath(route.location_code + '_arcs.csv')))
