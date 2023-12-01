import pandas as pd

from tt_geometry.geometry import time_to_degrees
from tt_file_tools import file_tools as ft


def index_arc_df(frame, name):
    date_time_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    date_angle_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    columns = frame.columns.to_list()
    for i, row in frame.iterrows():
        date_time_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('time')])
        date_angle_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('angle')])

    df = pd.DataFrame(columns=['date', 'name', 'time', 'angle'])
    for key in date_time_dict.keys():
        times = date_time_dict[key]
        angles = date_angle_dict[key]
        for i in range(len(times)):
            df.loc[len(df.name)] = [key, name + ' ' + str(i + 1), times[i], angles[i]]
    return df


class BatteryValidationDataframe:

    def __init__(self, download_path, f_date, l_date):
        # northbound depart 4.5 hours after low water at the battery
        # southbound depart 4 hours after high water at the battery

        if ft.csv_npy_file_exists(download_path):
            frame = ft.read_df(download_path)
            south_df = frame[frame['HL'] == 'H']
            south_df.insert(len(south_df.columns), 'best_time', south_df['datetime'] + pd.Timedelta(hours=4))
            north_df = frame[frame['HL'] == 'L']
            north_df.insert(len(north_df.columns), 'best_time', north_df['datetime'] + pd.Timedelta(hours=4.5))
            best_df = north_df.drop(['date', 'time', 'HL', 'datetime'], axis=1)
            best_df = pd.concat([best_df, south_df.drop(['date', 'time', 'HL', 'datetime'], axis=1)], ignore_index=True)

            best_df['date'] = best_df['best_time'].dt.date
            best_df['time'] = best_df['best_time'].dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.drop(['best_time'], axis=1)
            # best_df = best_df[best_df['date'] >= f_date]
            # best_df = best_df[best_df['date'] <= l_date]
            best_df = best_df[f_date <= best_df['date'] <= l_date]
            self.dataframe = index_arc_df(best_df, 'Battery Line')
        else:
            raise FileExistsError
