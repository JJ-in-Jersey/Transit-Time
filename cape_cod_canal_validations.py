import pandas as pd
from tt_file_tools import file_tools as ft
from tt_geometry.geometry import time_to_degrees


def index_arc_df(frame):
    date_time_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    date_angle_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    date_name_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    columns = frame.columns.to_list()
    for i, row in frame.iterrows():
        date_time_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('time')])
        date_angle_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('angle')])
        date_name_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('graphic_name')])

    df = pd.DataFrame(columns=['date', 'name', 'time', 'angle'])
    for key in date_time_dict.keys():
        times = date_time_dict[key]
        angles = date_angle_dict[key]
        names = date_name_dict[key]
        for i in range(len(times)):
            df.loc[len(df.name)] = [key, names[i] + ' ' + str(i + 1), times[i], angles[i]]
    return df


class CapeCodCanalRailBridgeDataframe:
    # Eastbound slack water flood begins (L) + 3.5 hours
    # Westbound slack water ebb begins (H) + 2.5 to 2.75 hours

    def __init__(self, download_path, f_date, l_date):

        if ft.csv_npy_file_exists(download_path):
            frame = ft.read_df(download_path)

            east_df = frame[frame['HL'] == 'L']
            east_df.insert(len(east_df.columns), 'best_time', east_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=3.5))
            east_df['graphic_name'] = 'CCC east'

            west_df = frame[frame['HL'] == 'H']
            west_df.insert(len(west_df.columns), 'best_time', west_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=2.5))
            # west_df.insert(len(west_df.columns), 'west_best_time_2', west_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=2.75))
            west_df['graphic_name'] = 'CCC west'

            best_df = east_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)
            best_df = pd.concat([best_df, west_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)], ignore_index=True)

            best_df['date'] = best_df['best_time'].dt.date
            best_df['time'] = best_df['best_time'].dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.drop(['best_time'], axis=1)
            best_df = best_df[best_df['date'] >= f_date]
            best_df = best_df[best_df['date'] <= l_date]
            self.dataframe = index_arc_df(best_df)
        else:
            raise FileExistsError
