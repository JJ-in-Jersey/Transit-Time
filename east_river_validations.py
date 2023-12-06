import pandas as pd
from tt_file_tools import file_tools as ft
from tt_geometry.geometry import time_to_degrees
from validations import index_arc_df


class HellGateValidationDataframe:

    first_slack_lookup = {'flood_slack': 'ebb_begins', 'slack_flood': 'flood_begins', 'slack_ebb': 'ebb_begins', 'ebb_slack': 'flood_begins'}

    def __init__(self, downloaded_path, f_date, l_date):

        if ft.csv_npy_file_exists(downloaded_path):
            slack_df = ft.read_df(downloaded_path)
            slack_df = slack_df[slack_df['Event'] == 'slack']
            slack_df.drop(columns=['Event', 'Speed (knots)', 'date_index', 'velocity'], inplace=True)
            slack_df['date_time'] = slack_df['date_time'].apply(pd.to_datetime)
            slack_df.sort_values('date_time')
            slack_df['date'] = slack_df['date_time'].apply(pd.to_datetime).dt.date
            slack_df['time'] = slack_df['date_time'].apply(pd.to_datetime).dt.time
            slack_df['angle'] = slack_df['time'].apply(time_to_degrees)
            slack_df = slack_df.filter(['date', 'time', 'angle'])
            slack_df = slack_df[slack_df['date'] >= f_date]
            slack_df = slack_df[slack_df['date'] <= l_date]
            slack_df = slack_df.assign(graphic_name='HG')
            self.dataframe = index_arc_df(slack_df)
        else:
            raise FileExistsError


class BatteryValidationDataframe:
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart 4.5 hours after low water at the battery
    # southbound depart 4 hours after high water at the battery

    def __init__(self, download_path, f_date, l_date):

        if ft.csv_npy_file_exists(download_path):
            frame = ft.read_df(download_path)

            south_df = frame[frame['HL'] == 'H']
            south_df.insert(len(south_df.columns), 'best_time', south_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=4))
            south_df = south_df.assign(graphic_name='ERB south')

            north_df = frame[frame['HL'] == 'L']
            north_df.insert(len(north_df.columns), 'best_time', north_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=4.5))
            north_df = north_df.assign(graphic_name='ERB north')

            best_df = north_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)
            best_df = pd.concat([best_df, south_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)], ignore_index=True)

            best_df['date'] = best_df['best_time'].dt.date
            best_df['time'] = best_df['best_time'].dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.drop(['best_time'], axis=1)
            best_df = best_df[best_df['date'] >= f_date]
            best_df = best_df[best_df['date'] <= l_date]
            self.dataframe = index_arc_df(best_df)
        else:
            raise FileExistsError
