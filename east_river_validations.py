import pandas as pd
from tt_file_tools import file_tools as ft
from tt_geometry.geometry import time_to_degrees
from validations import index_arc_df
from velocity import DownloadedSlackCSV
from tt_globals.globals import Globals
from tt_gpx.gpx import CurrentStationWP


class HellGateCurrentValidationDataframe:
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart hell gate slack water - flood begins (low tide)
    # southbound depart hell gate slack water - ebb begins (high tide)

    first_slack_lookup = {'flood_slack': 'ebb_begins', 'slack_flood': 'flood_begins', 'slack_ebb': 'ebb_begins', 'ebb_slack': 'flood_begins'}

    def __init__(self, wp: CurrentStationWP, f_date, l_date):

        filepath = DownloadedSlackCSV(Globals.FIRST_DAY, Globals.LAST_DAY, wp.folder, wp.code).filepath

        if filepath.exists():
            best_df = ft.read_df(filepath)
            best_df = best_df[best_df[' Type'] == 'slack']
            best_df.sort_values('date_time')
            best_df['start_date'] = best_df['date_time'].apply(pd.to_datetime).dt.date
            best_df['time'] = best_df['date_time'].apply(pd.to_datetime).dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.filter(['start_date', 'time', 'angle'])
            best_df = best_df[best_df['start_date'] >= f_date]
            best_df = best_df[best_df['start_date'] < l_date]
            best_df = best_df.assign(graphic_name='HG')
            self.frame = index_arc_df(best_df)
        else:
            raise FileExistsError


class BatteryValidationDataframe:
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart 4.5 hours after low water at the battery
    # southbound depart 4 hours after high water at the battery

    def __init__(self, download_path, f_date, l_date):

        if download_path.exists():
            frame = ft.read_df(download_path)

            north_df = frame[frame['HL'] == 'L']
            north_df.insert(len(north_df.columns), 'best_time', north_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=4.5))
            north_df = north_df.assign(graphic_name='ERB north')

            south_df = frame[frame['HL'] == 'H']
            south_df.insert(len(south_df.columns), 'best_time', south_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=4))
            south_df = south_df.assign(graphic_name='ERB south')

            best_df = pd.concat([north_df.drop(['date', 'time', 'HL', 'date_time'], axis=1), south_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)], ignore_index=True)

            best_df['start_date'] = best_df['best_time'].dt.date
            best_df['time'] = best_df['best_time'].dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.drop(['best_time'], axis=1)
            best_df = best_df[best_df['start_date'] >= f_date]
            best_df = best_df[best_df['start_date'] < l_date]
            self.frame = index_arc_df(best_df)
        else:
            raise FileExistsError


class HellGateTideValidationDataframe:
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart horns hook slack water - flood begins (low tide)
    # southbound depart horns hook slack water - ebb begins (high tide)

    def __init__(self, download_path, f_date, l_date):

        if download_path.exists():
            frame = ft.read_df(download_path)

            north_df = frame[frame['HL'] == 'L']
            north_df.insert(len(north_df.columns), 'best_time', north_df['date_time'].apply(pd.to_datetime))
            north_df = north_df.assign(graphic_name='HH fb')

            south_df = frame[frame['HL'] == 'H']
            south_df.insert(len(south_df.columns), 'best_time', south_df['date_time'].apply(pd.to_datetime))
            south_df = south_df.assign(graphic_name='HH eb')

            best_df = pd.concat([north_df.drop(['date', 'time', 'HL', 'date_time'], axis=1),
                                 south_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)], ignore_index=True)

            best_df['start_date'] = best_df['best_time'].dt.date
            best_df['time'] = best_df['best_time'].dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.drop(['best_time'], axis=1)
            best_df = best_df[best_df['start_date'] >= f_date]
            best_df = best_df[best_df['start_date'] < l_date]
            self.frame = index_arc_df(best_df)
        else:
            raise FileExistsError


class HornsHookValidationDataframe:
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart horns hook slack water - flood begins (low tide)
    # southbound depart horns hook slack water - ebb begins (high tide)

    def __init__(self, download_path, f_date, l_date):

        if download_path.exists():
            frame = ft.read_df(download_path)

            north_df = frame[frame['HL'] == 'L']
            north_df.insert(len(north_df.columns), 'best_time', north_df['date_time'].apply(pd.to_datetime))
            north_df = north_df.assign(graphic_name='HH fb')

            south_df = frame[frame['HL'] == 'H']
            south_df.insert(len(south_df.columns), 'best_time', south_df['date_time'].apply(pd.to_datetime))
            south_df = south_df.assign(graphic_name='HH eb')

            best_df = pd.concat([north_df.drop(['date', 'time', 'HL', 'date_time'], axis=1),
                                 south_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)], ignore_index=True)

            best_df['start_date'] = best_df['best_time'].dt.date
            best_df['time'] = best_df['best_time'].dt.time
            best_df['angle'] = best_df['time'].apply(time_to_degrees)
            best_df = best_df.drop(['best_time'], axis=1)
            best_df = best_df[best_df['start_date'] >= f_date]
            best_df = best_df[best_df['start_date'] < l_date]
            self.frame = index_arc_df(best_df)
        else:
            raise FileExistsError
