import pandas as pd
from tt_geometry.geometry import time_to_degrees
from tt_globals.globals import Globals
from tt_noaa_data.noaa_data import noaa_tide_dataframe, noaa_slack_dataframe
from tt_date_time_tools.date_time_tools import int_timestamp as date_time_index


def index_arc_df(frame):
    date_time_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    date_angle_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    date_name_dict = {key: [] for key in sorted(list(set(frame['start_date'])))}
    columns = frame.columns.to_list()
    for i, row in frame.iterrows():
        date_time_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('time')])
        date_angle_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('angle')])
        date_name_dict[row.iloc[columns.index('start_date')]].append(row.iloc[columns.index('graphic_name')])

    df = pd.DataFrame(columns=['start_date', 'name', 'time', 'angle'])
    for key in date_time_dict.keys():
        times = date_time_dict[key]
        angles = date_angle_dict[key]
        names = date_name_dict[key]
        for i in range(len(times)):
            df.loc[len(df.name)] = [key, names[i] + ' ' + str(i + 1), times[i], angles[i]]
    return df


def hell_gate_validation():
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart hell gate slack water - flood begins (low tide)
    # southbound depart hell gate slack water - ebb begins (high tide)
    wp_code = "NYH1924"

    noaa_frame = noaa_slack_dataframe(Globals.FIRST_DAY, Globals.LAST_DAY, wp_code)
    noaa_frame.rename(columns={'Time': 'date_time', ' Velocity_Major': 'velocity', ' Type': 'type'}, inplace=True)
    noaa_frame['date_index'] = noaa_frame['date_time'].apply(date_time_index)

    slack_frame = noaa_frame[noaa_frame['type'] == 'slack']
    slack_frame.sort_values('date_time')
    slack_frame['start_date'] = slack_frame['date_time'].apply(pd.to_datetime).dt.date
    slack_frame['time'] = slack_frame['date_time'].apply(pd.to_datetime).dt.time
    slack_frame['angle'] = slack_frame['time'].apply(time_to_degrees)
    slack_frame = slack_frame.filter(['start_date', 'time', 'angle'])
    slack_frame = slack_frame.assign(graphic_name='HG')
    slack_frame = index_arc_df(slack_frame)
    return slack_frame


def battery_validation():
    # flood current is to the north & east, ebb current is to the south & west
    # northbound depart 4.5 hours after low water at the battery
    # southbound depart 4 hours after high water at the battery
    wp_code = "8518750"

    frame = noaa_tide_dataframe(Globals.FIRST_DAY, Globals.LAST_DAY, wp_code)

    north_df = frame[frame['HL'] == 'L']
    north_df.insert(len(north_df.columns), 'best_time', north_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=4.5))
    north_df = north_df.assign(graphic_name='BN')
    south_df = frame[frame['HL'] == 'H']
    south_df.insert(len(south_df.columns), 'best_time', south_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=4))
    south_df = south_df.assign(graphic_name='BS')

    best_df = pd.concat([north_df.drop(['date', 'time', 'HL', 'date_time'], axis=1), south_df.drop(['date', 'time', 'HL', 'date_time'], axis=1)], ignore_index=True)
    best_df['start_date'] = best_df['best_time'].dt.date
    best_df['time'] = best_df['best_time'].dt.time
    best_df['angle'] = best_df['time'].apply(time_to_degrees)
    best_df = best_df.drop(['best_time'], axis=1)
    best_df = index_arc_df(best_df)
    return best_df


def chesapeake_city_validation():
    # eastbound depart 3 minutes before "Slack Water Flood Begins" at the Chesapeake City station for the very beginning of a fair current
    wp_code = "cb1301"

    noaa_frame = noaa_slack_dataframe(Globals.FIRST_DOWNLOAD_DAY, Globals.LAST_DOWNLOAD_DAY, wp_code)
    noaa_frame.rename(columns={'Time': 'date_time', ' Velocity_Major': 'velocity', ' Type': 'type'}, inplace=True)
    slack_frame = noaa_frame[noaa_frame['type'].shift(-1) == 'flood']
    slack_frame['date_time'] = slack_frame['date_time'].apply(pd.to_datetime) - pd.Timedelta(minutes=3)
    slack_frame = slack_frame[slack_frame['date_time'] >= Globals.FIRST_DAY]
    slack_frame = slack_frame[slack_frame['date_time'] < Globals.LAST_DAY]
    slack_frame['date_index'] = slack_frame['date_time'].apply(date_time_index)

    slack_frame['start_date'] = slack_frame['date_time'].apply(pd.to_datetime).dt.date
    slack_frame['time'] = slack_frame['date_time'].apply(pd.to_datetime).dt.time
    slack_frame['angle'] = slack_frame['time'].apply(time_to_degrees)

    slack_frame = slack_frame.filter(['start_date', 'time', 'angle'])
    slack_frame = slack_frame.assign(graphic_name='CC')
    slack_frame = index_arc_df(slack_frame)
    return slack_frame


def reedy_point_tower_validation():
    # westbound depart 7 minutes before "Slack Water Ebb Begins" at the Reedy Point Tower station for the very beginning of a fair current
    wp_code = "ACT6256"

    noaa_frame = noaa_slack_dataframe(Globals.FIRST_DOWNLOAD_DAY, Globals.LAST_DOWNLOAD_DAY, wp_code)
    noaa_frame.rename(columns={'Time': 'date_time', ' Velocity_Major': 'velocity', ' Type': 'type'}, inplace=True)
    slack_frame = noaa_frame[noaa_frame['type'].shift(-1) == 'ebb']
    slack_frame['date_time'] = slack_frame['date_time'].apply(pd.to_datetime) - pd.Timedelta(minutes=7)
    slack_frame = slack_frame[slack_frame['date_time'] >= Globals.FIRST_DAY]
    slack_frame = slack_frame[slack_frame['date_time'] < Globals.LAST_DAY]
    slack_frame['date_index'] = slack_frame['date_time'].apply(date_time_index)

    slack_frame['start_date'] = slack_frame['date_time'].apply(pd.to_datetime).dt.date
    slack_frame['time'] = slack_frame['date_time'].apply(pd.to_datetime).dt.time
    slack_frame['angle'] = slack_frame['time'].apply(time_to_degrees)

    slack_frame = slack_frame.filter(['start_date', 'time', 'angle'])
    slack_frame = slack_frame.assign(graphic_name='RP')
    slack_frame = index_arc_df(slack_frame)
    return slack_frame
