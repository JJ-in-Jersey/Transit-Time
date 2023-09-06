import pandas as pd
from tt_file_tools import file_tools as ft
from tt_geometry.geometry import time_to_degrees


class HellGateSlackTimes:

    first_slack_lookup = {'flood_slack': 'ebb_begins', 'slack_flood': 'flood_begins', 'slack_ebb': 'ebb_begins', 'ebb_slack': 'flood_begins'}

    def __init__(self, cy, env, waypoints):
        self.hell_gate_start_slack = None
        self.hell_gate_end_slack = None
        slack_df = None

        print('Calculating slack water times at Hell Gate')
        hell_gate = list(filter(lambda wp: not bool(wp.unique_name.find('Hell_Gate')), waypoints))[0]
        if ft.csv_npy_file_exists(hell_gate.interpolation_data_file):
            slack_df = ft.read_df(hell_gate.interpolation_data_file)
            slack_df = slack_df[slack_df['Event'] == 'slack'].copy()
            slack_df['date'] = slack_df['date_time'].apply(pd.to_datetime).dt.date
            slack_df['time'] = slack_df['date_time'].apply(pd.to_datetime).dt.time
            slack_df['angle'] = slack_df['time'].apply(time_to_degrees)
            slack_df = slack_df.filter(['date_time', 'date', 'time', 'angle'])
            slack_df = slack_df[slack_df['date'] >= cy.first_day.date()]
            slack_df = slack_df[slack_df['date'] <= cy.last_day.date()]
            self.hell_gate_start_slack = self.index_slack_df(slack_df, 'Hell Gate Start Line'), env.transit_folder.joinpath('hell_gate_start_slack')

            slack_df = ft.read_df(hell_gate.interpolation_data_file)
            slack_df = slack_df[slack_df['Event'] == 'slack'].copy()
            slack_df['date'] = slack_df['date_time'].apply(pd.to_datetime).dt.date
            slack_df['time'] = slack_df['date_time'] + pd.Timedelta(hours=3)
            slack_df['time'] = slack_df['time'].apply(pd.to_datetime).dt.time
            slack_df['angle'] = slack_df['time'].apply(time_to_degrees)
            slack_df = slack_df.filter(['date_time', 'date', 'time', 'angle'])
            slack_df = slack_df[slack_df['date'] >= cy.first_day.date()]
            slack_df = slack_df[slack_df['date'] <= cy.last_day.date()]
            self.hell_gate_end_slack = self.index_slack_df(slack_df, 'Hell Gate End Line'), env.transit_folder.joinpath('hell_gate_end_slack')

    @staticmethod
    def index_slack_df(frame, name):
        date_time_dict = {key: [] for key in sorted(list(set(frame['date'])))}
        date_angle_dict = {key: [] for key in sorted(list(set(frame['date'])))}
        columns = frame.columns.to_list()
        for i, row in frame.iterrows():
            date_time_dict[row[columns.index('date')]].append(row[columns.index('time')])
            date_angle_dict[row[columns.index('date')]].append(row[columns.index('angle')])

        df = pd.DataFrame(columns=['date', 'name', 'time', 'angle'])
        for key in date_time_dict.keys():
            times = date_time_dict[key]
            angles = date_angle_dict[key]
            for i in range(len(times)):
                df.loc[len(df.name)] = [key, name + ' ' + str(i+1), times[i], angles[i]]
        return df
