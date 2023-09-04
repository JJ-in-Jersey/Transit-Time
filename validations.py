import pandas as pd
from tt_file_tools import file_tools as ft
from tt_geometry.geometry import time_to_degrees


class HellGateSlackTimes:

    first_slack_lookup = {'flood_slack': 'ebb_begins', 'slack_flood': 'flood_begins', 'slack_ebb': 'ebb_begins', 'ebb_slack': 'flood_begins'}

    def __init__(self, cy, env, waypoints):
        self.hell_gate_slack = None
        self.hell_gate_3_slack = None
        slack_df = None

        print('Calculating slack water times at Hell Gate')
        hell_gate = list(filter(lambda wp: not bool(wp.unique_name.find('Hell_Gate')), waypoints))[0]
        if ft.csv_npy_file_exists(hell_gate.interpolation_data_file):
            if not ft.csv_npy_file_exists(env.transit_folder.joinpath('hell_gate_slack')):
                slack_df = ft.read_df(hell_gate.interpolation_data_file)
                slack_df = slack_df[slack_df['Event'] == 'slack'].copy()
                slack_df['date'] = slack_df['date_time'].apply(pd.to_datetime).dt.date
                slack_df['time'] = slack_df['date_time'].apply(pd.to_datetime).dt.time
                slack_df['angle'] = slack_df['time'].apply(time_to_degrees)
                slack_df = slack_df.filter(['date', 'time', 'angle'])
                slack_df = slack_df[slack_df['date'] >= cy.first_day.date()]
                slack_df = slack_df[slack_df['date'] <= cy.last_day.date()]
                ft.write_df(slack_df, env.transit_folder.joinpath('hell_gate_slack'))
            else:
                ft.read_df(env.transit_folder.joinpath('hell_gate_slack'))

            self.hell_gate_slack = self.index_slack_df(slack_df, 'Hell Gate Line')
            self.slack_df['time'] = self.slack_df['time'] + pd.Timedelta(hours=3)
            self.hell_gate_3_slack = self.index_slack_df(slack_df, 'Hell Gate Plus 3 Line')

    @staticmethod
    def index_slack_df(frame, name):
        date_time_dict = {key: [] for key in sorted(list(set(frame['date'])))}
        date_angle_dict = {key: [] for key in sorted(list(set(frame['date'])))}
        for i, row in frame.iterrows():
            date_time_dict[row[0]].append(row[1])
            date_angle_dict[row[0]].append(row[2])

        df = pd.DataFrame(columns=['date', 'name', 'time', 'angle'])
        for key in date_time_dict.keys():
            times = date_time_dict[key]
            angles = date_angle_dict[key]
            for i in range(len(times)):
                df.loc[len(df.name)] = [key, name + ' ' + str(i+1), times[i], angles[i]]
        return df
