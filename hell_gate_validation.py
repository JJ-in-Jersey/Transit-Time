import pandas as pd
from tt_file_tools import file_tools as ft
from tt_geometry.geometry import time_to_degrees
from tt_job_manager.job_manager import Job


def index_arc_df(frame, name):
    date_time_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    date_angle_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    columns = frame.columns.to_list()
    for i, row in frame.iterrows():
        # date_time_dict[row[columns.index('date')]].append(row[columns.index('time')])
        # date_angle_dict[row[columns.index('date')]].append(row[columns.index('angle')])
        date_time_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('time')])
        date_angle_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('angle')])

    df = pd.DataFrame(columns=['date', 'name', 'time', 'angle'])
    for key in date_time_dict.keys():
        times = date_time_dict[key]
        angles = date_angle_dict[key]
        for i in range(len(times)):
            df.loc[len(df.name)] = [key, name + ' ' + str(i + 1), times[i], angles[i]]
    return df


class HellGateSlackTimesDataframe:

    first_slack_lookup = {'flood_slack': 'ebb_begins', 'slack_flood': 'flood_begins', 'slack_ebb': 'ebb_begins', 'ebb_slack': 'flood_begins'}

    def __init__(self, f_date, l_date, downloaded_current_path):

        if ft.csv_npy_file_exists(downloaded_current_path):
            slack_df = ft.read_df(downloaded_current_path)
            slack_df = slack_df[slack_df['Event'] == 'slack']
            slack_df.drop(columns=['Event', 'Speed (knots)', 'date_index', 'velocity'], inplace=True)
            slack_df = pd.concat([slack_df, pd.DataFrame(columns=['date_time'], data=slack_df['date_time'].apply(pd.to_datetime) + pd.Timedelta(hours=3))])
            slack_df['date_time'] = slack_df['date_time'].apply(pd.to_datetime)
            slack_df.sort_values('date_time')
            slack_df['date'] = slack_df['date_time'].apply(pd.to_datetime).dt.date
            slack_df['time'] = slack_df['date_time'].apply(pd.to_datetime).dt.time
            slack_df['angle'] = slack_df['time'].apply(time_to_degrees)
            slack_df = slack_df.filter(['date', 'time', 'angle'])
            slack_df = slack_df[slack_df['date'] >= f_date]
            slack_df = slack_df[slack_df['date'] <= l_date]
            self.dataframe = index_arc_df(slack_df, 'Hell Gate Line')
        else:
            raise FileExistsError


class HellGateValidationJob(Job):

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, f_date, l_date, frame):
        result_key = 'hgv'
        job_name = "Hell Gate Validation"
        arguments = tuple([f_date, l_date, frame])
        super().__init__(job_name, result_key, HellGateSlackTimesDataframe, arguments)