import pandas as pd


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
