import pandas as pd
from time import perf_counter

from project_globals import TIMESTEP, boat_speeds, seconds, sign, output_file_exists
# noinspection PyUnresolvedReferences
from project_globals import write_df_pkl, read_df, write_df_csv

#  Elapsed times are reported in number of timesteps

def distance(water_vf, water_vi, boat_speed, time): return ((water_vf + water_vi) / 2 + boat_speed)*time  # distance is nm

def elapsed_time(departure_index, distances, length):  # returns number of timesteps
    distance_index = departure_index + 1  # distance at departure time is 0
    count = total = 0
    not_there_yet = True
    while not_there_yet:
        total += distances[distance_index]
        count += 1
        distance_index += 1
        not_there_yet = True if length > 0 and total < length or length < 0 and total > length else False
    return count  # count = number of time steps

class ElapsedTimeJob:

    def __init__(self, edge, chart_yr, env, intro=''):
        self.init_time = perf_counter()
        self.date = chart_yr
        self.env = env
        self.intro = intro
        self.length = edge.length()
        self.edge_name = edge.name()
        self.id = id(edge)
        self.start_velocity_table = edge.start().velocity_table()
        self.end_velocity_table = edge.end().velocity_table()
        self.output_file = env.elapsed_time_folder().joinpath(edge.name()+'_table')

        self.start = chart_yr.first_day_minus_one()
        self.end = chart_yr.last_day_plus_two()
        self.no_timesteps = int(seconds(self.start, self.end) / TIMESTEP)

    def execute(self):
        if output_file_exists(self.output_file):
            print(f'+     {self.intro} {self.edge_name} {round(self.length, 2)} nm reading data file', flush=True)
            return tuple([self.id, read_df(self.output_file)])
        else:
            print(f'+     {self.intro} {self.edge_name} {round(self.length, 2)} nm', flush=True)
            initial_velocities = self.start_velocity_table['velocity']  # in knots
            final_velocities = self.end_velocity_table['velocity']  # in knots
            elapsed_time_df = self.start_velocity_table.drop(['velocity'], axis=1)
            elapsed_time_df['date'] = pd.to_datetime(elapsed_time_df['date'])
            elapsed_time_df.rename(columns={'time_index': 'departure_index','date':'departure_time' }, inplace=True)
            elapsed_time_df = elapsed_time_df[elapsed_time_df['departure_time'] < self.end]  # trim off excess velocity rows

            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            for s in boat_speeds:
                col_name = self.edge_name+' '+str(s)
                et_df = pd.DataFrame()
                et_df['time_index'] = self.start_velocity_table['time_index']
                et_df['date'] = self.start_velocity_table['date']
                et_df['distance'] = distance(final_velocities[1:], initial_velocities[:-1], s, ts_in_hr) if s > 0 else distance(initial_velocities[1:], final_velocities[:-1], s, ts_in_hr)  # distance is nm
                et_df.fillna(0, inplace=True)
                write_df_pkl(et_df, self.env.create_edge_folder(self.edge_name).joinpath(self.edge_name+'_distance_table_'+str(s)))
                elapsed_time_df[col_name] = [elapsed_time(i, et_df['distance'].to_numpy(), sign(s)*self.length) for i in range(0, self.no_timesteps)]
            write_df_pkl(elapsed_time_df, self.output_file)
        return tuple([self.id, elapsed_time_df])  # elapsed times are reported in number of timesteps

    # noinspection PyUnusedLocal
    def execute_callback(self, result):
        print(f'-     {self.intro} {self.edge_name} {round((perf_counter() - self.init_time)/60, 2)} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} {self.edge_name} process has raised an error: {result}', flush=True)
