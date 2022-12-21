import pandas as pd
from os.path import exists
from time import perf_counter

from project_globals import TIMESTEP, boat_speeds, seconds, sign

def distance(water_vf, water_vi, boat_speed, time): return ((water_vf + water_vi) / 2 + boat_speed)*time

def elapsed_time(departure_index, distances, length):
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
        self.output_file = env.elapsed_time_folder().joinpath(edge.name()+'_ets_table.csv')
        self.start = chart_yr.first_day_minus_one()
        self.end = chart_yr.last_day_plus_two()
        self.no_timesteps = int(seconds(self.start, self.end) / TIMESTEP)

    def execute(self):
        if exists(self.output_file):
            print(f'+     {self.intro} {self.edge_name} reading data file', flush=True)
            return tuple([self.id, pd.read_csv(self.output_file, header='infer')])
        else:
            print(f'+     {self.intro} {self.edge_name} elapsed time (1st day - 1, last day + 2)', flush=True)
            sa = self.start_velocity_table['velocity']
            ea = self.end_velocity_table['velocity']
            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            elapsed_time_df = pd.DataFrame()
            elapsed_time_df['departure_index'] = self.start_velocity_table['time_index']
            elapsed_time_df['departure_time'] = pd.to_timedelta(elapsed_time_df['departure_index'], unit='seconds') + self.date.index_basis()
            elapsed_time_df = elapsed_time_df[elapsed_time_df['departure_time'] < self.end]

            for s in boat_speeds:
                col_name = self.edge_name+' '+str(s)
                et_df = pd.DataFrame()
                et_df['time_index'] = self.start_velocity_table['time_index']
                et_df['date'] = self.start_velocity_table['date']
                et_df['distance'] = distance(ea[1:], sa[:-1], s, ts_in_hr) if s > 0 else distance(sa[1:], ea[:-1], s, ts_in_hr)
                et_df.fillna(0, inplace=True)
                et_df.to_csv(self.env.edge_folder(self.edge_name).joinpath(self.edge_name+'_distance_table_'+str(s)+'.csv'), index=False)
                elapsed_time_df[col_name] = [elapsed_time(i, et_df['distance'].to_numpy(), sign(s)*self.length) for i in range(0, self.no_timesteps)]
            elapsed_time_df.to_csv(self.output_file, index=False)  # number of time steps
        return tuple([self.id, elapsed_time_df])

    # noinspection PyUnusedLocal
    def execute_callback(self, result):
        print(f'-     {self.intro} {self.edge_name} {round((perf_counter() - self.init_time), 2)} seconds {round(self.length, 2)} nautical miles', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} {self.edge_name} process has raised an error: {result}', flush=True)
