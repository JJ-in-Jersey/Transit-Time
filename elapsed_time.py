import numpy as np
import pandas as pd
from pathlib import Path
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

    def __init__(self, edge, chart_yr, environ, intro=''):
        self.__init_time = perf_counter()
        self.__chart_yr = chart_yr
        self.__index_basis = chart_yr.index_basis()
        self.__intro = intro
        self.__id = id(edge)
        self.__length = edge.length()
        self.__edge_name = edge.name()
        self.__start_velocity_table = edge.start().velocity_table()
        self.__end_velocity_table = edge.end().velocity_table()
        self.__distance_table = Path(str(environ.elapsed_time_folder())+'/'+str(self.__edge_name)+'_distance_table_')
        self.__output_file = Path(str(environ.elapsed_time_folder())+'/'+str(self.__edge_name)+'.csv')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_two()
        self.__no_timesteps = int(seconds(self.__start, self.__end) / TIMESTEP)

    def execute(self):
        if exists(self.__output_file):
            print(f'+     {self.__intro} {self.__edge_name} reading data file', flush=True)
            return tuple([self.__id, pd.read_csv(self.__output_file, header='infer')])
        else:
            print(f'+     {self.__intro} {self.__edge_name} elapsed time (1st day - 1, last day + 2)', flush=True)
            sa = self.__start_velocity_table['velocity']
            ea = self.__end_velocity_table['velocity']
            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            elapsed_times_df = pd.DataFrame()
            elapsed_times_df['departure_index'] = self.__start_velocity_table['time_index']
            elapsed_times_df['departure_time'] = pd.to_timedelta(elapsed_times_df['departure_index'], unit='seconds') + self.__index_basis
            elapsed_times_df = elapsed_times_df[elapsed_times_df['departure_time'] < self.__end]

            for s in boat_speeds:
                col_name = self.__edge_name+' '+str(s)
                et_df = pd.DataFrame()
                et_df['date'] = self.__start_velocity_table['date']
                et_df['time_index'] = self.__start_velocity_table['time_index']
                et_df['dist'] = distance(ea[1:], sa[:-1], s, ts_in_hr) if s > 0 else distance(sa[1:], ea[:-1], s, ts_in_hr)
                et_df.fillna(0, inplace=True)
                et_df.to_csv(Path(str(self.__distance_table)+str(s)+'.csv'), index=False)
                elapsed_times_df[col_name] = [elapsed_time(i, et_df['dist'].to_numpy(), sign(s)*self.__length) for i in range(0, self.__no_timesteps)]
            elapsed_times_df.to_csv(self.__output_file, index=False)  # number of time steps
        return tuple([self.__id, elapsed_times_df])

    # noinspection PyUnusedLocal
    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__edge_name} {round((perf_counter() - self.__init_time), 2)} seconds {round(self.__length, 2)} nautical miles', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__edge_name} process has raised an error: {result}', flush=True)
