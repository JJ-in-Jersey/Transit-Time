import numpy as np
import pandas as pd
from pathlib import Path
from os.path import exists

from project_globals import boat_speeds, timestep, seconds, sign

def distance(water_vf, water_vi, boat_speed, time): return ((water_vf+water_vi)/2+boat_speed)*time

def elapsed_time(starting_index, distances, length):
    index = starting_index
    count = total = 0
    not_there_yet = True
    while not_there_yet:
        total += distances[index]
        count += 1
        index += 1
        not_there_yet = True if length > 0 and total < length or length < 0 and total > length else False
    return count  # count = number of time steps

class ElapsedTimeJob:

    def __init__(self, edge, chart_yr, environ, intro=''):
        self.__chart_yr = chart_yr
        self.__intro = intro
        self.__id = id(edge)
        self.__length = edge.length()
        self.__edge_name = edge.name()
        self.__start_array = edge.start().velocity_array()
        self.__end_array = edge.end().velocity_array()
        self.__output_file = Path(str(environ.elapsed_time_folder())+'/'+str(self.__edge_name)+'.csv')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_two()
        self.__seconds = seconds(self.__start, self.__end)
        self.__no_timesteps = int(self.__seconds / timestep)

    def execute(self):
        if exists(self.__output_file):
            print(f'+     {self.__intro} {self.__edge_name} reading data file', flush=True)
            return tuple([self.__id, pd.read_csv(self.__output_file, header='infer')])
        else:
            print(f'+     {self.__intro} {self.__edge_name} elapsed time (1st day - 1, last day + 2)', flush=True)
            sa = self.__start_array
            ea = self.__end_array
            ts_in_hr = timestep / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            elapsed_times_df = pd.DataFrame()
            for s in boat_speeds:
                col_name = self.__edge_name+' '+str(s)
                et_df = pd.DataFrame()
                et_df['dist'] = distance(ea[1:], sa[:-1], s, ts_in_hr) if s > 0 else distance(sa[1:], ea[:-1], s, ts_in_hr)
                dist = et_df['dist'].to_numpy()
                elapsed_times_df[col_name] = np.fromiter([elapsed_time(i, dist, sign(s)*self.__length) for i in range(0, self.__no_timesteps)], dtype=int)
        elapsed_times_df.to_csv(self.__output_file, index=False)  # number of time steps
        return tuple([self.__id, elapsed_times_df])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__edge_name} {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"} {self.__no_timesteps}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__edge_name} process has raised an error: {result}', flush=True)
