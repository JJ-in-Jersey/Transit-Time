import numpy as np
import pandas as pd
from pathlib import Path
from os.path import exists

from project_globals import boat_speeds, timestep, seconds

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
    return int(count*timestep)  # count = number of timesteps to next waypoint, count*timestep = elapsed time

class ElapsedTimeJob:

    def __init__(self, edge, chart_yr, d_dir, intro=''):
        self.__chart_yr = chart_yr
        self.__p_dir = d_dir.project_folder()
        self.__intro = intro
        self.__id = id(edge)
        self.__length = edge.length()
        self.__edge_name = edge.name()
        self.__start_array = edge.start().velocity_array()
        self.__end_array = edge.end().velocity_array()
        self.__calc_start = chart_yr.calc_start()
        self.__calc_end = int(seconds(chart_yr.calc_start(), chart_yr.last_day_plus_one())/timestep)
        self.__output_file = Path(str(d_dir.project_folder())+'/'+str(self.__edge_name)+'.csv')

    def execute(self):
        if exists(self.__output_file):
            print(f'+     {self.__intro} {self.__edge_name} reading data file')
            return tuple([self.__id, pd.read_csv(self.__output_file, header='infer')])
        else:
            print(f'+     {self.__intro} {self.__edge_name} elapsed time calculation starting', flush=True)
            elapsed_times_df = pd.DataFrame()
            for s in boat_speeds:
                col_name = self.__edge_name+' '+str(s)
                et_df = pd.DataFrame()
                et_df['dist'] = distance(self.__end_array[1:], self.__start_array[:-1], s, timestep / 3600)  # timestep/3600 because velocities are per hour
                elapsed_times_df[col_name] = np.fromiter([elapsed_time(i, et_df['dist'].to_numpy(), self.__length) for i in range(0, self.__calc_end)], dtype=int)
            for s in boat_speeds:
                col_name = self.__edge_name+' '+str(-1*s)
                et_df = pd.DataFrame()
                et_df['dist'] = distance(self.__start_array[1:], self.__end_array[:-1], -1*s, timestep / 3600)
                elapsed_times_df[col_name] = np.fromiter([elapsed_time(i, et_df['dist'].to_numpy(), -1*self.__length) for i in range(0, self.__calc_end)], dtype=int)
                elapsed_times_df.to_csv(self.__output_file, index=False)
                return tuple([self.__id, elapsed_times_df])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__edge_name} {"SUCCESSFUL" if isinstance(result[1], pd.DataFrame) else "FAILED"}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__edge_name} process has raised an error: {result}', flush=True)
