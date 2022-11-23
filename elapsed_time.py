import numpy as np
import pandas as pd
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

    def __init__(self, edge, chart_yr):
        self.__edge = edge
        self.__edge_name = edge.start().code() + '-' + edge.end().code()
        self.__chart_yr = chart_yr
        self.__start_array = edge.start().velocity_array()
        self.__end_array = edge.end().velocity_array()
        self.__calc_start = chart_yr.calc_start()
        self.__calc_end = int(seconds(chart_yr.calc_start(), chart_yr.last_day_plus_one())/timestep)

    def execute(self):
        # cols = [self.__edge_name+' '+str(sign*speed) for sign in range(1, -2, -2) for speed in boat_speeds]
        # elapsed_times_df = pd.DataFrame(columns=cols)
        elapsed_times_df = pd.DataFrame()

        for s in boat_speeds:
            print(s)
            col_name = self.__edge_name+' '+str(s)
            et_df = pd.DataFrame()
            et_df['dist'] = distance(self.__end_array[1:], self.__start_array[:-1], s, timestep / 3600)  # timestep/3600 because velocities are per hour
            elapsed_times_df[col_name] = np.fromiter([elapsed_time(i, et_df['dist'].to_numpy(), self.__edge.length()) for i in range(0, self.__calc_end)], dtype=int)

        for s in boat_speeds:
            print(s)
            s = -1*s
            col_name = self.__edge_name+' '+str(s)
            et_df = pd.DataFrame()
            et_df['dist'] = distance(self.__start_array[1:], self.__end_array[:-1], s, timestep / 3600)
            elapsed_times_df[col_name] = np.fromiter([elapsed_time(i, et_df['dist'].to_numpy(), -1*self.__edge.length()) for i in range(0, self.__calc_end)], dtype=int)

        print(elapsed_times_df)
#
#     def execute_callback(self, result):
#         pass
#
#     def error_callback(self, result):
#         pass
