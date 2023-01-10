import numpy as np
import pandas as pd
from time import perf_counter

from project_globals import TIMESTEP, boat_speeds, sign, output_file_exists
from project_globals import read_df, write_df, min_sec, write_arr

#  Elapsed times are reported in number of timesteps

df_type = 'hdf'

def distance(water_vf, water_vi, boat_speed, time): return ((water_vf + water_vi) / 2 + boat_speed)*time  # distance is nm

def elapsed_time(distance_start_index, distances, length):  # returns number of timesteps
    distance_index = distance_start_index + 1  # distance at departure time is 0
    count = total = 0
    not_there_yet = True
    while not_there_yet:
        total += distances[distance_index]
        count += 1
        distance_index += 1
        not_there_yet = True if length > 0 and total < length or length < 0 and total > length else False
    return count  # count = number of time steps

class ElapsedTimeJob:

    def distance_array_path(self, speed): return self.edge_folder.joinpath(self.name + '_distance_array_' + str(speed))

    def __init__(self, edge, chart_yr, intro=''):
        self.intro = intro
        self.length = edge.length()
        self.name = edge.name()
        self.id = id(edge)
        self.start_velocities = edge.start().velocities()
        self.end_velocities = edge.end().velocities()
        self.edge_folder = edge.edge_folder()
        self.elapsed_time_table_path = edge.elapsed_time_table_path()

        self.start_index = chart_yr.edge_start_index()
        self.end_index = chart_yr.edge_end_index()
        self.edge_range = chart_yr.edge_range()
        self.waypoint_range = chart_yr.waypoint_range()

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self.elapsed_time_table_path):
            print(f'+     {self.intro} {self.name} ({round(self.length, 2)} nm) reading data file', flush=True)
            elapsed_times_df = read_df(self.elapsed_time_table_path)
            return tuple([self.id, elapsed_times_df, init_time])
        else:
            print(f'+     {self.intro} {self.name} ({round(self.length, 2)} nm)', flush=True)
            initial_velocities = self.start_velocities  # in knots
            final_velocities = self.end_velocities  # in knots
            elapsed_times_df = pd.DataFrame(data=self.edge_range, columns=['departure_index'])
            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            for s in boat_speeds:
                col_name = self.name+' '+str(s)
                dist = distance(final_velocities[1:], initial_velocities[:-1], s, ts_in_hr)  # distance in nm
                dist = np.insert(dist,0,0.0)  # because distance uses an offset calculation VIx VFx+1, we need to add a zero to the begining
                write_arr(dist, self.distance_array_path(s))
                elapsed_times_df[col_name] = [elapsed_time(i, dist, sign(s)*self.length) for i in range(0, len(self.edge_range))]
                elapsed_times_df.fillna(0, inplace=True)
            write_df(elapsed_times_df, self.elapsed_time_table_path, df_type)
        return tuple([self.id, elapsed_times_df, init_time])  # elapsed times are reported in number of timesteps

    def execute_callback(self, result):
        print(f'-     {self.intro} {self.name} ({round(self.length, 2)} nm) {min_sec(perf_counter() - result[2])} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} {self.name} process has raised an error: {result}', flush=True)
