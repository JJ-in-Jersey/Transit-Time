import numpy as np
import pandas as pd
from pathlib import Path
from time import perf_counter
from GPX import Edge

from project_globals import TIMESTEP, DF_FILE_TYPE, boat_speeds, sign, file_exists, mins_secs
from ReadWrite import ReadWrite as rw
from MemoryHelper import MemoryHelper as mh

#  Elapsed times are reported in number of timesteps

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

# noinspection PyProtectedMember
class ElapsedTimeJob:

    @staticmethod
    def distance(water_vf, water_vi, boat_speed, ts_in_hr): return ((water_vf + water_vi) / 2 + boat_speed) * ts_in_hr  # distance is nm

    def __init__(self, edge: Edge):
        self.edge = edge
        self.result_key = id(edge)
        self.length = edge.length
        self.init_velo = edge.start.output_data
        self.final_velo = edge.end.output_data
        self.unique_name = edge.unique_name

    def execute(self):
        init_time = perf_counter()
        if file_exists(self.edge.output_data_file):
            print(f'+     {self.unique_name} ({round(self.length, 2)} nm)', flush=True)
            elapsed_times_df = rw.read_df(self.edge.output_data_file)
            return tuple([self.result_key, elapsed_times_df, init_time])
        else:
            print(f'+     {self.unique_name} ({round(self.length, 2)} nm)', flush=True)
            elapsed_times_df = pd.DataFrame(data={'departure_index': self.edge.edge_range})
            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            for s in boat_speeds:
                col_name = str(s) + ' ' + self.unique_name
                dist = ElapsedTimeJob.distance(self.final_velo[1:], self.init_velo[:-1], s, ts_in_hr)  # distance in nm
                dist = np.insert(dist, 0, 0.0)  # because distance uses an offset calculation VIx VFx+1, we need to add a zero to the beginning
                elapsed_times_df[col_name] = [elapsed_time(i, dist, sign(s)*self.length) for i in range(len(self.edge.edge_range))]
            elapsed_times_df.fillna(0, inplace=True)
            elapsed_times_df = mh.shrink_dataframe(elapsed_times_df)
            elapsed_times_df.to_csv(self.edge.output_data_file.with_suffix('.csv'), index=include)

        return tuple([self.result_key, elapsed_times_df, init_time])  # elapsed times are reported in number of timesteps

    def execute_callback(self, result):
        print(f'-     {self.unique_name} ({round(self.length, 2)} nm) {mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.unique_name} process has raised an error: {result}', flush=True)
