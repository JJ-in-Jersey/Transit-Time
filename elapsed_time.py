import numpy as np
import pandas as pd
from time import perf_counter

from tt_gpx.gpx import Edge
from tt_file_tools import file_tools as ft
from tt_date_time_tools import date_time_tools as dtt

from project_globals import TIMESTEP, sign

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


class ElapsedTimeJob:

    ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)

    @staticmethod
    def distance(water_vf, water_vi, boat_speed, ts_in_hr): return ((water_vf + water_vi) / 2 + boat_speed) * ts_in_hr  # distance is nm

    def __init__(self, edge: Edge, speed: int):
        # always minimize the amount of stuff to be pickled
        self.name = edge.unique_name
        self.speed = speed
        self.result_key = edge.unique_name
        edge.final_data_filepath = edge.folder.joinpath(self.result_key)
        self.final_data_filepath = edge.folder.joinpath(self.result_key)
        self.length = edge.length
        self.range = edge.edge_range
        self.init_velo = edge.start.current_data['velocity'].to_numpy(dtype='float16')
        self.final_velo = edge.end.current_data['velocity'].to_numpy(dtype='float16')

    def execute(self):
        init_time = perf_counter()
        if ft.csv_npy_file_exists(self.final_data_filepath):
            print(f'+     {self.name} {self.speed} ({round(self.length, 2)} nm)', flush=True)
            elapsed_times_df = ft.read_df(self.final_data_filepath)
        else:
            print(f'+     {self.name} {self.speed} ({round(self.length, 2)} nm)', flush=True)
            elapsed_times_df = pd.DataFrame(data={'departure_index': self.range})
            dist = ElapsedTimeJob.distance(self.final_velo[1:], self.init_velo[:-1], self.speed, ElapsedTimeJob.ts_in_hr)  # distance in nm
            dist = np.insert(dist, 0, 0.0)  # because distance uses an offset calculation VIx VFx+1, we need to add a zero to the beginning
            elapsed_times_df[self.name] = [elapsed_time(i, dist, sign(self.speed)*self.length) for i in range(len(self.range))]
            elapsed_times_df.fillna(0, inplace=True)
            ft.write_df(elapsed_times_df, self.final_data_filepath)

        return tuple([self.result_key, elapsed_times_df, init_time])  # elapsed times are reported in number of timesteps

    def execute_callback(self, result):
        print(f'-     {self.name} {self.speed} ({round(self.length, 2)} nm) {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.name} {self.speed} process has raised an error: {result}', flush=True)
