import numpy as np
import pandas as pd
from pathlib import Path
from time import perf_counter
from GPX import Edge

from project_globals import TIMESTEP, DF_FILE_TYPE, boat_speeds, sign, output_file_exists, mins_secs
from ReadWrite import ReadWrite as rw

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

    def __init__(self, cy, edge: Edge):
        self.edge = edge
        self.result_key = id(edge)
        self.length = edge.length
        self.init_velo = edge.start.velo_arr
        self.final_velo = edge.end.velo_arr
        self.name = edge.name

        self.start_index = cy.edge_start_index()
        self.end_index = cy.edge_end_index()
        self.edge_range = cy.edge_range()

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self.edge.et_file):
            print(f'+     {self.name} ({round(self.length, 2)} nm)', flush=True)
            elapsed_times_df = rw.read_df(self.edge.et_file)
            return tuple([self.result_key, elapsed_times_df, init_time])
        else:
            print(f'+     {self.name} ({round(self.length, 2)} nm)', flush=True)
            elapsed_times_df = pd.DataFrame(data=self.edge_range, columns=['departure_index'])
            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            for s in boat_speeds:
                col_name = str(s) + ' ' + self.name
                dist = ElapsedTimeJob.distance(self.final_velo[1:], self.init_velo[:-1], s, ts_in_hr)  # distance in nm
                dist = np.insert(dist, 0, 0.0)  # because distance uses an offset calculation VIx VFx+1, we need to add a zero to the beginning
                elapsed_times_df[col_name] = [elapsed_time(i, dist, sign(s)*self.length) for i in range(0, len(self.edge_range))]
            elapsed_times_df.fillna(0, inplace=True)
            rw.write_df(elapsed_times_df, self.edge.et_file, DF_FILE_TYPE)
        return tuple([self.result_key, elapsed_times_df, init_time])  # elapsed times are reported in number of timesteps

    def execute_callback(self, result):
        print(f'-     {self.name} ({round(self.length, 2)} nm) {mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.name} process has raised an error: {result}', flush=True)
