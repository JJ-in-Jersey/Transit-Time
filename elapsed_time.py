import numpy as np
import pandas as pd
from time import perf_counter

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

    def __init__(self, mpm, segment):
        self._result_key = id(segment)
        self._length = segment._length
        self._init_velo = segment._start_velo
        self._final_velo = segment._end_velo
        self._name = segment._name

        self._edge_folder = mpm.env.edge_folder(self._name)
        self._table_pathfile = self._edge_folder.joinpath(self._name + '_table')
        self._start_index = mpm.cy.edge_start_index()
        self._end_index = mpm.cy.edge_end_index()
        self._edge_range = mpm.cy.edge_range()

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self._table_pathfile):
            print(f'+     {self._name} ({round(self._length, 2)} nm)', flush=True)
            elapsed_times_df = rw.read_df(self._table_pathfile)
            return tuple([self._result_key, elapsed_times_df, init_time])
        else:
            print(f'+     {self._name} ({round(self._length, 2)} nm)', flush=True)
            elapsed_times_df = pd.DataFrame(data=self._edge_range, columns=['departure_index'])
            ts_in_hr = TIMESTEP / 3600  # in hours because NOAA speeds are in knots (nautical miles per hour)
            for s in boat_speeds:
                col_name = str(s) + ' ' + self._name
                dist = ElapsedTimeJob.distance(self._final_velo[1:], self._init_velo[:-1], s, ts_in_hr)  # distance in nm
                dist = np.insert(dist, 0, 0.0)  # because distance uses an offset calculation VIx VFx+1, we need to add a zero to the beginning
                elapsed_times_df[col_name] = [elapsed_time(i, dist, sign(s)*self._length) for i in range(0, len(self._edge_range))]
            elapsed_times_df.fillna(0, inplace=True)
            rw.write_df(elapsed_times_df, self._table_pathfile, DF_FILE_TYPE)
        return tuple([self._result_key, elapsed_times_df, init_time])  # elapsed times are reported in number of timesteps

    def execute_callback(self, result):
        print(f'-     {self._name} ({round(self._length, 2)} nm) {mins_secs(perf_counter() - result[2])} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     {self._name} process has raised an error: {result}', flush=True)
