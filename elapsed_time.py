import numpy as np
import pandas as pd

from tt_gpx.gpx import Edge
from tt_file_tools import file_tools as ft
from tt_job_manager.job_manager import Job
from project_globals import TIMESTEP, sign

#  Elapsed times are reported in number of timesteps


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


class ElapsedTimeDataframe:

    @staticmethod
    def distance(water_vf, water_vi, boat_speed, ts_in_hr):
        return ((water_vf + water_vi) / 2 + boat_speed) * ts_in_hr  # distance is nm

    def __init__(self, name, folder, init_velos, final_velos, edge_range, length, speed):

        filename = name + '_' + str(speed)
        et_path = folder.joinpath(filename)

        if ft.csv_npy_file_exists(et_path):
            self.dataframe = ft.read_df(et_path)
        else:
            self.dataframe = pd.DataFrame(data={'departure_index': edge_range})
            dist = ElapsedTimeDataframe.distance(final_velos[1:], init_velos[:-1], speed, TIMESTEP/3600)
            dist = np.insert(dist, 0, 0.0)  # distance uses an offset calculation VIx, VFx+1, need a zero at the beginning
            self.dataframe[name] = [elapsed_time(i, dist, sign(speed)*length) for i in range(len(edge_range))]
            self.dataframe.fillna(0, inplace=True)
            ft.write_df(self.dataframe, et_path)


class ElapsedTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, edge: Edge, speed):
        job_name = edge.unique_name + '_' + str(speed)
        result_key = job_name
        init_velo = edge.start.spline_fit_data['velocity'].to_numpy(dtype='float16')
        final_velo = edge.end.spline_fit_data['velocity'].to_numpy(dtype='float16')
        arguments = tuple([edge.unique_name, edge.folder, init_velo, final_velo, edge.edge_range, edge.length, speed])
        super().__init__(job_name, result_key, ElapsedTimeDataframe, arguments)
