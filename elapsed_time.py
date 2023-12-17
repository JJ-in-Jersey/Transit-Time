import numpy as np
import pandas as pd

from tt_gpx.gpx import Edge
from tt_file_tools import file_tools as ft
from tt_job_manager.job_manager import Job
from project_globals import TIMESTEP

#  Elapsed times are reported in number of timesteps


def elapsed_time(distance_start_index, distances, length):  # returns number of timesteps
    distance_index = distance_start_index + 1  # distance at departure time is 0
    count = total = 0
    not_there_yet = True
    while not_there_yet:
        total += abs(distances[distance_index])
        count += 1
        distance_index += 1
        not_there_yet = True if length > 0 and total < length else False
    return count  # count = number of time steps


class ElapsedTimeDataframe:

    @staticmethod
    def distance(water_vf, water_vi, boat_speed, ts_in_hr):
        return ((water_vf + water_vi) / 2 + boat_speed) * ts_in_hr  # distance is nm

    def __init__(self, name, folder, init_velos, final_velos, edge_range, length, speed):

        filename = name + '_' + str(speed) + '.csv'
        et_path = folder.joinpath(filename)

        if et_path.exists():
            self.frame = ft.read_df(et_path)
        else:
            self.frame = pd.DataFrame(data={'departure_index': edge_range})
            dist = ElapsedTimeDataframe.distance(final_velos[1:], init_velos[:-1], speed, TIMESTEP/3600)
            dist = np.insert(dist, 0, 0.0)  # distance uses an offset calculation VIx, VFx+1, need a zero at the beginning
            self.frame[filename] = [elapsed_time(i, dist, length) for i in range(len(edge_range))]
            self.frame.fillna(0, inplace=True)
            ft.write_df(self.frame, et_path)


class ElapsedTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, edge: Edge, speed):
        job_name = edge.unique_name + ' ' + str(round(edge.length, 3)) + ' ' + str(speed)
        result_key = edge.unique_name + '_' + str(speed)
        start_velocity = ft.read_df(edge.start.folder.joinpath('harmonic_velocity_spline_fit.csv'))
        init_velo = start_velocity['velocity'].to_numpy()
        end_velocity = ft.read_df(edge.end.folder.joinpath('harmonic_velocity_spline_fit.csv'))
        final_velo = end_velocity['velocity'].to_numpy()
        arguments = tuple([edge.unique_name, edge.folder, init_velo, final_velo, edge.edge_range, edge.length, speed])
        super().__init__(job_name, result_key, ElapsedTimeDataframe, arguments)
