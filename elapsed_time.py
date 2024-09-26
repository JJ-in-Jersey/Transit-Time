import numpy as np
import pandas as pd
from pathlib import Path

from tt_gpx.gpx import Edge, Route
from tt_date_time_tools.date_time_tools import index_to_date
from tt_job_manager.job_manager import Job
from tt_globals.globals import Globals
from tt_file_tools.file_tools import print_file_exists, read_df, write_df


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

    def __init__(self, folder: Path, init_velos, final_velos, edge_range, length, speed):

        self.filepath = None
        filename = folder.name + '_' + str(speed) + '.csv'
        filepath = folder.joinpath(filename)

        if print_file_exists(filepath):
            self.filepath = filepath
        else:
            frame = pd.DataFrame(data={'departure_index': edge_range, 'date_time': index_to_date(edge_range)})
            dist = ElapsedTimeDataframe.distance(final_velos[1:], init_velos[:-1], speed, Globals.TIMESTEP / 3600)
            # noinspection PyTypeChecker
            dist = np.insert(dist, 0, 0.0)  # distance uses an offset calculation VIx, VFx+1, need a zero at the beginning
            frame[filename] = [elapsed_time(i, dist, length) for i in range(len(edge_range))]
            self.filepath = write_df(frame, filepath)
            print_file_exists(self.filepath)


class ElapsedTimeJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, edge: Edge, speed):
        job_name = edge.unique_name + ' ' + str(round(edge.length, 3)) + ' ' + str(speed)
        result_key = edge.unique_name + '_' + str(speed)
        start_velocity = read_df(edge.start.folder.joinpath(Globals.EDGE_DATAFILE_NAME))
        init_velo = start_velocity['velocity'].to_numpy()
        end_velocity = read_df(edge.end.folder.joinpath(Globals.EDGE_DATAFILE_NAME))
        final_velo = end_velocity['velocity'].to_numpy()
        arguments = tuple([edge.folder, init_velo, final_velo, Globals.ELAPSED_TIME_INDEX_RANGE, edge.length, speed])
        super().__init__(job_name, result_key, ElapsedTimeDataframe, arguments)


def edge_processing(route: Route, job_manager):

    print(f'\nCreating template elapsed time dataframe')

    for s in Globals.BOAT_SPEEDS:
        print(f'\nCalculating elapsed timesteps for edges at {s} kts')
        speed_path = Globals.EDGES_FOLDER.joinpath('elapsed_timesteps_'+str(s) + '.csv')

        if not print_file_exists(speed_path):
            elapsed_time_df = Globals.TEMPLATE_ELAPSED_TIME_DATAFRAME.copy(deep=True)
            keys = [job_manager.put(ElapsedTimeJob(edge, s)) for edge in route.edges]
            # for edge in route.edges:
            #     job = ElapsedTimeJob(edge, s)
            #     result = job.execute()
            job_manager.wait()

            print(f'\nAggregating elapsed timesteps at {s} kts into a dataframe', flush=True)
            for path in [job_manager.get(key).filepath for key in keys]:
                elapsed_time_df = elapsed_time_df.merge(read_df(path).drop(['date_time'], axis=1), on='departure_index')

            write_df(elapsed_time_df, speed_path)
            print_file_exists(speed_path)

        print(f'Posting elapsed times paths to route for speed {s}')
        route.elapsed_time_csv_to_speed[s] = speed_path
