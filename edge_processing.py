import pandas as pd
from tt_file_tools import file_tools as ft
from tt_file_tools.file_tools import print_file_exists
from elapsed_time import ElapsedTimeJob
from tt_globals.globals import Globals


def check_edges(env):

    waypoint_processing_required = False
    for s in Globals.BOAT_SPEEDS:
        speed_path = env.elapsed_time_folder.joinpath('elapsed_timesteps_'+str(s) + '.csv')
        if not speed_path.exists():
            waypoint_processing_required = True
    return waypoint_processing_required


def edge_processing(route, job_manager):

    for s in Globals.BOAT_SPEEDS:
        speed_path = Globals.EDGES_FOLDER.joinpath('elapsed_timesteps_'+str(s) + '.csv')
        elapsed_time_df = pd.DataFrame(data={'departure_index': Globals.ELAPSED_TIME_INDEX_RANGE})  # add departure_index as the join column

        print(f'\nCalculating elapsed timesteps for edges at {s} kts')
        if not speed_path.exists():
            keys = [job_manager.put(ElapsedTimeJob(edge, s)) for edge in route.edges]

            # for edge in route.edges:
            #     job = ElapsedTimeJob(edge, s)
            #     job.execute()

            job_manager.wait()
            filepaths = [job_manager.get(key).filepath for key in keys]
            for path in filepaths:
                print_file_exists(path)

            print(f'\nAggregating elapsed timesteps at {s} kts into a dataframe', flush=True)
            for path in filepaths:
                elapsed_time_df = elapsed_time_df.merge(ft.read_df(path).drop(['date_time'], axis=1), on='departure_index')

            elapsed_time_df.drop(['departure_index'], axis=1, inplace=True)
            ft.write_df(elapsed_time_df, speed_path)

        print_file_exists(speed_path)
