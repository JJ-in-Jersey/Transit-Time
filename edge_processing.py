from tt_file_tools import file_tools as ft
from tt_file_tools.file_tools import print_file_exists
from elapsed_time import ElapsedTimeJob
from tt_globals.globals import Globals


# def check_edges(env):
#
#     waypoint_processing_required = False
#     for s in Globals.BOAT_SPEEDS:
#         speed_path = env.elapsed_time_folder.joinpath('elapsed_timesteps_'+str(s) + '.csv')
#         if not speed_path.exists():
#             waypoint_processing_required = True
#     return waypoint_processing_required


def edge_processing(route, job_manager):

    print(f'\nCreating template elapsed time dataframe')

    for s in Globals.BOAT_SPEEDS:
        print(f'\nCalculating elapsed timesteps for edges at {s} kts')
        speed_path = Globals.EDGES_FOLDER.joinpath('elapsed_timesteps_'+str(s) + '.csv')
        elapsed_time_df = Globals.TEMPLATE_ELAPSED_TIME_DATAFRAME.copy(deep=True)

        if not speed_path.exists():
            keys = [job_manager.put(ElapsedTimeJob(edge, s)) for edge in route.edges]
            job_manager.wait()
            filepaths = [job_manager.get(key).filepath for key in keys]
            for path in filepaths:
                print_file_exists(path)

            print(f'\nAggregating elapsed timesteps at {s} kts into a dataframe', flush=True)
            for path in filepaths:
                elapsed_time_df = elapsed_time_df.merge(ft.read_df(path).drop(['date_time'], axis=1), on='departure_index')
            ft.write_df(elapsed_time_df, speed_path)

        print_file_exists(speed_path)
