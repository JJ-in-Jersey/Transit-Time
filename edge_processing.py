import pandas as pd
from tt_file_tools import file_tools as ft
from tt_file_tools.file_tools import print_file_exists
from elapsed_time import ElapsedTimeJob
from tt_globals.globals import Globals
from tt_date_time_tools.date_time_tools import index_to_date


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
    template_path = Globals.PROJECT_FOLDER.joinpath('elapsed_timesteps_template.csv')
    if template_path.exists():
        template_df = ft.read_df(template_path)
    else:
        template_df = pd.DataFrame(data={'departure_index': Globals.ELAPSED_TIME_INDEX_RANGE})  # add departure_index as the join column
        template_df['date_time'] = template_df['departure_index'].apply(index_to_date)
        ft.write_df(template_df, template_path)
    print_file_exists(template_path)

    for s in Globals.BOAT_SPEEDS:
        print(f'\nCalculating elapsed timesteps for edges at {s} kts')
        speed_path = Globals.EDGES_FOLDER.joinpath('elapsed_timesteps_'+str(s) + '.csv')
        elapsed_time_df = template_df.copy(deep=True)

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
