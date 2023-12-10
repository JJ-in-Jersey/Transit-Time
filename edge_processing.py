import pandas as pd
from tt_file_tools import file_tools as ft
from elapsed_time import ElapsedTimeJob
from project_globals import BOAT_SPEEDS, CHECKMARK


def edge_processing(route, env, cy, job_manager):

    for s in BOAT_SPEEDS:
        speed_path = env.elapsed_time_folder.joinpath('elapsed_timesteps_'+str(s))
        if ft.csv_npy_file_exists(speed_path):
            elapsed_time_df = ft.read_df(speed_path)
        else:
            print(f'\nCalculating elapsed timesteps for edges at {s} kts (1st day-1 to last day+3)')
            for edge in route.elapsed_time_path.edges:
                job_manager.put(ElapsedTimeJob(edge, s))
            job_manager.wait()

            print(f'\nAggregating elapsed timesteps at {s} kts into a dataframe', flush=True)
            elapsed_time_df = pd.DataFrame(data={'departure_index': cy.edge_range()})  # add departure_index as the join column
            for edge in route.elapsed_time_path.edges:
                result = job_manager.get(edge.unique_name + '_' + str(s))
                elapsed_time_df = elapsed_time_df.merge(result.frame, on='departure_index')
                print(f'{CHECKMARK}     {edge.unique_name} {s}', flush=True)
            elapsed_time_df.drop(['departure_index'], axis=1, inplace=True)
            ft.write_df(elapsed_time_df, speed_path)
        print(f'\nAdding {s} kt dataframe to route', flush=True)
        route.elapsed_time_lookup[s] = elapsed_time_df
