# C:\Users\jason\PycharmProjects\Transit-Time\venv\Scripts\python.exe C:\Users\jason\PycharmProjects\Transit-Time\main.py "ER" "C:\users\jason\Developer Workspace\GPX\East River West to East.gpx" 2023 -dd
# C:\Users\bronfelj\PycharmProjects\Transit-Time\venv\Scripts\python.exe C:\Users\bronfelj\PycharmProjects\Transit-Time\main.py "ER" "C:\users\bronfelj\Developer Workspace\GPX\East River West to East.gpx" 2023 -dd

from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager
from numpy import ndarray as array
import pandas as pd
from pandas import DataFrame as dataframe

from tt_gpx.gpx import Route, Waypoint, Edge, CurrentStationWP, InterpolationWP, DataWP
from tt_semaphore import simple_semaphore as semaphore
from tt_chrome_driver import chrome_driver as cd
from tt_file_tools import file_tools as ft

import multiprocess as mpm
from velocity import CurrentStationJob, InterpolationJob, InterpolationDataJob
from elapsed_time import ElapsedTimeJob
from dataframe_merge import elapsed_time_reduce
from transit_time import TransitTimeMinimaJob
from project_globals import TIMESTEP, boat_speeds, Environment, ChartYear


# from validations import EastRiverValidation

checkmark = u'\N{check mark}'

if __name__ == '__main__':

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())
    env = Environment(args)
    cy = ChartYear(args)

    cd.update_driver()  # update chrome driver before launching process that use it

    Waypoint.velocity_folder = env.velocity_folder()
    Edge.elapsed_time_folder = env.elapsed_time_folder()

    # Assemble route and route objects
    route = Route(args['filepath'], cy.waypoint_start_index(), cy.waypoint_end_index(), cy.edge_range())

    print(f'\nCalculating route "{route.name}"')
    print(f'total waypoints: {len(route.whole_path.edges)+1}')
    print(f'elapsed time waypoints: {len(route.elapsed_time_path.edges)+1}')
    print(f'timestep: {TIMESTEP}')
    print(f'boat speeds: {boat_speeds}')
    print(f'length {round(route.elapsed_time_path.length,1)} nm')
    print(f'direction {route.elapsed_time_path.direction}')
    print(f'heading {route.elapsed_time_path.heading}\n')

    # EastRiverValidation(route.waypoints)

    mgr = Manager()
    mpm.result_lookup = mgr.dict()
    jm = mpm.WaitForProcess(target=mpm.JobManager, args=(mpm.job_queue, mpm.result_lookup))
    jm.start()

    # Download noaa data and create velocity arrays for each CURRENT waypoint
    print(f'\nDownloading and processing currents at CURRENT and INTERPOLATION DATA waypoints (1st day-1 to last day+4)', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, DataWP):  # DataWP must come before CurrentStationWP because DataWP IS A CurrentStationWP
            mpm.job_queue.put(InterpolationDataJob(args['year'], wp))
            # idj = InterpolationDataJob(args['year'], wp)
            # idj.execute()
            # pass
        elif isinstance(wp, CurrentStationWP):
            mpm.job_queue.put(CurrentStationJob(args['year'], wp, TIMESTEP))
            # csj = CurrentStationJob(args['year'], wp, TIMESTEP)
            # csj.execute()
            # pass
    mpm.job_queue.join()

    print(f'\nAdding results to waypoints', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, CurrentStationWP) or isinstance(wp, DataWP):
            wp.output_data = mpm.result_lookup[id(wp)]
            if isinstance(wp.output_data, array): print(f'{checkmark}     {wp.unique_name}', flush=True)
            else: print(f'X     {wp.unique_name}', flush=True)

    # Calculate the approximation of the velocity at interpolation points
    if route.interpolation_groups is not None:
        print(f'\nApproximating the velocity at INTERPOLATION waypoints (1st day-1 to last day+4)', flush=True)
        for group in route.interpolation_groups:
            interpolation_pt = group[0]

            if not ft.file_exists(interpolation_pt.interpolation_data_file):
                group_range = range(len(group[1].output_data))
                for i in group_range: mpm.job_queue.put(InterpolationJob(group, i))  # (group, i, True) to display results
                mpm.job_queue.join()

                wp_data = [mpm.result_lookup[str(id(interpolation_pt)) + '_' + str(i)][2].evalf() for i in group_range]
                InterpolationJob.write_dataframe(interpolation_pt, wp_data)

            mpm.job_queue.put(CurrentStationJob(args['year'], interpolation_pt, TIMESTEP))
            mpm.job_queue.join()

            if isinstance(interpolation_pt, InterpolationWP):
                interpolation_pt.output_data = mpm.result_lookup[id(interpolation_pt)]
                if isinstance(interpolation_pt.output_data, array): print(f'{checkmark}     {interpolation_pt.unique_name}', flush=True)
                else: print(f'X     {interpolation_pt.unique_name}', flush=True)

    # Calculate the number of timesteps to get from the start of the edge to the end of the edge
    print(f'\nCalculating elapsed times for edges (1st day-1 to last day+3)')
    for edge in route.elapsed_time_path.edges:
        mpm.job_queue.put(ElapsedTimeJob(edge))
        # etj = ElapsedTimeJob(edge)
        # etj.execute()
    mpm.job_queue.join()

    print(f'\nAdding results to edges')
    for edge in route.elapsed_time_path.edges:
        edge.output_data = mpm.result_lookup[id(edge)]
        if isinstance(edge.output_data, dataframe): print(f'{checkmark}     {edge.unique_name}', flush=True)
        else: print(f'X     {edge.unique_name}', flush=True)

    # combine elapsed times by speed
    print(f'\nSorting elapsed times by speed', flush=True)
    elapsed_time_reduce(env.elapsed_time_folder(), route)

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit times (1st day-1 to last day+2)')
    for speed in boat_speeds: mpm.job_queue.put(TransitTimeMinimaJob(env, cy, route, speed))
    # tt = TransitTimeMinimaJob(env, cy, route, -3)
    # tt.execute()
    mpm.job_queue.join()

    file_name_header = str(cy.year()) + '_' + str(route.elapsed_time_path.heading) + '_'

    print(f'\nAdding transit time speed results to route')
    for speed in boat_speeds:
        route.transit_time_lookup[speed] = mpm.result_lookup[speed]
        if isinstance(route.transit_time_lookup[speed], dataframe): print(f'{checkmark}     tt {speed}', flush=True)
        else: print(f'X     tt {speed}', flush=True)

    if not ft.file_exists(env.transit_folder.joinpath(file_name_header + 'text_rotation')):
        text_rotation_df = pd.DataFrame(columns=['date', 'angle'])
        text_arcs_df = pd.concat([route.transit_time_lookup[7], route.transit_time_lookup[-7]])
        text_arcs_df.sort_values(['date', 'start'], ignore_index=True, inplace=True)
        for date in text_arcs_df['date'].drop_duplicates(ignore_index=True):
            date_df = text_arcs_df[text_arcs_df['date'] == date].sort_index().reset_index(drop=True)
            angle = (date_df.loc[1, 'start'] + date_df.loc[0, 'end'])/2
            text_rotation_df = pd.concat([text_rotation_df, pd.DataFrame.from_dict({'date': [date], 'angle': [angle]})])
            text_arcs_df = text_arcs_df[text_arcs_df['date'] != date]
        ft.write_df(text_rotation_df, env.transit_folder.joinpath(file_name_header + 'legend'))

    arcs_df = pd.concat([route.transit_time_lookup[key] for key in route.transit_time_lookup])
    arcs_df.sort_values(['date'], ignore_index=True, inplace=True)
    min_rotation_df = arcs_df[arcs_df['min'].notna()]
    min_rotation_df = min_rotation_df.rename(columns={'min': 'angle', 'name': 'base_name'})
    min_rotation_df['name'] = min_rotation_df['base_name'] + 'm'
    min_rotation_df = min_rotation_df.drop(['date_time', 'start', 'end'], axis=1)
    ft.write_df(min_rotation_df, env.transit_folder.joinpath(file_name_header + 'minima'))
    arcs_df.drop(['date_time', 'min'], axis=1, inplace=True)
    ft.write_df(arcs_df, env.transit_folder.joinpath(file_name_header + 'arcs'))

    semaphore.off(mpm.job_manager_semaphore)
