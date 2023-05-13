# C:\Users\jason\PycharmProjects\Transit-Time\venv\Scripts\python.exe C:\Users\jason\PycharmProjects\Transit-Time\main.py "East River" "C:\users\jason\Developer Workspace\GPX\East River West to East.gpx" 2023 -dd
from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager, Value
from numpy import ndarray as array
from pandas import DataFrame as dataframe
from sympy import Point

import multiprocess as mpm
from GPX import Route, Waypoint, Edge, CurrentStationWP, InterpolationWP, DataWP
from velocity import CurrentStationJob, InterpolationJob, InterpolationDataJob, VelocityJob
from elapsed_time import ElapsedTimeJob
from elapsed_time_reduce import elapsed_time_reduce
from transit_time import TransitTimeMinimaJob
from project_globals import TIMESTEP, boat_speeds, Environment, ChartYear, file_exists

from Semaphore import SimpleSemaphore as Semaphore
from ChromeDriver import ChromeDriver as cd

from VelocityInterpolation import Interpolator as vi

checkmark = u'\N{check mark}'

def assign_verify_output_data(wp):
    wp.output_data = mpm.result_lookup[id(wp)]
    if isinstance(wp.output_data, array):
        print(f'{checkmark}     {wp.unique_name}', flush=True)
    else:
        print(f'X     {wp.unique_name}', flush=True)

if __name__ == '__main__':

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())
    envr = Environment(args)
    cyr = ChartYear(args)

    Waypoint.velocity_folder = envr.velocity_folder()
    Waypoint.start = cyr.waypoint_start_index()
    Waypoint.end = cyr.waypoint_end_index()
    Edge.elapsed_time_folder = envr.elapsed_time_folder()
    mp_year = Value('i', cyr.year())
    mp_wp_si = Value('f', cyr.waypoint_start_index())
    mp_wp_ei = Value('f', cyr.waypoint_end_index())

    # Assemble route and route objects
    route = Route(args['filepath'])

    print(f'\nCalculating route "{route.name}"')
    print(f'total waypoints: {len(route.whole_path.edges)+1}')
    print(f'elapsed time waypoints: {len(route.velo_path.edges)+1}')
    print(f'timestep: {TIMESTEP}')
    print(f'boat speeds: {boat_speeds}')
    print(f'length {round(route.velo_path.length,1)} nm')
    print(f'direction {route.velo_path.direction}\n')

    mgr = Manager()
    mpm.result_lookup = mgr.dict()
    jm = mpm.WaitForProcess(target=mpm.JobManager, args=(mpm.job_queue, mpm.result_lookup))
    jm.start()

    cd.update_driver()  # update chrome driver before launching process that use it

    # Download noaa data and create velocity arrays for each CURRENT waypoint
    print(f'\nDownloading and processing currents at CURRENT and INTERPOLATION DATA waypoints (1st day-1 to last day+3)', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, DataWP):  # DataWP must come before CurrentStationWP because DataWP IS A CurrentStationWP
            mpm.job_queue.put(InterpolationDataJob(mp_year, mp_wp_si, mp_wp_ei, wp))
        elif isinstance(wp, CurrentStationWP):
            mpm.job_queue.put(CurrentStationJob(mp_year, mp_wp_si, mp_wp_ei, wp, TIMESTEP))
    mpm.job_queue.join()

    print(f'\nAdding results to waypoints', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, CurrentStationWP) or isinstance(wp, DataWP):
            assign_verify_output_data(wp)

    # Calculate the approximation of the velocity at interpolation points
    print(f'\nApproximating the velocity at INTERPOLATION waypoints (1st day-1 to last day+3)', flush=True)
    for group in route.interpolation_groups:
        interpolation_pt = group[0]

        if not file_exists(interpolation_pt.interpolation_data_file):
            group_range = range(len(group[1].output_data))
            for i in group_range: mpm.job_queue.put(InterpolationJob(group, i))  # (group, i, True) to display results
            mpm.job_queue.join()

            wp_data = [mpm.result_lookup[str(id(interpolation_pt)) + '_' + str(i)][2].evalf() for i in group_range]
            InterpolationJob.write_dataframe(interpolation_pt, wp_data)

        mpm.job_queue.put(CurrentStationJob(mp_year, mp_wp_si, mp_wp_ei, interpolation_pt, TIMESTEP))
        mpm.job_queue.join()

        if isinstance(interpolation_pt, InterpolationWP):
            assign_verify_output_data(interpolation_pt)

    # Calculate the number of timesteps to get from the start of the edge to the end of the edge
    print(f'\nCalculating elapsed times for edges (1st day-1 to last day+2)')
    for edge in route.velo_path.edges: mpm.job_queue.put(ElapsedTimeJob(envr, cyr, edge))
    mpm.job_queue.join()

    print(f'\nAdding results to edges')
    for edge in route.velo_path.edges:
        edge.dataframe = mpm.result_lookup[id(edge)]
        if isinstance(edge.dataframe, dataframe): print(f'{checkmark}     {edge.name}', flush=True)
        else: print(f'X     {edge.name}', flush=True)

    # combine elapsed times by speed
    print(f'\nSorting elapsed times by speed', flush=True)
    elapsed_time_reduce(envr, route)

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit times (1st day-1 to last day+1)')
    for speed in boat_speeds: mpm.job_queue.put(TransitTimeMinimaJob(envr, cyr, route, speed))
    mpm.job_queue.join()

    Semaphore.off(mpm.job_manager_semaphore)
