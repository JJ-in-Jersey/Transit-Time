# C:\Users\jason\PycharmProjects\Transit-Time\venv\Scripts\python.exe C:\Users\jason\PycharmProjects\Transit-Time\main.py ER "C:\users\jason\Developer Workspace\GPX\East River West to East.gpx" 2023 -dd
from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager

import multiprocess as mpm
from GPX import Route, CurrentStationWP, InterpolationWP
from velocity import CurrentStationJob, InterpolationJob
from elapsed_time import ElapsedTimeJob
from elapsed_time_reduce import elapsed_time_reduce
from transit_time import TransitTimeMinimaJob
from project_globals import TIMESTEP, boat_speeds, Environment, ChartYear

from Semaphore import SimpleSemaphore as Semaphore
from ChromeDriver import ChromeDriver as cd
from VelocityInterpolation import Interpolator as vi

if __name__ == '__main__':

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())

    # Assemble route and route objects
    route = Route(args['filepath'])

    print(f'\nCalculating route "{route.name}"')
    print(f'total waypoints:{len(route.waypoints)}')
    print(f'elapsed time waypoints:{len(route.elapsed_time_waypoints)}')
    print(f'elapsed time segments: {len(route.elapsed_time_segments)}')
    print(f'timestep: {TIMESTEP}')
    print(f'boat speeds: {boat_speeds}')
    print(f'length {round(route.path.total_length(),1)} nm')
    print(f'direction {route.path.direction()}\n')

    envr = Environment()
    cyr = ChartYear()
    envr.make_folders(args)
    cyr.initialize(args)

    mgr = Manager()
    mpm.result_lookup = mgr.dict()
    mpm.som.start(mpm.pm_init)
    mpm.cy = mpm.som.CY()
    mpm.env = mpm.som.ENV()

    mpm.env.make_folders(args)
    mpm.cy.initialize(args)
    jm = mpm.WaitForProcess(target=mpm.JobManager, args=(mpm.job_queue, mpm.result_lookup))
    jm.start()

    # cd.update_driver()  # update chrome driver before launching process that use it

    # Download noaa data and create velocity arrays for each waypoint (node)
    print(f'\nCalculating currents at waypoints (1st day-1 to last day+3)')
    current_stations = [wp for wp in route.waypoints if isinstance(wp, CurrentStationWP)]
    for wp in current_stations: mpm.job_queue.put(CurrentStationJob(mpm, wp))
    mpm.job_queue.join()
    print(f'\nAdding results to waypoints')
    for wp in current_stations:
        wp.velo_array = mpm.result_lookup[id(wp)]
        if not wp.velo_array == None: print( u'\N{check mark}', wp.short_name )


    # print(f'\nCalculating currents at interpolation waypoints (1st day-1 to last day+3)')
    # interpolations = [wp for wp in route.waypoints if isinstance(wp, InterpolationWP)]
    # for wp in interpolations: mpm.job_queue.put(InterpolationJob(mpm, wp))
    # mpm.job_queue.join()
    # for wp in interpolations: wp.velo_array(mpm.result_lookup[id(wp)])

    for segment in route.elapsed_time_segments:
        segment.add_endpoint_velocities()  # add velocities to segments

    # Calculate the number of timesteps to get from the start of the edge to the end of the edge
    print(f'\nCalculating elapsed times for segments (1st day-1 to last day+2)')
    for segment in route.elapsed_time_segments: mpm.job_queue.put(ElapsedTimeJob(mpm, segment))
    mpm.job_queue.join()
    for segment in route.elapsed_time_segments: segment.elapsed_times_df = mpm.result_lookup[id(segment)]

    # for segment in route.elapsed_time_segments:
    #     ej = ElapsedTimeJob(mpm, segment)
    #     ej.execute()
    #     mpm.job_queue.put(ej)
    #     mpm.job_queue.join()
    #     print(segment.elapsed_times_df)

    # combine elapsed times by speed
    print(f'\nSorting elapsed times by speed', flush=True)
    elapsed_time_reduce(mpm, route)
    # for speed in boat_speeds: print(route.elapsed_time_lookup(speed))

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit times (1st day-1 to last day+1)')
    for speed in boat_speeds: mpm.job_queue.put(TransitTimeMinimaJob(mpm, route, speed))
    mpm.job_queue.join()
    # for speed in boat_speeds: route.transit_time_lookup(speed, mp.result_lookup[speed])
    # tj = TransitTimeMinimaJob(route, -7, mp.environs, mp.chart_yr, mp.pool_notice)
    # tj.execute()

    Semaphore.off(mpm.job_manager_semaphore)
    mpm.som.shutdown()
