from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager

import multiprocess as mpm
from route_objects import Route
from velocity import VelocityJob
from elapsed_time import ElapsedTimeJob
from elapsed_time_reduce import elapsed_time_reduce
from transit_time import TransitTimeMinimaJob
from project_globals import TIMESTEP, boat_speeds

from Semaphore import SimpleSemaphore as Semaphore
from ChromeDriver import ChromeDriver as cd

if __name__ == '__main__':

    cd.update_driver()  # update chrome driver before launching process that use it

    mgr = Manager()
    mpm.result_lookup = mgr.dict()
    mpm.som.start(mpm.pm_init)
    mpm.cy = mpm.som.CY()
    mpm.env = mpm.som.ENV()

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())

    mpm.env.project_folder(args)
    mpm.cy.initialize(args)
    jm = mpm.WaitForProcess(target=mpm.JobManager, args=(mpm.job_queue, mpm.result_lookup))
    jm.start()

    # Assemble route and route objects
    route = Route(args['filepath'])
    # noinspection PyProtectedMember
    print(f'Number of waypoints: {len(route._waypoints)}')
    # noinspection PyProtectedMember
    print(f'Number of velocity waypoints: {len(route._velocity_waypoints)}')
    # noinspection PyProtectedMember
    print(f'Number of elapsed time segments: {len(route._elapsed_time_segments)}')
    print(f'timestep: {TIMESTEP}')
    print(f'boat speeds: {boat_speeds}')
    # noinspection PyProtectedMember
    print(f'length {route._path.total_length()} nm')
    # noinspection PyProtectedMember
    print(f'direction {route._path.direction()}')

    # Download noaa data and create velocity arrays for each waypoint (node)
    print(f'\nCalculating currents at waypoints (1st day-1 to last day+3)')
    # noinspection PyProtectedMember
    for waypoint in route._velocity_waypoints: mpm.job_queue.put(VelocityJob(mpm, waypoint))
    mpm.job_queue.join()
    # noinspection PyProtectedMember
    for waypoint in route._velocity_waypoints: waypoint.velo_array(mpm.result_lookup[id(waypoint)])
    # waypoint = route._velocity_waypoints[0]
    # vj = VelocityJob(mpm, waypoint)
    # result_tuple = vj.execute()
    # waypoint.velo_array(result_tuple[1])

    # noinspection PyProtectedMember
    for segment in route._elapsed_time_segments: segment.update()

    # Calculate the number of timesteps to get from the start of the edge to the end of the edge
    print(f'\nCalculating elapsed times for segments (1st day-1 to last day+2)')
    # noinspection PyProtectedMember
    for segment in route._elapsed_time_segments: mpm.job_queue.put(ElapsedTimeJob(mpm, segment))
    mpm.job_queue.join()
    # noinspection PyProtectedMember
    for segment in route._elapsed_time_segments: segment.elapsed_times_df(mpm.result_lookup[id(segment)])
    # segment = route._elapsed_time_segments[0]
    # ej = ElapsedTimeJob(mpm, segment)
    # ej.execute()

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
