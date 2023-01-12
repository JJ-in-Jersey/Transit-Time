from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager

import multiprocess as mp
from route_objects import GpxRoute
from velocity import VelocityJob
from elapsed_time import ElapsedTimeJob
from elapsed_time_reduce import elapsed_time_reduce
from transit_time import TransitTimeMinimaJob
from project_globals import TIMESTEP, boat_speeds, semaphore_off

if __name__ == '__main__':

    mgr = Manager()
    mp.result_lookup = mgr.dict()
    mp.som.start(mp.pm_init)
    mp.chart_yr = mp.som.CY()
    mp.environs = mp.som.ENV()

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())

    mp.environs.project_folder(args)
    mp.chart_yr.initialize(args)
    jm = mp.WaitForProcess(target=mp.JobManager, args=(mp.job_queue, mp.result_lookup))
    jm.start()

    # Assemble route and route objects
    route = GpxRoute(args['filepath'], mp.environs)
    print(f'Number of waypoints: {len(route.route_nodes())}')
    print(f'timestep: {TIMESTEP}')
    print(f'boat speeds: {boat_speeds}')
    print(f'length {round(route.length(),2)} nm')
    print(f'direction {route.direction()}')

    # Download noaa data and create velocity arrays for each waypoint (node)
    print(f'\nCalculating currents at waypoints (1st day-1 to last day+3)')
    for node in route.route_nodes(): mp.job_queue.put(VelocityJob(node, mp.chart_yr, mp.pool_notice))
    mp.job_queue.join()
    for node in route.route_nodes(): node.velocities(mp.result_lookup[id(node)])
    # node = route.route_nodes()[0]
    # vj = VelocityJob(node, mp.chart_yr, mp.pool_notice)
    # vj.execute()

    # Calculate the number of timesteps to get from the start of the edge to the end of the edge
    print(f'\nCalculating elapsed times for edges (1st day-1 to last day+2)')
    for segment in route.route_segments(): mp.job_queue.put(ElapsedTimeJob(segment, mp.chart_yr, mp.pool_notice))
    mp.job_queue.join()
    for segment in route.route_segments(): segment.elapsed_time_df(mp.result_lookup[id(segment)])
    # segment = route.route_segments()[0]
    # ej = ElapsedTimeJob(segment, mp.chart_yr, mp.pool_notice)
    # ej.execute()

    # combine elapsed times by speed
    print(f'\nSorting elapsed times by speed', flush=True)
    elapsed_time_reduce(route, mp.environs)
    # for speed in boat_speeds: print(route.elapsed_time_lookup(speed))

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit times (1st day-1 to last day+1)')
    # for speed in boat_speeds: mp.job_queue.put(TransitTimeMinimaJob(route, speed, mp.environs, mp.chart_yr, mp.pool_notice))
    # mp.job_queue.join()
    # for speed in boat_speeds: route.transit_time_lookup(speed, mp.result_lookup[speed])
    tj = TransitTimeMinimaJob(route, -9, mp.environs, mp.chart_yr, mp.pool_notice)
    tj.execute()

    semaphore_off(mp.job_manager_semaphore)
    mp.som.shutdown()
