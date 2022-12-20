from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager

import multiprocess as mp
from route_objects import GpxRoute
from velocity import VelocityJob
from elapsed_time import ElapsedTimeJob
from transit_time import TransitTimeMinimaJob
from project_globals import TIMESTEP

if __name__ == '__main__':

    mgr = Manager()
    mp.result_lookup = mgr.dict()
    mp.som.start(mp.pm_init)
    mp.chart_yr = mp.som.CY()
    mp.environs = mp.som.ENV()
    jm = mp.WaitForProcess(target=mp.JobManager, args=(mp.job_queue, mp.result_lookup))
    jm.start()

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())

    mp.environs.project_folder(args)
    mp.chart_yr.set_year(args)

    # Assemble route and route objects
    route = GpxRoute(args['filepath'])
    print(f'Number of waypoints: {len(route.route_nodes())}')
    print(f'timestep: {TIMESTEP}')

    # Download noaa data and create velocity arrays for each waypoint (node)
    print(f'\nCalculating velocities')
    for rn in route.route_nodes(): mp.job_queue.put(VelocityJob(rn, mp.chart_yr, mp.environs, mp.pool_notice))
    mp.job_queue.join()
    for rn in route.route_nodes(): rn.velocity_table(mp.result_lookup[id(rn)])
    # vj = VelocityJob(route.route_nodes()[0], mp.chart_yr, mp.environs, mp.pool_notice)
    # vj.execute()

    # Calculate the number of timesteps to get from the start of the edge to the end of the edge
    print(f'\nCalculating elapsed times')
    for re in route.route_edges(): mp.job_queue.put(ElapsedTimeJob(re, mp.chart_yr, mp.environs, mp.pool_notice))
    mp.job_queue.join()
    for re in route.route_edges(): re.elapsed_time_dataframe(mp.result_lookup[id(re)])
    # ej = ElapsedTimeJob(route.route_edges()[0], mp.chart_yr, mp.environs, mp.pool_notice)
    # ej.execute()

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit times')
    # for speed in boat_speeds: mp.job_queue.put(TransitTimeMinimaJob(route, speed, mp.environs, mp.chart_yr, mp.pool_notice))
    # mp.job_queue.join()
    # for speed in boat_speeds: route.transit_time_lookup(speed, mp.result_lookup[speed])
    tj = TransitTimeMinimaJob(route, -3, mp.environs, mp.chart_yr, mp.pool_notice)
    tj.execute()

    # # Aggregate the elapsed time information by speed rather than edge
    # print(f'\nAssigning elapsed times by speed')
    # route.elapsed_times_by_speed()

    mp.som.shutdown()
    if jm.is_alive(): jm.terminate()
