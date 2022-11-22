from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager

import multiprocess as mp
from route_objects import GpxRoute
from velocity import VelocityJob
from elapsed_time import ElapsedTimeJob

if __name__ == '__main__':

    mgr = Manager()
    mp.result_lookup = mgr.dict()
    mp.som.start(mp.pm_init)
    mp.chart_yr = mp.som.CY()
    mp.d_dir = mp.som.DD()
    jm = mp.WaitForProcess(target=mp.JobManager, args=(mp.job_queue, mp.result_lookup))
    jm.start()

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())

    mp.d_dir.project_folder(args)
    mp.chart_yr.set_year(args)

    # Build route and linked list of waypoint nodes
    route = GpxRoute(args['filepath'])

    # Download noaa data and create velocity arrays for each waypoint (node)
    for rn in route.route_nodes(): mp.job_queue.put(VelocityJob(rn, mp.chart_yr, mp.d_dir, mp.pool_notice))
    mp.job_queue.join()
    for rn in route.route_nodes(): rn.velocity_array(mp.result_lookup[id(rn)])

    ElapsedTimeJob(route.route_edges()[0],mp.chart_yr)



    mp.som.shutdown()
    if jm.is_alive(): jm.terminate()
