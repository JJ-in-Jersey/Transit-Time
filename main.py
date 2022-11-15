from argparse import ArgumentParser as argParser
from pathlib import Path
from multiprocessing import Manager

import project_globals
import multiprocess
from route_objects import GpxRoute
from velocity import VelocityJob

if __name__ == '__main__':

    multiprocess.mgr = Manager()
    multiprocess.obj_lookup = multiprocess.mgr.dict()
    print(type(multiprocess.obj_lookup))
    multiprocess.som.start(multiprocess.pm_init)
    multiprocess.object_lookup = multiprocess.mgr.dict()
    multiprocess.chart_yr = multiprocess.som.CY()
    multiprocess.d_dir = multiprocess.som.DD()
    multiprocess.jm.start()

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    args = vars(ap.parse_args())

    multiprocess.d_dir.set_project_name(args['project_name'])
    multiprocess.chart_yr.set_year(args['year'])

    # Build route and linked list of waypoint nodes
    route = GpxRoute(args['filepath'])
    # Download noaa data and create velocity arrays for each waypoint (node)
    node = route.first_route_node()
    vj = VelocityJob(node, multiprocess.chart_yr, multiprocess.d_dir, multiprocess.pool_notice)
    multiprocess.job_queue.put(vj)
    multiprocess.job_queue.join()
    #result = vj.execute()
    #print(result)

    #for n in route.route_nodes():
        #n.velocity_job().execute_callback(n.velocity_job().execute())
        #project_globals.job_queue.put(n.velocity_job())

    #
    # for n in route.route_nodes():
    #     print(n.name(), len(n.velocity_array()))
    #
    # print(f'Route length is {round(route.length(),3)} nautical miles')
    # print(f'Route direction is {route.direction()}')

    multiprocess.som.shutdown()
    if multiprocess.jm.is_alive(): multiprocess.jm.terminate()
