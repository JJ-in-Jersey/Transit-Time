from argparse import ArgumentParser as argParser
from pathlib import Path

import project_globals
import multiprocess
from route_objects import GpxRoute
from velocity import VelocityJob

if __name__ == '__main__':
    project_globals.shared_object_manager = multiprocess.SharedObjectManager()
    project_globals.shared_object_manager.start(multiprocess.pm_init)
    project_globals.chart_year = project_globals.shared_object_manager.CY()
    project_globals.download_dir = project_globals.shared_object_manager.DD()
    #jm = multiprocess.WaitForProcess(target=multiprocess.JobManager, args=(project_globals.job_queue,))
    #jm.start()

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    args = vars(ap.parse_args())

    project_globals.download_dir.set_project_name(args['project_name'])
    project_globals.chart_year.set_year(args['year'])

    # Build route and linked list of waypoint objects
    # Calculate the distances between waypoints
    # Download noaa data and create velocity arrays for each waypoint
    route = GpxRoute(args['filepath'])
    for n in route.route_nodes():
        vj = VelocityJob(n, project_globals.chart_year, project_globals.download_dir)
        #project_globals.job_queue.put(vj)
        vj.execute_callback(vj.execute())
        print(n.name(), len(n.velocity_array()))

    print(f'Route length is {round(route.length(),3)} nautical miles')
    print(f'Route direction is {route.direction()}')

    project_globals.shared_object_manager.shutdown()
    #if jm.is_alive(): jm.terminate()
