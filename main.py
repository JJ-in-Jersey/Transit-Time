from argparse import ArgumentParser as argParser
from pathlib import Path

from tt_gpx.gpx import Route, Waypoint, Edge, EdgeNode, GpxFile
from tt_file_tools.file_tools import read_df, write_df, print_file_exists
from tt_chrome_driver import chrome_driver
from tt_job_manager.job_manager import JobManager
from tt_globals.globals import Globals

from waypoint_processing import waypoint_processing
from elapsed_time import edge_processing
from transit_time import transit_time_processing

from pandas import concat

# import validations

if __name__ == '__main__':

    # ---------- PARSE ARGUMENTS ----------

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    ap.add_argument('-er', '--east_river', action='store_true')
    ap.add_argument('-cdc', '--chesapeake_delaware_canal', action='store_true')
    args = vars(ap.parse_args())

    # ---------- SET UP GLOBALS ----------

    Globals.initialize_dates(args)
    Globals.initialize_folders(args)
    Globals.initialize_structures()
    
    Waypoint.waypoints_folder = Globals.WAYPOINTS_FOLDER
    Edge.edges_folder = Globals.EDGES_FOLDER

    # ---------- ROUTE OBJECT ----------

    gpx_file = GpxFile(args['filepath'])

    if gpx_file.type == Globals.TYPE['rte']:
        route = Route(gpx_file.tree)
    else:
        route = None

    route.location_name = args['filepath'].stem
    route.location_code = args['project_name']

    print(f'\nCalculating {gpx_file.type} {route.location_name}')
    print(f'code {route.location_code}')
    print(f'calendar year: {Globals.YEAR}')
    print(f'start date: {Globals.FIRST_DAY_DATE}')
    print(f'end date: {Globals.LAST_DAY_DATE}')
    print(f'total waypoints: {len(route.waypoints)}')
    print(f'total edge nodes: {len(list(filter(lambda w: isinstance(w, EdgeNode), route.waypoints)))}')
    print(f'total edges: {len(route.edges)}')
    print(f'boat speeds: {Globals.BOAT_SPEEDS}')
    print(f'length {round(route.edge_path.length, 1)} nm')
    print(f'direction {route.edge_path.direction}')
    print(f'heading {route.edge_path.route_heading}\n')

    Globals.TRANSIT_TIMES_FOLDER.joinpath(str(route.edge_path.route_heading) + '.heading').touch()

    # ---------- CHECK CHROME ----------
    chrome_driver.check_driver()
    if chrome_driver.installed_driver_version is None or chrome_driver.latest_stable_version > chrome_driver.installed_driver_version:
        chrome_driver.install_stable_driver()

    # ---------- START MULTIPROCESSING ----------
    job_manager = JobManager()

    # ---------- WAYPOINT PROCESSING ----------
    waypoint_processing(route, job_manager)

    # ---------- EDGE PROCESSING ----------
    edge_processing(route, job_manager)

    # ---------- TRANSIT TIMES ----------
    transit_time_processing(job_manager, route)

    aggregate_transit_time_df = concat([read_df(route.rounded_transit_time_csv_to_speed[key]) for key in route.rounded_transit_time_csv_to_speed.keys()])
    aggregate_transit_time_df.drop(['idx', 'arc_round_angle'], axis=1, inplace=True)
    transit_time_df = (aggregate_transit_time_df
                       .drop(['start_round_angle', 'min_round_angle', 'end_round_angle'], axis=1)
                       .rename({'start_round_datetime': 'start', 'min_round_datetime': 'best', 'end_round_datetime': 'end'}))
    arc_df = (aggregate_transit_time_df
              .drop(['start_round_datetime', 'min_round_datetime', 'end_round_datetime'], axis=1)
              .rename({'start_round_angle': 'start', 'min_round_angle': 'best', 'end_round_angle': 'end'}))

    print_file_exists(write_df(transit_time_df, Globals.TRANSIT_TIMES_FOLDER.joinpath(args['project_name'] + '_transit_times.csv')))
    print_file_exists(write_df(arc_df, Globals.TRANSIT_TIMES_FOLDER.joinpath(args['project_name'] + '_arcs.csv')))

    # # if args['east_river']:
    # #     print(f'\nEast River validation')
    # #
    # #     validation_frame = pd.DataFrame()
    # #
    # #     frame = validations.hell_gate_validation()
    # #     validation_frame = pd.concat([validation_frame, frame])
    # #
    # #     frame = validations.battery_validation()
    # #     validation_frame = pd.concat([validation_frame, frame])
    # #
    # #     validation_frame.sort_values(['start_date'], ignore_index=True, inplace=True)
    # #     write_df(validation_frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('east_river_validation.csv'))
    # #
    # # if args['chesapeake_delaware_canal']:
    # #     print(f'\nChesapeake Delaware Canal validation')
    # #
    # #     validation_frame = pd.DataFrame()
    # #
    # #     frame = validations.chesapeake_city_validation()
    # #     validation_frame = pd.concat([validation_frame, frame])
    # #
    # #     frame = validations.reedy_point_tower_validation()
    # #     validation_frame = pd.concat([validation_frame, frame])
    # #
    # #     validation_frame.sort_values(['start_date'], ignore_index=True, inplace=True)
    # #     write_df(validation_frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('chesapeake_delaware_validation.csv'))

    print(f'\nProcess Complete')

    job_manager.stop_queue()
