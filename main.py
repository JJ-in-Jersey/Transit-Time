from argparse import ArgumentParser as argParser
from pathlib import Path

from tt_gpx.gpx import Route, Waypoint, Edge, EdgeNode, GpxFile
from tt_file_tools.file_tools import read_df
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

    arcs_df = concat([read_df(path) for path in [job_manager.get(key).filepath for key in keys]])
    #
    # # arcs_df.sort_values(['start_datetime', 'speed', 'idx'], ignore_index=True, inplace=True)
    # arcs_df.sort_values(['speed', 'start_datetime', 'idx'], ignore_index=True, inplace=True)
    #
    # rounded_arcs = arcs_df[['idx', 'start_round_datetime', 'min_round_datetime', 'end_round_datetime', 'speed']].copy()
    # rounded_arcs[args['project_name'] + ' date'] = pd.to_datetime(arcs_df['start_datetime']).dt.strftime("%m/%d/%Y")
    #
    # rounded_arcs.rename(columns={'idx': args['project_name'] + ' idx', 'start_round_datetime': args['project_name'] + ' start', 'min_round_datetime': args['project_name'] + ' best',
    #                              'end_round_datetime': args['project_name'] + ' end', 'speed': args['project_name'] + ' speed'}, inplace=True)
    # rounded_arcs[args['project_name'] + ' start'] = pd.to_datetime(rounded_arcs[args['project_name'] + ' start']).dt.strftime("%H:%M")
    # rounded_arcs[args['project_name'] + ' best'] = pd.to_datetime(rounded_arcs[args['project_name'] + ' best']).dt.strftime("%H:%M")
    # rounded_arcs[args['project_name'] + ' end'] = pd.to_datetime(rounded_arcs[args['project_name'] + ' end']).dt.strftime("%H:%M")
    #
    # # min_rotation_df = arcs_df[arcs_df['min_angle'].notna()]
    # # min_rotation_df = min_rotation_df.replace(to_replace='arc', value='min', regex=True)
    # # write_df(min_rotation_df, Globals.TRANSIT_TIMES_FOLDER.joinpath('minima.csv'))
    #
    # write_df(arcs_df, Globals.TRANSIT_TIMES_FOLDER.joinpath(args['project_name'] + ' arcs.csv'))
    # write_df(rounded_arcs, Globals.TRANSIT_TIMES_FOLDER.joinpath(args['project_name'] + ' rounded_arcs.csv'))
    #
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
