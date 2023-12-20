from argparse import ArgumentParser as argParser
from pathlib import Path
from pandas import concat as concat
from tt_gpx.gpx import Route, Waypoint, Edge, EdgeNode, CurrentStationWP
from tt_file_tools.file_tools import write_df, read_df
from tt_chrome_driver import chrome_driver
from tt_job_manager.job_manager import JobManager
from tt_globals.globals import Globals

from waypoint_processing import waypoint_processing
from edge_processing import edge_processing

from east_river_validations import BatteryValidationDataframe, HellGateCurrentValidationDataframe, HornsHookValidationDataframe
from cape_cod_canal_validations import CapeCodCanalRailBridgeDataframe
from transit_time import TransitTimeJob

if __name__ == '__main__':

    # ---------- PARSE ARGUMENTS ----------

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    ap.add_argument('-hg', '--hell_gate', action='store_true')
    ap.add_argument('-bat', '--battery', action='store_true')
    ap.add_argument('-hh', '--horns_hook', action='store_true')
    ap.add_argument('-ccc', '--cape_cod_canal', action='store_true')
    args = vars(ap.parse_args())

    # ---------- SET UP GLOBALS ----------

    Globals.initialize_dates(args)
    Globals.initialize_folders(args)

    Waypoint.waypoints_folder = Globals.WAYPOINTS_FOLDER
    Edge.edges_folder = Globals.EDGES_FOLDER

    # ---------- ROUTE OBJECT ----------

    route = Route(args['filepath'])

    print(f'\nCalculating route "{route.filepath.stem}"')
    print(f'total waypoints: {len(route.waypoints)}')
    print(f'total edge nodes: {len(list(filter(lambda w: isinstance(w, EdgeNode), route.waypoints)))}')
    print(f'total edges: {len(route.edges)}')
    print(f'transit time window: {Globals.WINDOW_MARGIN}')
    print(f'boat speeds: {Globals.BOAT_SPEEDS}')
    print(f'length {round(route.edge_path.length, 1)} nm')
    print(f'direction {route.edge_path.direction}')
    print(f'heading {route.edge_path.heading}\n')

    Globals.TRANSIT_TIMES_FOLDER.joinpath(str(route.edge_path.heading) + '.heading').touch()

    # ---------- START MULTIPROCESSING ----------

    job_manager = JobManager()

    # ---------- CHECK CHROME ----------

    # chrome_driver.check_driver()
    # if chrome_driver.latest_stable_version > chrome_driver.installed_driver_version:
    #     chrome_driver.install_stable_driver()

    # ---------- WAYPOINT PROCESSING ----------

    # if check_edges(env):
    #     waypoint_processing(route, cy, job_manager)
    waypoint_processing(route, job_manager)

    # ---------- EDGE PROCESSING ----------

    # if check_arcs(env, cy.year):
    edge_processing(route, job_manager)

    # ---------- TRANSIT TIMES ----------

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit timesteps')
    keys = [job_manager.put(TransitTimeJob(speed, Globals.EDGES_FOLDER.joinpath('elapsed_timesteps_' + str(speed) + '.csv'), Globals.TRANSIT_TIMES_FOLDER)) for speed in Globals.BOAT_SPEEDS]
    # for speed in Globals.BOAT_SPEEDS:
    #     job = TransitTimeJob(speed, Globals.EDGES_FOLDER.joinpath('elapsed_timesteps_' + str(speed) + '.csv'), Globals.TRANSIT_TIMES_FOLDER)
    #     job.execute()
    job_manager.wait()

    # frames = [read_df(path) for path in [job_manager.get(key).filepath for key in keys]]

    arcs_df = concat([read_df(path) for path in [job_manager.get(key).filepath for key in keys]])
    arcs_df.sort_values(['start_date', 'name'], ignore_index=True, inplace=True)
    min_rotation_df = arcs_df[arcs_df['min_angle'].notna()]
    min_rotation_df = min_rotation_df.replace(to_replace=r'arc', value='min', regex=True)

    write_df(min_rotation_df, Globals.TRANSIT_TIMES_FOLDER.joinpath('minima.csv'))
    write_df(arcs_df, Globals.TRANSIT_TIMES_FOLDER.joinpath('arcs.csv'))

    if args['hell_gate']:
        print(f'\nHell Gate validations')
        folder = list(filter(lambda w: 'Hell_Gate_Current' in w.unique_name, filter(lambda w: isinstance(w, CurrentStationWP), route.waypoints)))[0]
        frame = HellGateCurrentValidationDataframe(folder, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        write_df(frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('hell_gate_current_validation.csv'))

        # path = list(filter(lambda w: 'Hell_Gate_Tide' in w.unique_name, filter(lambda w: isinstance(w, TideStationWP), route.waypoints)))[0].folder.joinpath('tide.csv')
        # frame = HellGateTideValidationDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        # write_df(frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('hell_gate_tide_validation.csv'))

    if args['battery']:
        print(f'\nEast River Battery validation')
        path = list(filter(lambda w: 'NEW_YORK' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = BatteryValidationDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        write_df(frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('battery_validation.csv'))

    if args['horns_hook']:
        print(f'\nEast River Horns Hook validation')
        path = list(filter(lambda w: 'Horns_Hook' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = HornsHookValidationDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        write_df(frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('horns_hook_validation.csv'))

    if args['cape_cod_canal']:
        print(f'\nCape Cod Canal Battery validation')
        path = list(filter(lambda w: 'Cape_Cod_Canal_RR' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = CapeCodCanalRailBridgeDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        write_df(frame, Globals.TRANSIT_TIMES_FOLDER.joinpath('cape_cod_canal_validation.csv'))

    print(f'\nProcess Complete')

    job_manager.stop_queue()
