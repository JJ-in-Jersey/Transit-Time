from argparse import ArgumentParser as argParser
from pathlib import Path
from pandas import concat as concat
from tt_gpx.gpx import Route, Waypoint, Edge, EdgeNode
from tt_file_tools import file_tools as ft
from tt_chrome_driver import chrome_driver
from tt_job_manager.job_manager import JobManager
from tt_globals.globals import Globals

from waypoint_processing import waypoint_processing
from edge_processing import edge_processing

from east_river_validations import BatteryValidationDataframe, HellGateValidationDataframe, HornsHookValidationDataframe
from cape_cod_canal_validations import CapeCodCanalRailBridgeDataframe
from transit_time import TransitTimeJob
from project_globals import Environment


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

    Globals.initialize_dates(args['year'])
    env = Environment(args)

    Waypoint.waypoints_folder = env.waypoint_folder
    Edge.elapsed_time_folder = env.elapsed_time_folder

    # ---------- ROUTE OBJECT ----------

    route = Route(args['filepath'])

    print(f'\nCalculating route "{route.filepath.stem}"')
    print(f'total waypoints: {len(route.waypoints)}')
    print(f'total edge nodes: {len(list(filter(lambda w: isinstance(w, EdgeNode), route.waypoints)))}')
    print(f'total edges: {len(route.elapsed_time_edges)}')
    print(f'transit time window: {Globals.WINDOW_MARGIN}')
    print(f'boat speeds: {Globals.BOAT_SPEEDS}')
    print(f'length {round(route.elapsed_time_path.length, 1)} nm')
    print(f'direction {route.elapsed_time_path.direction}')
    print(f'heading {route.elapsed_time_path.heading}\n')

    env.transit_time_folder.joinpath(str(route.elapsed_time_path.heading) + '.heading').touch()

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
    edge_processing(route, env, job_manager)

    # ---------- TRANSIT TIMES ----------

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit timesteps (1st day-1 to last day+2)')
    for speed in Globals.BOAT_SPEEDS:
        f_date = Globals.FIRST_DAY_DATE
        l_date = Globals.LAST_DAY_DATE
        tt_folder = env.transit_time_folder
        et_folder = env.elapsed_time_folder
        et_file = env.elapsed_time_folder.joinpath('elapsed_timesteps_' + str(speed) + '.csv')
        job = TransitTimeJob(speed, Globals.YEAR, f_date, l_date, Globals.TRANSIT_TIME_INDEX_RANGE, et_file, tt_folder)
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    print(f'\nAdding transit time speed results to route')
    for speed in Globals.BOAT_SPEEDS:
        result = job_manager.get(speed)
        route.transit_time_lookup[speed] = result.frame
        print(f'{Globals.CHECKMARK}     tt {speed}', flush=True)

    arcs_df = concat([route.transit_time_lookup[key] for key in route.transit_time_lookup])
    arcs_df.sort_values(['start_date', 'name'], ignore_index=True, inplace=True)
    min_rotation_df = arcs_df[arcs_df['min_angle'].notna()]
    min_rotation_df = min_rotation_df.replace(to_replace=r'arc', value='min', regex=True)

    ft.write_df(min_rotation_df, env.transit_time_folder.joinpath('minima'))
    ft.write_df(arcs_df, env.transit_time_folder.joinpath('arcs'))

    if args['hell_gate']:
        print(f'\nHell Gate validation')
        path = list(filter(lambda w: 'Hell_Gate' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = HellGateValidationDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('hell_gate_validation'))

    if args['battery']:
        print(f'\nEast River Battery validation')
        path = list(filter(lambda w: 'NEW_YORK' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = BatteryValidationDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('battery_validation'))

    if args['horns_hook']:
        print(f'\nEast River Horns Hook validation')
        path = list(filter(lambda w: 'Horns_Hook' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = HornsHookValidationDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('horns_hook_validation'))

    if args['cape_cod_canal']:
        print(f'\nCape Cod Canal Battery validation')
        path = list(filter(lambda w: 'Cape_Cod_Canal_RR' in w.unique_name, route.waypoints))[0].folder.joinpath('tide.csv')
        frame = CapeCodCanalRailBridgeDataframe(path, Globals.FIRST_DAY_DATE, Globals.LAST_DAY_DATE).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('cape_cod_canal_validation'))

    print(f'\nProcess Complete')

    job_manager.stop_queue()
