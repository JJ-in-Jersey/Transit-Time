from argparse import ArgumentParser as argParser
from pathlib import Path
from pandas import concat as concat
from tt_gpx.gpx import Route, Waypoint, Edge
from tt_file_tools import file_tools as ft
from tt_chrome_driver import chrome_driver
from tt_job_manager.job_manager import JobManager

from waypoint_processing import waypoint_processing
from edge_processing import edge_processing, check_edges

from east_river_validations import BatteryValidationDataframe, HellGateValidationDataframe, HornsHookValidationDataframe
from cape_cod_canal_validations import CapeCodCanalRailBridgeDataframe
from transit_time import TransitTimeJob, check_arcs
from project_globals import WINDOW_MARGIN, BOAT_SPEEDS, CHECKMARK, Environment, ChartYear


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

    env = Environment(args)
    cy = ChartYear(args)

    Waypoint.waypoints_folder = env.waypoint_folder
    Edge.elapsed_time_folder = env.elapsed_time_folder

    # ---------- ROUTE OBJECT ----------

    route = Route(args['filepath'], cy.edge_range())

    print(f'\nCalculating route "{route.filepath.stem}"')
    print(f'total waypoints: {len(route.waypoints)}')
    print(f'total edges: {len(route.elapsed_time_edges)}')
    print(f'transit time window: {WINDOW_MARGIN}')
    print(f'boat speeds: {BOAT_SPEEDS}')
    print(f'length {round(route.elapsed_time_path.length, 1)} nm')
    print(f'direction {route.elapsed_time_path.direction}')
    print(f'heading {route.elapsed_time_path.heading}\n')

    env.transit_time_folder.joinpath(str(route.elapsed_time_path.heading) + '.heading').touch()

    # ---------- START MULTIPROCESSING ----------

    job_manager = JobManager()

    # ---------- CHECK CHROME ----------

    chrome_driver.check_driver()
    if chrome_driver.latest_stable_version > chrome_driver.installed_driver_version:
        chrome_driver.install_stable_driver()

    # ---------- WAYPOINT PROCESSING ----------

    # if check_edges(env):
    #     waypoint_processing(route, cy, job_manager)
    waypoint_processing(route, cy, job_manager)

    # ---------- EDGE PROCESSING ----------

    # if check_arcs(env, cy.year()):
    edge_processing(route, env, cy, job_manager)

    # ---------- TRANSIT TIMES ----------

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit timesteps (1st day-1 to last day+2)')
    for speed in BOAT_SPEEDS:
        f_date = cy.first_day.date()
        l_date = cy.last_day.date()
        tt_range = cy.transit_range()
        tt_folder = env.transit_time_folder
        job = TransitTimeJob(speed, cy.year(), f_date, l_date, tt_range, route.elapsed_time_lookup[speed], tt_folder)
        job_manager.put(job)
    job_manager.wait()

    print(f'\nAdding transit time speed results to route')
    for speed in BOAT_SPEEDS:
        result = job_manager.get(speed)
        route.transit_time_lookup[speed] = result.frame
        print(f'{CHECKMARK}     tt {speed}', flush=True)

    arcs_df = concat([route.transit_time_lookup[key] for key in route.transit_time_lookup])
    arcs_df.sort_values(['start_date', 'name'], ignore_index=True, inplace=True)
    min_rotation_df = arcs_df[arcs_df['min_angle'].notna()]
    min_rotation_df = min_rotation_df.replace(to_replace=r'arc', value='min', regex=True)

    ft.write_df(min_rotation_df, env.transit_time_folder.joinpath('minima'))
    ft.write_df(arcs_df, env.transit_time_folder.joinpath('arcs'))

    if args['hell_gate']:
        print(f'\nHell Gate validation')
        path = list(filter(lambda wpt: not bool(wpt.unique_name.find('Hell_Gate')), route.waypoints))[0].downloaded_path
        frame = HellGateValidationDataframe(path, cy.first_day.date(), cy.last_day.date()).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('hell_gate_validation'))

    if args['battery']:
        print(f'\nEast River Battery validation')
        path = list(filter(lambda wpt: not bool(wpt.unique_name.find('NEW_YORK')), route.waypoints))[0].downloaded_path
        frame = BatteryValidationDataframe(path, cy.first_day.date(), cy.last_day.date()).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('battery_validation'))

    if args['horns_hook']:
        print(f'\nEast River Horns Hook validation')
        path = list(filter(lambda wpt: not bool(wpt.unique_name.find('Horns_Hook')), route.waypoints))[0].downloaded_path
        frame = HornsHookValidationDataframe(path, cy.first_day.date(), cy.last_day.date()).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('horns_hook_validation'))

    if args['cape_cod_canal']:
        print(f'\nCape Cod Canal Battery validation')
        path = list(filter(lambda wpt: not bool(wpt.unique_name.find('Cape_Cod_Canal_RR')), route.waypoints))[0].downloaded_path
        frame = CapeCodCanalRailBridgeDataframe(path, cy.first_day.date(), cy.last_day.date()).frame
        ft.write_df(frame, env.transit_time_folder.joinpath('cape_cod_canal_validation'))

    print(f'\nProcess Complete')

    job_manager.stop_queue()
