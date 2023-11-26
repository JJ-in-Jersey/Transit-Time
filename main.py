from argparse import ArgumentParser as argParser
from pathlib import Path

import pandas as pd

# noinspection PyPep8Naming
from pandas import concat as Concat

from tt_gpx.gpx import Route, Waypoint, Edge
from tt_gpx.gpx import TideWP, InterpolatedDataWP, CurrentStationWP, InterpolatedWP
from tt_file_tools import file_tools as ft
from tt_chrome_driver import chrome_driver
from tt_job_manager.job_manager import JobManager

# import multiprocess as mpm
from velocity import NoaaCurrentDownloadJob, SplineFitCurrentDataJob, interpolate_group
from battery_validation import TideStationJob
from elapsed_time import ElapsedTimeJob
from transit_time import TransitTimeJob
from project_globals import TIMESTEP, TIME_RESOLUTION, WINDOW_MARGIN, boat_speeds, Environment, ChartYear

from hell_gate_validation import HellGateSlackTimes

checkmark = u'\N{check mark}'

if __name__ == '__main__':

    # ---------- PARSE ARGUMENTS ----------

    ap = argParser()
    ap.add_argument('project_name', type=str, help='name of transit window project')
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    ap.add_argument('year', type=int, help='calendar year for analysis')
    ap.add_argument('-dd', '--delete_data', action='store_true')
    args = vars(ap.parse_args())

    # ---------- SET UP GLOBALS ----------

    env = Environment(args)
    cy = ChartYear(args)

    Waypoint.velocity_folder = env.velocity_folder
    Waypoint.current_folder = env.current_folder
    Edge.elapsed_time_folder = env.elapsed_time_folder

    # ---------- ROUTE OBJECT ----------

    route = Route(args['filepath'], cy.edge_range())

    print(f'\nCalculating route "{route.filepath.stem}"')
    print(f'total waypoints: {len(route.elapsed_time_wps)}')
    print(f'elapsed time waypoints: {len(route.elapsed_time_edges)}')
    print(f'timestep: {TIMESTEP}')
    print(f'chart resolution: {TIME_RESOLUTION}')
    print(f'transit time window: {WINDOW_MARGIN}')
    print(f'boat speeds: {boat_speeds}')
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

    # ---------- INTERPOLATION WAYPOINTS ----------

    print(f'\nDownloading current data for INTERPOLATED DATA WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, InterpolatedDataWP):
            job_manager.put(NoaaCurrentDownloadJob(wp, cy.year(), cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding downloaded data to INTERPOLATED DATA WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, InterpolatedDataWP):
            result = job_manager.get(id(wp))
            wp.downloaded_current_data = result.dataframe
            print(f'{checkmark}     {wp.unique_name}', flush=True)

    print(f'\nSpline fitting data for INTERPOLATED DATA WAYPOINTS', flush=True)
    # normalizing the time points for interpolation, don't want too many points, so using 3 hour timestep
    for wp in route.waypoints:
        if isinstance(wp, InterpolatedDataWP):
            job_manager.put(SplineFitCurrentDataJob(wp, cy.waypoint_start_index(), cy.waypoint_end_index(), 10800))  # 3 hour timestep
    job_manager.wait()

    print(f'\nAdding spline data to INTERPOLATED DATA WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, InterpolatedDataWP):
            result = job_manager.get(id(wp))
            wp.spline_fit_current_data = result.dataframe
            print(f'{checkmark}     {wp.unique_name}', flush=True)

    print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for group in route.interpolation_groups:
        interpolate_group(group, job_manager)

    print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, InterpolatedWP):
            job_manager.put(SplineFitCurrentDataJob(wp, cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding spline data to INTERPOLATED WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, InterpolatedWP):
            result = job_manager.get(id(wp))
            wp.spline_fit_current_data = result.dataframe
            print(f'{checkmark}     {wp.unique_name}', flush=True)

    # ---------- CURRENT STATION WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, CurrentStationWP):
            job_manager.put(NoaaCurrentDownloadJob(wp, cy.year(), cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding downloaded data to CURRENT STATION WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, CurrentStationWP):
            result = job_manager.get(id(wp))
            wp.downloaded_current_data = result.dataframe
            print(f'{checkmark}     {wp.unique_name}', flush=True)

    print(f'\nSpline fit data from CURRENT STATION WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, CurrentStationWP):
            job_manager.put(SplineFitCurrentDataJob(wp, cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding spline data to CURRENT STATION WAYPOINTS', flush=True)
    for wp in route.waypoints:
        if isinstance(wp, CurrentStationWP):
            result = job_manager.get(id(wp))
            wp.spline_fit_current_data = result.dataframe
            print(f'{checkmark}     {wp.unique_name}', flush=True)

    # ---------- ELAPSED TIME ----------

    for s in boat_speeds:
        speed_path = env.elapsed_time_folder.joinpath('elapsed_timesteps_'+str(s))
        if ft.csv_npy_file_exists(speed_path):
            elapsed_time_df = ft.read_df(speed_path)
        else:
            print(f'\nCalculating elapsed timesteps for edges at {s} kts (1st day-1 to last day+3)')
            for edge in route.elapsed_time_path.edges:
                job_manager.put(ElapsedTimeJob(edge, s))
            job_manager.wait()

            print(f'\nAggregating elapsed timesteps at {s} kts into a dataframe', flush=True)
            elapsed_time_df = pd.DataFrame(data={'departure_index': cy.edge_range()})  # add departure_index as the join column
            for edge in route.elapsed_time_path.edges:
                result = job_manager.get(edge.unique_name + '_' + str(s))
                elapsed_time_df = elapsed_time_df.merge(result.dataframe, on='departure_index')
                print(f'{checkmark}     {edge.unique_name} {s}', flush=True)
            elapsed_time_df.drop(['departure_index'], axis=1, inplace=True)
            ft.write_df(elapsed_time_df, speed_path)
        print(f'\nAdding {s} kt dataframe to route', flush=True)
        route.elapsed_time_lookup[s] = elapsed_time_df

    # ---------- TRANSIT TIMES ----------

    # calculate the number of timesteps from first node to last node
    print(f'\nCalculating transit timesteps (1st day-1 to last day+2)')
    for speed in boat_speeds:
        f_date = cy.first_day.date()
        l_date = cy.last_day.date()
        tt_range = cy.transit_range()
        tt_folder = env.transit_time_folder
        job_manager.put(TransitTimeJob(speed, cy.year(), f_date, l_date, tt_range, route.elapsed_time_lookup[speed], tt_folder))
    job_manager.wait()

    print(f'\nAdding transit time speed results to route')
    for speed in boat_speeds:
        result = job_manager.get(speed)
        route.transit_time_lookup[speed] = result.dataframe
        print(f'{checkmark}     tt {speed}', flush=True)

    arcs_df = Concat([route.transit_time_lookup[key] for key in route.transit_time_lookup])
    arcs_df.sort_values(['date', 'name'], ignore_index=True, inplace=True)
    min_rotation_df = arcs_df[arcs_df['min'].notna()]
    min_rotation_df['name'] = min_rotation_df['name'].apply(lambda name_string: name_string.replace('arc', 'min'))

    arcs_df.drop(['date_time', 'min'], axis=1, inplace=True)
    min_rotation_df.drop(['date_time', 'start', 'end'], axis=1, inplace=True)
    ft.write_df(arcs_df, env.transit_time_folder.joinpath('arcs'))
    ft.write_df(min_rotation_df, env.transit_time_folder.joinpath('minima'))

    # erv = HellGateSlackTimes(cy, route.waypoints)
    # ft.write_df(erv.hell_gate_slack, env.transit_time_folder.joinpath('hell_gate_slack'))
    #
    # print(f'\nCreating Battery tide arcs')
    # battery_wp = TideWP(args['filepath'].parent.joinpath('NOAA Tide Stations/8518750.gpx'))
    # battery_job = TideStationJob(cy, battery_wp, TIMESTEP)
    # battery_job.execute()
    # ft.write_df(battery_job.battery_lines, env.transit_time_folder.joinpath('battery_tide'))

    print(f'\nProcess Complete')

    job_manager.stop_queue()

    # del job_manager
