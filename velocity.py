import pandas as pd
from scipy.interpolate import CubicSpline
from sympy import Point
from pathlib import Path

# from tt_noaa_data.noaa_data import noaa_current_14_months
from tt_noaa_data.noaa_data import noaa_current_dataframe, noaa_slack_dataframe
from tt_interpolation.velocity_interpolation import Interpolator as VInt
from tt_file_tools import file_tools as ft
from tt_date_time_tools.date_time_tools import int_timestamp as date_time_index
from tt_job_manager.job_manager import Job
from tt_gpx.gpx import DownloadedDataWP, Waypoint
from tt_globals.globals import Globals


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


class DownloadedVelocityCSV:

    def __init__(self, start, end, folder: Path, code: str, wp_type: str):

        self.filepath = folder.joinpath(wp_type.lower() + '_velocity.csv')

        if not self.filepath.exists():

            frame = noaa_current_dataframe(start, end, folder, code)
            frame.rename(columns={'Time': 'date_time', ' Velocity_Major': 'velocity'}, inplace=True)
            frame['date_index'] = frame['date_time'].apply(date_time_index)

            ft.write_df(frame, self.filepath)


class DownloadVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, start, end, wp: DownloadedDataWP):
        result_key = id(wp)
        arguments = tuple([start, end, wp.folder, wp.code, wp.type])
        super().__init__(wp.unique_name, result_key, DownloadedVelocityCSV, arguments)


class SplineFitHarmonicVelocityCSV:

    # def __init__(self, velocity_file: Path, download_range: range):
    def __init__(self, index_range, velocity_file):
        self.filepath = velocity_file.parent.joinpath(velocity_file.stem + '_spline_fit.csv')
        velocity_frame = ft.read_df(velocity_file)

        if not self.filepath.exists():
            cs = CubicSpline(velocity_frame['date_index'], velocity_frame['velocity'])
            frame = pd.DataFrame()
            frame['date_index'] = index_range
            frame['date_time'] = pd.to_datetime(frame['date_index'], unit='s').round('min')
            frame['velocity'] = frame['date_index'].apply(cs)
            ft.write_df(frame, self.filepath)


class SplineFitHarmonicVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, index_range, waypoint: Waypoint):
        result_key = id(waypoint)
        filepath = waypoint.folder.joinpath('harmonic_velocity.csv')
        arguments = tuple([index_range, filepath])
        super().__init__(waypoint.unique_name, result_key, SplineFitHarmonicVelocityCSV, arguments)


class InterpolatedPoint:

    def __init__(self, interpolation_pt_data, surface_points, date_index):
        interpolator = VInt(surface_points)
        interpolator.set_interpolation_point(Point(interpolation_pt_data[1], interpolation_pt_data[2], 0))
        self.date_velocity = tuple([date_index, round(interpolator.get_interpolated_point().z.evalf(), 4)])


class InterpolatePointJob(Job):

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, interpolation_pt, velocity_data, index):

        date_indices = velocity_data[0]['date_index']

        result_key = str(id(interpolation_pt)) + '_' + str(index)
        interpolation_pt_data = tuple([interpolation_pt.unique_name, interpolation_pt.lat, interpolation_pt.lon])
        surface_points = tuple([Point(frame.at[index, 'lat'], frame.at[index, 'lon'], frame.at[index, 'velocity']) for frame in velocity_data])
        arguments = tuple([interpolation_pt_data, surface_points, date_indices[index]])
        super().__init__(str(index) + ' ' + interpolation_pt.unique_name, result_key, InterpolatedPoint, arguments)


def interpolate_group(waypoints, job_manager):

    interpolation_pt = waypoints[0]
    data_waypoints = waypoints[1:]
    output_filepath = interpolation_pt.folder.joinpath('harmonic_velocity.csv')

    if not output_filepath.exists():

        velocity_data = [ft.read_df(wp.folder.joinpath('harmonic_velocity.csv')) for wp in data_waypoints]
        for i, wp in enumerate(data_waypoints):
            velocity_data[i]['lat'] = wp.lat
            velocity_data[i]['lon'] = wp.lon
            
        keys = [job_manager.put(InterpolatePointJob(interpolation_pt, velocity_data, i)) for i in range(len(velocity_data[0]))]
        job_manager.wait()

        result_array = tuple([job_manager.get(key).date_velocity for key in keys])
        frame = pd.DataFrame(result_array, columns=['date_index', 'velocity'])
        frame.sort_values('date_index', inplace=True)
        frame['date_time'] = frame['date_index'].apply(date_time_index)
        frame.reset_index(drop=True, inplace=True)
        interpolation_pt.downloaded_data = frame
        ft.write_df(frame, output_filepath)


class SubordinateVelocityAdjustment:

    def __init__(self, index_range, velocity_file: Path):

        self.filepath = velocity_file.parent.joinpath('harmonic_velocity.csv')
        velocity_frame = ft.read_df(velocity_file)

        if not self.filepath.exists():
            cs = CubicSpline(velocity_frame['date_index'], velocity_frame['velocity'])
            frame = pd.DataFrame()
            frame['date_index'] = index_range
            frame['date_time'] = pd.to_datetime(frame['date_index'], unit='s').round('min')
            frame['velocity'] = frame['date_index'].apply(cs)
            ft.write_df(frame, self.filepath)


class SubordinateVelocityAdjustmentJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint: Waypoint):
        result_key = id(waypoint)
        filepath = waypoint.folder.joinpath('subordinate_velocity.csv')
        arguments = tuple([Globals.DOWNLOAD_INDEX_RANGE, filepath])
        super().__init__(waypoint.unique_name + ' ' + waypoint.type, result_key, SubordinateVelocityAdjustment, arguments)


class DownloadedSlackCSV:

    def __init__(self, start, end, folder: Path, station: str):

        self.filepath = folder.joinpath(station + '_slack.csv')

        if not self.filepath.exists():

            frame = noaa_slack_dataframe(start, end, folder, station)
            frame.rename(columns={'Time': 'date_time', ' Velocity_Major': 'velocity'}, inplace=True)
            frame['date_index'] = frame['date_time'].apply(date_time_index)

            ft.write_df(frame, self.filepath)

# from tt_chrome_driver import chrome_driver as cd
# from selenium.webdriver.support.ui import Select
# from selenium.webdriver.support import expected_conditions as ec
# from selenium.webdriver.common.by import By

# class DownloadedVelocityDataframe:
#
#     @staticmethod
#     def click_sequence(year, code):
#         code_string = 'Annual?id=' + code
#         cd.WDW.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
#         Select(cd.driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
#         Select(cd.driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
#         dropdown = Select(cd.driver.find_element(By.ID, 'year'))
#         options = [int(o.text) for o in dropdown.options]
#         dropdown.select_by_index(options.index(year))
#
#     @staticmethod
#     def download_event():
#         cd.WDW.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()
#
#     def __init__(self, year, downloaded_path, folder, url, code, start_index, end_index):
#         self.frame = None
#
#         if ft.csv_npy_file_exists(downloaded_path):
#             self.frame = ft.read_df(downloaded_path)
#         else:
#             self.frame = pd.DataFrame()
#             cd.set_driver(folder)
#
#             for y in range(year - 1, year + 2):  # + 2 because of range behavior
#                 cd.driver.get(url)
#                 self.click_sequence(y, code)
#                 downloaded_file = ft.wait_for_new_file(folder, self.download_event)
#                 file_df = pd.read_csv(downloaded_file, parse_dates=['Date_Time (LST/LDT)'])
#                 self.frame = pd.concat([self.frame, file_df])
#
#             cd.driver.quit()
#
#             self.frame.rename(columns={' Event': 'Event', ' Speed (knots)': 'Speed (knots)', 'Date_Time (LST/LDT)': 'date_time'}, inplace=True)
#             self.frame['Event'] = self.frame['Event'].apply(lambda s: s.strip())
#             self.frame['date_index'] = self.frame['date_time'].apply(lambda x: dtt.int_timestamp(x))
#             self.frame['velocity'] = self.frame['Speed (knots)'].apply(dash_to_zero)
#             self.frame = self.frame[(start_index <= self.frame['date_index']) & (self.frame['date_index'] <= end_index)]
#             self.frame.reset_index(drop=True, inplace=True)
#             ft.write_df(self.frame, downloaded_path)


# class DownloadVelocityJob(Job):  # super -> job name, result key, function/object, arguments
#
#     def execute(self): return super().execute()
#     def execute_callback(self, result): return super().execute_callback(result)
#     def error_callback(self, result): return super().error_callback(result)
#
#     def __init__(self, waypoint, year, start, end):
#         result_key = id(waypoint)
#         arguments = tuple([year, waypoint.downloaded_path, waypoint.folder, waypoint.noaa_url, waypoint.code, start, end])
#         super().__init__(waypoint.unique_name, result_key, DownloadedVelocityDataframe, arguments)
