import pandas as pd
from scipy.interpolate import CubicSpline
from sympy import Point
from pathlib import Path

from tt_noaa_data.noaa_data import noaa_current_datafile
# from tt_chrome_driver import chrome_driver as cd
from tt_interpolation.velocity_interpolation import Interpolator as VInt
from tt_file_tools import file_tools as ft
from tt_date_time_tools import date_time_tools as dtt
from tt_job_manager.job_manager import Job
from tt_gpx.gpx import DownloadedDataWP

# from selenium.webdriver.support.ui import Select
# from selenium.webdriver.support import expected_conditions as ec
# from selenium.webdriver.common.by import By

from project_globals import TIMESTEP


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


class DownloadedVelocityCSV:

    def __init__(self, year: int, folder: Path, code: str):
        # noaa columns:   'Time', ' Depth', ' Velocity_Major', ' meanFloodDir', ' meanEbbDir', ' Bin'

        output_filepath = folder.joinpath('velocity.csv')

        if not ft.csv_npy_file_exists(output_filepath):
            frame = pd.DataFrame()
            station_bin = code.split('_')

            file = noaa_current_datafile(folder, year - 1, 12, station_bin[0], station_bin[1])
            frame = pd.concat([frame, pd.read_csv(file, header='infer')])

            for m in range(1, 13):
                file = noaa_current_datafile(folder, year, m, station_bin[0], station_bin[1])
                frame = pd.concat([frame, pd.read_csv(file, header='infer')])

            file = noaa_current_datafile(folder, year + 1, 1, station_bin[0], station_bin[1])
            frame = pd.concat([frame, pd.read_csv(file, header='infer')])
            frame.rename(columns={'Time': 'date_time', ' Velocity_Major': 'velocity'}, inplace=True)

            ft.write_df(frame, output_filepath)


class DownloadVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, wp: DownloadedDataWP, year: int):
        result_key = id(wp)
        arguments = tuple([year, wp.folder, wp.code])
        super().__init__(wp.unique_name, result_key, DownloadedVelocityCSV, arguments)


class SplineFitVelocityCSV:

    # def __init__(self, spline_fit_path, velocity_path: pd.DataFrame, timestep):
    def __init__(self, velocity_file: Path, timestep):

        output_filepath = velocity_file.parent.joinpath(velocity_file.stem + '_spline_fit.csv')

        velocity_frame = ft.read_df(velocity_file)
        velocity_frame['date_index'] = velocity_frame['Time'].apply(dtt.int_timestamp)

        if not ft.csv_npy_file_exists(output_filepath):
            cs = CubicSpline(velocity_frame['date_index'], velocity_frame[' Velocity_Major'])
            frame = pd.DataFrame()
            frame['date_index'] = range(velocity_frame['date_index'].iloc[0], velocity_frame['date_index'].iloc[-1], timestep)
            frame['date_time'] = pd.to_datetime(frame['date_index'], unit='s').round('min')
            frame['velocity'] = frame['date_index'].apply(cs)
            ft.write_df(frame, output_filepath)


class SplineFitVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint, timestep=TIMESTEP):
        result_key = id(waypoint)
        filepath = waypoint.folder.joinpath('velocity.csv')
        arguments = tuple([filepath, timestep])
        super().__init__(waypoint.unique_name, result_key, SplineFitVelocityCSV, arguments)


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

        date_indices = velocity_data[0]['date_time'].apply(dtt.int_timestamp)

        result_key = str(id(interpolation_pt)) + '_' + str(index)
        interpolation_pt_data = tuple([interpolation_pt.unique_name, interpolation_pt.lat, interpolation_pt.lon])
        surface_points = tuple([Point(frame.at[index, 'lat'], frame.at[index, 'lon'], frame.at[index, 'velocity']) for frame in velocity_data])
        arguments = tuple([interpolation_pt_data, surface_points, date_indices[index]])
        super().__init__(str(index) + ' ' + interpolation_pt.unique_name, result_key, InterpolatedPoint, arguments)


def interpolate_group(waypoints, job_manager):

    interpolation_pt = waypoints[0]
    data_waypoints = waypoints[1:]
    output_filepath = interpolation_pt.folder.joinpath('velocity.csv')

    if not ft.csv_npy_file_exists(output_filepath):

        velocity_data = [ft.read_df(wp.folder.joinpath('velocity.csv')) for wp in data_waypoints]
        for i, wp in enumerate(data_waypoints):
            velocity_data[i]['lat'] = wp.lat
            velocity_data[i]['lon'] = wp.lon

        for i in range(len(velocity_data[0])):
            job = InterpolatePointJob(interpolation_pt, velocity_data, i)
            job_manager.put(job)
            # result = job.execute()
        job_manager.wait()

        result_array = tuple([job_manager.get(str(id(interpolation_pt)) + '_' + str(i)).date_velocity for i in range(len(velocity_data[0]))])
        frame = pd.DataFrame(result_array, columns=['date_index', 'velocity'])
        frame.sort_values('date_index', inplace=True)
        frame['date_time'] = frame['date_index'].apply(dtt.datetime)
        frame.reset_index(drop=True, inplace=True)
        interpolation_pt.downloaded_data = frame
        ft.write_df(frame, output_filepath)

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
