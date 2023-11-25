import pandas as pd
from scipy.interpolate import CubicSpline
from sympy import Point

from tt_chrome_driver import chrome_driver
# noinspection PyPep8Naming
from tt_interpolation.velocity_interpolation import Interpolator as VI
from tt_file_tools import file_tools as ft
from tt_date_time_tools import date_time_tools as dtt
from tt_job_manager.job_manager import Job

from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

from project_globals import WDW, TIMESTEP


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


def download_event(wdw): wdw[0].until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()


class VelocityDownloadedDataframe:

    @staticmethod
    def set_up_download(year, driver, wdw, code):
        code_string = 'Annual?id=' + code
        wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
        Select(driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
        Select(driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
        dropdown = Select(driver.find_element(By.ID, 'year'))
        options = [int(o.text) for o in dropdown.options]
        dropdown.select_by_index(options.index(year))

    def __init__(self, year, downloaded_path, folder, url, code, start, end):
        self.dataframe = None

        if ft.csv_npy_file_exists(downloaded_path):
            self.dataframe = ft.read_df(downloaded_path)
        else:
            self.dataframe = pd.DataFrame()
            driver = chrome_driver.get_driver(folder)
            wdw = WebDriverWait(driver, WDW)

            for y in range(year - 1, year + 2):  # + 2 because of range behavior
                driver.get(url)
                self.set_up_download(y, driver, wdw, code)
                downloaded_file = ft.wait_for_new_file(folder, download_event, wdw)
                file_df = pd.read_csv(downloaded_file, parse_dates=['Date_Time (LST/LDT)'])
                self.dataframe = pd.concat([self.dataframe, file_df])

            driver.quit()

            self.dataframe.rename(columns={' Event': 'Event', ' Speed (knots)': 'Speed (knots)', 'Date_Time (LST/LDT)': 'date_time'}, inplace=True)
            self.dataframe['Event'] = self.dataframe['Event'].apply(lambda s: s.strip())
            self.dataframe['date_index'] = self.dataframe['date_time'].apply(lambda x: dtt.int_timestamp(x))
            self.dataframe['velocity'] = self.dataframe['Speed (knots)'].apply(dash_to_zero)
            self.dataframe = self.dataframe[(start <= self.dataframe['date_index']) & (self.dataframe['date_index'] <= end)]
            self.dataframe.reset_index(drop=True, inplace=True)
            ft.write_df(self.dataframe, downloaded_path)


class VelocitySplineFitDataframe:

    def __init__(self, spline_fit_data_path, downloaded_dataframe, start, end):
        self.dataframe = None

        if ft.csv_npy_file_exists(spline_fit_data_path):
            self.dataframe = ft.read_df(spline_fit_data_path)
        else:
            cs = CubicSpline(downloaded_dataframe['date_index'], downloaded_dataframe['velocity'])
            self.dataframe = pd.DataFrame()
            self.dataframe['date_index'] = range(start, end, TIMESTEP)
            self.dataframe['date_time'] = pd.to_datetime(self.dataframe['date_index'], unit='s').round('min')
            self.dataframe['velocity'] = self.dataframe['date_index'].apply(cs)
            ft.write_df(self.dataframe, spline_fit_data_path)


class InterpolatedPoint:

    def __init__(self, interpolation_pt_data, surface_points, date_index):
        interpolator = VI(surface_points)
        interpolator.set_interpolation_point(Point(interpolation_pt_data[1], interpolation_pt_data[2], 0))
        self.date_velo = [date_index, round(interpolator.get_interpolated_point().z.evalf(), 4)]
        # interpolator.set_interpolation_point(input_point)
        # interpolator.show_axes


class NoaaCurrentDownloadJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint, year, start, end):
        result_key = id(waypoint)
        arguments = tuple([year, waypoint.downloaded_current_path, waypoint.folder, waypoint.noaa_url, waypoint.code, start, end])
        super().__init__(waypoint.unique_name, result_key, VelocityDownloadedDataframe, arguments)


class SplineFitCurrentDataJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint, start, end):
        result_key = id(waypoint)
        arguments = tuple([waypoint.spline_fit_current_path, waypoint.downloaded_current_data, start, end])
        super().__init__(waypoint.unique_name, result_key, VelocitySplineFitDataframe, arguments)


class InterpolatePointJob(Job):

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, interpolation_pt, data_waypoints, index):
        result_key = str(id(interpolation_pt)) + '_' + str(index)
        interpolation_pt_data = tuple([interpolation_pt.unique_name, interpolation_pt.lat, interpolation_pt.lon])
        surface_points = [Point(wp.lat, wp.lon, wp.spline_fit_current_data.at[index, 'velocity']) for wp in data_waypoints]
        date_index = data_waypoints[0].spline_fit_current_data.at[index, 'date_index']
        arguments = tuple([interpolation_pt_data, surface_points, date_index])
        super().__init__(str(index) + ' '+ interpolation_pt.unique_name, result_key, InterpolatedPoint, arguments)


def interpolate_group(group, job_manager):

    interpolation_pt = group[0]
    data_waypoints = group[1:]

    if ft.csv_npy_file_exists(interpolation_pt.spline_fit_current_path):
        interpolation_pt.spline_fit_current_data = ft.read_df(interpolation_pt.spline_fit_current_path)
    else:
        group_range = range(len(data_waypoints[0].downloaded_current_data))
        for i in group_range:
            job_manager.put(InterpolatePointJob(interpolation_pt, data_waypoints, i))
        job_manager.wait()

        result_array = [job_manager.get(str(id(interpolation_pt)) + '_' + str(i)).date_velo for i in group_range]
        frame = pd.DataFrame(result_array, columns=['date_index', 'velocity'])
        frame.sort_values('date_index', inplace=True)
        frame['date_time'] = pd.to_datetime(frame['date_index'], unit='s')
        frame.reset_index(drop=True, inplace=True)
        interpolation_pt.spline_fit_current_data = frame
        ft.write_df(frame, interpolation_pt.spline_fit_current_path)
