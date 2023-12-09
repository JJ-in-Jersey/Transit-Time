import pandas as pd
from scipy.interpolate import CubicSpline
from sympy import Point

from tt_chrome_driver import chrome_driver as cd
from tt_interpolation.velocity_interpolation import Interpolator as VInt
from tt_file_tools import file_tools as ft
from tt_date_time_tools import date_time_tools as dtt
from tt_job_manager.job_manager import Job

from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

from project_globals import TIMESTEP


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


class DownloadedVelocityDataframe:

    @staticmethod
    def click_sequence(year, code):
        code_string = 'Annual?id=' + code
        cd.WDW.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
        Select(cd.driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
        Select(cd.driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
        dropdown = Select(cd.driver.find_element(By.ID, 'year'))
        options = [int(o.text) for o in dropdown.options]
        dropdown.select_by_index(options.index(year))

    @staticmethod
    def download_event():
        cd.WDW.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()

    def __init__(self, year, downloaded_path, folder, url, code, start_index, end_index):
        self.dataframe = None

        if ft.csv_npy_file_exists(downloaded_path):
            self.dataframe = ft.read_df(downloaded_path)
        else:
            self.dataframe = pd.DataFrame()
            cd.set_driver(folder)

            for y in range(year - 1, year + 2):  # + 2 because of range behavior
                cd.driver.get(url)
                self.click_sequence(y, code)
                downloaded_file = ft.wait_for_new_file(folder, self.download_event)
                file_df = pd.read_csv(downloaded_file, parse_dates=['Date_Time (LST/LDT)'])
                self.dataframe = pd.concat([self.dataframe, file_df])

            cd.driver.quit()

            self.dataframe.rename(columns={' Event': 'Event', ' Speed (knots)': 'Speed (knots)', 'Date_Time (LST/LDT)': 'date_time'}, inplace=True)
            self.dataframe['Event'] = self.dataframe['Event'].apply(lambda s: s.strip())
            self.dataframe['date_index'] = self.dataframe['date_time'].apply(lambda x: dtt.int_timestamp(x))
            self.dataframe['velocity'] = self.dataframe['Speed (knots)'].apply(dash_to_zero)
            self.dataframe = self.dataframe[(start_index <= self.dataframe['date_index']) & (self.dataframe['date_index'] <= end_index)]
            self.dataframe.reset_index(drop=True, inplace=True)
            ft.write_df(self.dataframe, downloaded_path)


class SplineFitVelocityDataframe:

    def __init__(self, spline_fit_path, downloaded_dataframe, start_index, end_index, timestep):
        self.dataframe = None

        if ft.csv_npy_file_exists(spline_fit_path):
            self.dataframe = ft.read_df(spline_fit_path)
        else:
            cs = CubicSpline(downloaded_dataframe['date_index'], downloaded_dataframe['velocity'])
            self.dataframe = pd.DataFrame()
            self.dataframe['date_index'] = range(start_index, end_index, timestep)
            self.dataframe['date_time'] = pd.to_datetime(self.dataframe['date_index'], unit='s').round('min')
            self.dataframe['velocity'] = self.dataframe['date_index'].apply(cs)
            ft.write_df(self.dataframe, spline_fit_path)


class InterpolatedPoint:

    def __init__(self, interpolation_pt_data, surface_points, date_index):
        interpolator = VInt(surface_points)
        interpolator.set_interpolation_point(Point(interpolation_pt_data[1], interpolation_pt_data[2], 0))
        self.date_velo = [date_index, round(interpolator.get_interpolated_point().z.evalf(), 4)]
        # interpolator.set_interpolation_point(input_point)
        # interpolator.show_axes


class DownloadVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint, year, start, end):
        result_key = id(waypoint)
        arguments = tuple([year, waypoint.downloaded_path, waypoint.folder, waypoint.noaa_url, waypoint.code, start, end])
        super().__init__(waypoint.unique_name, result_key, DownloadedVelocityDataframe, arguments)


class SplineFitVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint, start, end, timestep=TIMESTEP):
        result_key = id(waypoint)
        arguments = tuple([waypoint.spline_fit_path, waypoint.downloaded_data, start, end, timestep])
        super().__init__(waypoint.unique_name, result_key, SplineFitVelocityDataframe, arguments)


class InterpolatePointJob(Job):

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, interpolation_pt, data_waypoints, index):
        result_key = str(id(interpolation_pt)) + '_' + str(index)
        interpolation_pt_data = tuple([interpolation_pt.unique_name, interpolation_pt.lat, interpolation_pt.lon])
        surface_points = [Point(wp.lat, wp.lon, wp.spline_fit_data.at[index, 'velocity']) for wp in data_waypoints]
        date_index = data_waypoints[0].spline_fit_data.at[index, 'date_index']
        arguments = tuple([interpolation_pt_data, surface_points, date_index])
        super().__init__(str(index) + ' ' + interpolation_pt.unique_name, result_key, InterpolatedPoint, arguments)


def interpolate_group(group, job_manager):

    interpolation_pt = group[0]
    data_waypoints = group[1:]

    if ft.csv_npy_file_exists(interpolation_pt.downloaded_path):
        interpolation_pt.downloaded_data = ft.read_df(interpolation_pt.downloaded_path)
    else:
        group_range = range(len(data_waypoints[0].spline_fit_data))
        for i in group_range:
            job_manager.put(InterpolatePointJob(interpolation_pt, data_waypoints, i))
        job_manager.wait()

        result_array = [job_manager.get(str(id(interpolation_pt)) + '_' + str(i)).date_velo for i in group_range]
        frame = pd.DataFrame(result_array, columns=['date_index', 'velocity'])
        frame.sort_values('date_index', inplace=True)
        frame['date_time'] = pd.to_datetime(frame['date_index'], unit='s')
        frame.reset_index(drop=True, inplace=True)
        interpolation_pt.downloaded_data = frame
        ft.write_df(frame, interpolation_pt.downloaded_path)
