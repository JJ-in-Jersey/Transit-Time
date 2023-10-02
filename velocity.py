from time import perf_counter
import pandas as pd
import numpy as np
from scipy.interpolate import CubicSpline
from sympy import Point

from tt_chrome_driver import chrome_driver as cd
# noinspection PyPep8Naming
from tt_interpolation.velocity_interpolation import Interpolator as VI
from tt_file_tools import file_tools as ft
from tt_date_time_tools import date_time_tools as dtt

from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

from project_globals import WDW

#  VELOCITIES ARE DOWNLOADED, CALCULATED AND SAVE AS NAUTICAL MILES PER HOUR!


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


def download_event(wdw): wdw[0].until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()


def set_up_download(year, driver, wdw, waypoint):
    code_string = 'Annual?id=' + waypoint.code
    wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
    Select(driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
    Select(driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
    dropdown = Select(driver.find_element(By.ID, 'year'))
    options = [int(o.text) for o in dropdown.options]
    dropdown.select_by_index(options.index(year))


class VelocityDownloadedDataframe:

    def __init__(self, year, waypoint, headless=False):
        self.headless = headless
        self.dataframe = None

        if ft.csv_npy_file_exists(waypoint.downloaded_data_filepath):
            self.dataframe = ft.read_df(waypoint.downloaded_data_filepath)
        else:
            self.dataframe = pd.DataFrame()
            driver = cd.get_driver(waypoint.folder, headless)
            wdw = WebDriverWait(driver, WDW)
            driver.get(waypoint.noaa_url)

            for y in range(year - 1, year + 2):  # + 2 because of range behavior
                set_up_download(y, driver, wdw, waypoint)
                downloaded_file = ft.wait_for_new_file(waypoint.folder, download_event, wdw)
                file_df = pd.read_csv(downloaded_file, parse_dates=['Date_Time (LST/LDT)'])
                self.dataframe = pd.concat([self.dataframe, file_df])

            driver.quit()

            self.dataframe.rename(columns={' Event': 'Event', ' Speed (knots)': 'Speed (knots)', 'Date_Time (LST/LDT)': 'date_time'}, inplace=True)
            self.dataframe['Event'] = self.dataframe['Event'].apply(lambda s: s.strip())
            self.dataframe['date_index'] = self.dataframe['date_time'].apply(lambda x: dtt.int_timestamp(x))
            self.dataframe['velocity'] = self.dataframe['Speed (knots)'].apply(dash_to_zero)
            self.dataframe = self.dataframe[(waypoint.start_index <= self.dataframe['date_index']) & (self.dataframe['date_index'] <= waypoint.end_index)]
            ft.write_df(self.dataframe, waypoint.downloaded_data_filepath)


class VelocityInterpolatedArray:

    def __init__(self, waypoint, timestep, downloaded_dataframe):
        self.velocity_array = None

        if ft.csv_npy_file_exists(waypoint.final_data_filepath):
            self.velocity_array = ft.read_arr(waypoint.final_data_filepath)
        else:
            dataframe = downloaded_dataframe.dataframe
            cs = CubicSpline(dataframe['date_index'], dataframe['velocity'])
            df = pd.DataFrame()
            df['date_index'] = range(waypoint.start_index, waypoint.end_index, timestep)
            df['date_time'] = pd.to_datetime(df['date_index'], unit='s').round('min')
            df['velocity'] = df['date_index'].apply(cs)
            ft.write_df(df, waypoint.final_data_filepath)
            self.velocity_array = np.array(df['velocity'].to_list(), dtype=np.half)
            ft.write_arr(self.velocity_array, waypoint.final_data_filepath)


class CurrentStationJob:

    def execute(self):
        init_time = perf_counter()
        print(f'+     {self.waypoint.unique_name}', flush=True)
        downloaded_dataframe = VelocityDownloadedDataframe(self.year, self.waypoint, self.headless)
        interpolated_array = VelocityInterpolatedArray(self.waypoint, self.timestep, downloaded_dataframe)
        return tuple([self.result_key, interpolated_array.velocity_array, init_time])

    def execute_callback(self, result):
        print(f'-     {self.waypoint.unique_name} {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.waypoint.unique_name} process has raised an error: {result}', flush=True)

    def __init__(self, year, waypoint, timestep, headless=False):
        self.headless = headless
        self.year = year
        self.waypoint = waypoint
        self.timestep = timestep
        self.result_key = id(waypoint)


class InterpolationDataJob(CurrentStationJob):

    interpolation_timestep = 10800  # three hour timestep
    # interpolation_timestep = 600000  # three hour timestep

    def __init__(self, year, waypoint):
        super().__init__(year, waypoint, InterpolationDataJob.interpolation_timestep)


class InterpolationJob:

    @staticmethod
    def write_dataframe(wp, velocities):
        download_df = pd.DataFrame(data={'date_index': range(wp.start_index, wp.end_index, InterpolationDataJob.interpolation_timestep), 'velocity': velocities})
        download_df['date_time'] = pd.to_datetime(download_df['date_index']).round('min')
        ft.write_df(download_df, wp.final_data_filepath)

    def execute(self):
        init_time = perf_counter()
        print(f'+     {self.wp.unique_name} {self.index} of {self.size}', flush=True)
        interpolator = VI(self.surface_points)
        interpolator.set_interpolation_point(self.input_point)
        output = interpolator.get_interpolated_point()
        if self.display:
            interpolator.set_interpolation_point(self.input_point)
            interpolator.show_axes()
        return tuple([self.result_key, output, init_time])

    def execute_callback(self, result):
        print(f'-     {self.wp.unique_name} {self.index} {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.wp.unique_name} process has raised an error: {result}', flush=True)

    def __init__(self, waypoints, index: int, display=False):
        self.display = display
        interpolation_point = waypoints[0]
        self.size = len(waypoints[1].output_data)
        self.wp = interpolation_point
        self.index = index
        self.input_point = Point(interpolation_point.lat, interpolation_point.lon, 0)
        self.result_key = str(id(interpolation_point))+'_'+str(index)
        self.surface_points = [Point(wp.lat, wp.lon, wp.output_data[index]) for wp in waypoints[1:]]
