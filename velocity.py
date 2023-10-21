from time import perf_counter
import pandas as pd
from scipy.interpolate import CubicSpline
from sympy import Point

from tt_chrome_driver import chrome_driver
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


def set_up_download(year, driver, wdw, code):
    code_string = 'Annual?id=' + code
    wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
    Select(driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
    Select(driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
    dropdown = Select(driver.find_element(By.ID, 'year'))
    options = [int(o.text) for o in dropdown.options]
    dropdown.select_by_index(options.index(year))


class VelocityDownloadedDataframe:

    def __init__(self, year, lookup, headless=False):
        self.headless = headless
        self.dataframe = None

        if ft.csv_npy_file_exists(lookup['downloaded_filepath']):
            self.dataframe = ft.read_df(lookup['downloaded_filepath'])
        else:
            self.dataframe = pd.DataFrame()
            driver = chrome_driver.get_driver(lookup['folder'], headless)
            wdw = WebDriverWait(driver, WDW)

            for y in range(year - 1, year + 2):  # + 2 because of range behavior
                driver.get(lookup['url'])
                set_up_download(y, driver, wdw, lookup['code'])
                downloaded_file = ft.wait_for_new_file(lookup['folder'], download_event, wdw)
                file_df = pd.read_csv(downloaded_file, parse_dates=['Date_Time (LST/LDT)'])
                self.dataframe = pd.concat([self.dataframe, file_df])

            driver.quit()

            self.dataframe.rename(columns={' Event': 'Event', ' Speed (knots)': 'Speed (knots)', 'Date_Time (LST/LDT)': 'date_time'}, inplace=True)
            self.dataframe['Event'] = self.dataframe['Event'].apply(lambda s: s.strip())
            self.dataframe['date_index'] = self.dataframe['date_time'].apply(lambda x: dtt.int_timestamp(x))
            self.dataframe['velocity'] = self.dataframe['Speed (knots)'].apply(dash_to_zero)
            self.dataframe = self.dataframe[(lookup['start'] <= self.dataframe['date_index']) & (self.dataframe['date_index'] <= lookup['end'])]
            ft.write_df(self.dataframe, lookup['downloaded_filepath'])


class VelocitySplineFitDataframe:

    def __init__(self, lookup, timestep, downloaded_dataframe):
        self.dataframe = None

        if ft.csv_npy_file_exists(lookup['final_filepath']):
            self.dataframe = ft.read_df(lookup['final_filepath'])
        else:
            cs = CubicSpline(downloaded_dataframe['date_index'], downloaded_dataframe['velocity'])
            self.dataframe = pd.DataFrame()
            self.dataframe['date_index'] = range(lookup['start'], lookup['end'], timestep)
            self.dataframe['date_time'] = pd.to_datetime(self.dataframe['date_index'], unit='s').round('min')
            self.dataframe['velocity'] = self.dataframe['date_index'].apply(cs)
            ft.write_df(self.dataframe, lookup['final_filepath'])


# download data for a current station and curve fit the results
class CurrentStationJob:

    def execute(self):
        init_time = perf_counter()
        print(f'+     {self.lookup['name']}', flush=True)
        downloaded_dataframe = VelocityDownloadedDataframe(self.year, self.lookup, self.headless)
        spline_fit_dataframe = VelocitySplineFitDataframe(self.lookup, self.timestep, downloaded_dataframe.dataframe)
        return tuple([self.result_key, spline_fit_dataframe.dataframe, init_time])

    def execute_callback(self, result):
        print(f'-     {self.lookup['name']} {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.lookup['name']} process has raised an error: {result}', flush=True)

    def __init__(self, year, waypoint, timestep, headless=False):
        self.headless = headless
        self.year = year
        self.timestep = timestep
        self.result_key = id(waypoint)
        self.lookup = {'url': waypoint.noaa_url, 'code': waypoint.code, 'name': waypoint.unique_name,
                       'downloaded_filepath': waypoint.downloaded_data_filepath, 'final_filepath': waypoint.final_data_filepath,
                       'start': waypoint.start_index, 'end': waypoint.end_index, 'folder': waypoint.folder}


# For one of the waypoints used for interpolation download and curve fit the results with a different timestep
class InterpolationStationJob(CurrentStationJob):
    # downloads data from NOAA for a waypoint used for the interpolated datapoint
    # uses large timestep for spline to create exact same times across waypoints
    # downloaded_data.csv is the original data from NOAA
    # final_data.csv is 3 hour timestep, spline interpolation

    interpolation_timestep = 10800  # three hour timestep (3 hours * 60 mins * 60 seconds = 10800)

    def __init__(self, year, waypoint):
        super().__init__(year, waypoint, InterpolationStationJob.interpolation_timestep, headless=False)


class InterpolatePointJob:
    # For each data point in interpolation data waypoint (3 hr timestep, same time point across all data waypoints)
    #      calculate the interpolated value of velocity at the interpolation waypoint
    # Write data from above as downloaded_data.csv. CurrentDataJob will read it and process the spline.

    @staticmethod
    def write_dataframe(wp, velocities):
        download_df = pd.DataFrame(data={'date_index': range(wp.start_index, wp.end_index, InterpolationStationJob.interpolation_timestep), 'velocity': velocities})
        download_df['date_time'] = pd.to_datetime(download_df['date_index']).round('min')
        ft.write_df(download_df, wp.final_data_filepath)

    def execute(self):
        init_time = perf_counter()
        print(f'+     {self.interpolation_pt_data[0]} {self.index} of {self.size}', flush=True)
        interpolator = VI(self.surface_points)
        interpolator.set_interpolation_point(Point(self.interpolation_pt_data[1], self.interpolation_pt_data[2], 0))
        output = [self.date_index, interpolator.get_interpolated_point().z.evalf()]
        # if self.display:
        #     # interpolator.set_interpolation_point(input_point)
        #     interpolator.show_axes()
        return tuple([self.result_key, output, init_time])

    def execute_callback(self, result):
        print(f'-     {self.interpolation_pt_data[0]} {self.index} {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.interpolation_pt_data[0]} process has raised an error: {result}', flush=True)

    def __init__(self, interpolation_pt, data_waypoints, data_size, index, display=False):
        self.interpolation_pt_data = tuple([interpolation_pt.unique_name, interpolation_pt.lat, interpolation_pt.lon])
        self.surface_points = [Point(wp.lat, wp.lon, wp.current_data.at[index, 'velocity']) for wp in data_waypoints]
        self.size = data_size
        self.result_key = str(id(interpolation_pt))+'_'+str(index)
        self.index = index
        self.date_index = data_waypoints[0].current_data.loc[index, 'date_index']
        self.display = display


class InterpolationPointJob:
    #  Finish process the interpolation point by spline fitting the data

    def execute(self):
        init_time = perf_counter()
        print(f'+     {self.lookup['name']}', flush=True)
        if ft.csv_npy_file_exists(self.lookup['final_filepath']):
            return tuple([self.result_key, ft.read_df(self.lookup['final_filepath']), init_time])
        else:
            interpolated_dataframe = VelocitySplineFitDataframe(self.lookup, self.timestep, ft.read_df(self.lookup['downloaded_filepath']))
            ft.write_df(interpolated_dataframe.dataframe, self.lookup['final_filepath'])
        return tuple([self.result_key, interpolated_dataframe.dataframe, init_time])

    def execute_callback(self, result):
        print(f'-     {self.lookup['name']} {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.lookup['name']} process has raised an error: {result}', flush=True)

    def __init__(self, year, waypoint, timestep):
        self.year = year
        self.timestep = timestep
        self.result_key = id(waypoint)
        self.lookup = {'final_filepath': waypoint.final_data_filepath, 'name': waypoint.unique_name,
                       'start': waypoint.start_index, 'end': waypoint.end_index,
                       'downloaded_filepath': waypoint.downloaded_data_filepath,
                       'final_filepath': waypoint.final_data_filepath}
