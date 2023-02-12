import logging
from time import sleep, perf_counter
import pandas as pd
import numpy as np
from scipy.interpolate import CubicSpline

from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

from project_globals import WDW, DF_FILE_TYPE, output_file_exists
from project_globals import mins_secs, date_to_index
from ChromeDriver import ChromeDriver as cd
from ReadWrite import ReadWrite as rw
from FileUtilities import FileUtilities as fu

#  VELOCITIES ARE DOWNLOADED, CALCULATED AND SAVE AS NAUTICAL MILES PER HOUR!

logging.getLogger('WDM').setLevel(logging.NOTSET)

def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value

# noinspection PyProtectedMember
class VelocityJob:

    @classmethod
    def velocity_download(cls, folder, wdw):
        newest_before = newest_after = fu.newest_file(folder)
        wdw.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()
        while newest_before == newest_after:
            sleep(0.1)
            newest_after = fu.newest_file(folder)
        return newest_after

    @classmethod
    def velocity_page(cls, year, code, driver, wdw):
        code_string = 'Annual?id=' + code
        wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
        Select(driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
        Select(driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
        dropdown = Select(driver.find_element(By.ID, 'year'))
        options = [int(o.text) for o in dropdown.options]
        dropdown.select_by_index(options.index(year))

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self._velo_array_pathfile):
            print(f'+     {self._code} {self._name}', flush=True)
            velo_array = rw.read_arr(self._velo_array_pathfile)
            return tuple([self._result_key, velo_array, init_time])
        else:
            print(f'+     {self._code} {self._name}', flush=True)
            download_df = pd.DataFrame()

            # download all the noaa files, aggregate them and write the out
            driver = cd.get_driver(self._download_folder)
            for y in range(self._year - 1, self._year + 2):  # + 2 because of range behavior
                driver.get(self._url)
                wdw = WebDriverWait(driver, WDW)
                VelocityJob.velocity_page(y, self._code, driver, wdw)
                downloaded_file = VelocityJob.velocity_download(self._download_folder, wdw)
                file_df = pd.read_csv(downloaded_file, usecols=[' Speed (knots)', 'Date_Time (LST/LDT)'], converters={' Speed (knots)': dash_to_zero, 'Date_Time (LST/LDT)': date_to_index})
                file_df.rename(columns={'Date_Time (LST/LDT)': 'date_index', ' Speed (knots)': 'velocity'}, inplace=True)
                download_df = pd.concat([download_df, file_df])
            driver.quit()
            download_df = download_df[(self._start_index <= download_df['date_index']) & (download_df['date_index'] <= self._end_index)]
            rw.write_df(download_df, self._download_folder.joinpath(self._code + '_table'), DF_FILE_TYPE)

            # create cubic spline
            cs = CubicSpline(download_df['date_index'], download_df['velocity'])

            output_df = pd.DataFrame()
            output_df['date_index'] = self._velo_range
            output_df['velocity'] = output_df['date_index'].apply(cs)
            velo_array = np.array(output_df['velocity'].to_list(), dtype=np.half)
            rw.write_arr(velo_array, self._velo_array_pathfile)
            return tuple([self._result_key, velo_array, init_time])

    def execute_callback(self, result):
        print(f'-     {self._code} {self._name} {mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self._code} {self._name} process has raised an error: {result}', flush=True)

    def __init__(self, mpm, waypoint):
        self._year = mpm.cy.year()
        self._start_index = mpm.cy.waypoint_start_index()
        self._end_index = mpm.cy.waypoint_end_index()
        self._velo_range = mpm.cy.waypoint_range()
        self._code = waypoint._noaa_code
        self._name = waypoint._name
        self._url = waypoint._noaa_url
        self._result_key = id(waypoint)
        self._download_folder = mpm.env.waypoint_folder(waypoint._name)
        self._velo_array_pathfile = mpm.env.velocity_folder().joinpath(waypoint._name + '_array')
