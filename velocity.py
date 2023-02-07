import logging
from os import umask
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
        if output_file_exists(self.__velocity_array_file):
            return tuple([self.__id, None, init_time])
        else:
            print(f'+     {self.__intro} {self.__code} {self.__name}', flush=True)
            download_df = pd.DataFrame()

            # download all the noaa files, aggregate them and write the out
            driver = cd.get_driver(self.__download_folder)
            for y in range(self.__year - 1, self.__year + 2):  # + 2 because of range behavior
                driver.get(self.__url)
                wdw = WebDriverWait(driver, WDW)
                VelocityJob.velocity_page(y, self.__code, driver, wdw)
                downloaded_file = VelocityJob.velocity_download(self.__download_folder, wdw)
                file_df = pd.read_csv(downloaded_file, usecols=[' Speed (knots)', 'Date_Time (LST/LDT)'], converters={' Speed (knots)': dash_to_zero, 'Date_Time (LST/LDT)': date_to_index})
                file_df.rename(columns={'Date_Time (LST/LDT)': 'date_index', ' Speed (knots)': 'velocity'}, inplace=True)
                download_df = pd.concat([download_df, file_df])
            driver.quit()
            download_df = download_df[(self.__start_index <= download_df['date_index']) & (download_df['date_index'] <= self.__end_index)]
            rw.write_df(download_df, self.__download_folder.joinpath(self.__code + '_table'), DF_FILE_TYPE)

            # create cubic spline
            cs = CubicSpline(download_df['date_index'], download_df['velocity'])

            output_df = pd.DataFrame()
            output_df['date_index'] = self.__velo_range
            output_df['velocity'] = output_df['date_index'].apply(cs)
            velo_array = np.array(output_df['velocity'].to_list(), dtype=np.half)
            rw.write_arr(velo_array, self.__velocity_array_file)
            return tuple([self.__id, velo_array, init_time])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__code} {mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__code} process has raised an error: {result}', flush=True)

    def __init__(self, waypoint, mpm, intro=''):
        self.__year = mpm.chart_yr.year()
        self.__start_index = mpm.chart_yr.waypoint_start_index()
        self.__end_index = mpm.chart_yr.waypoint_end_index()
        self.__velo_range = mpm.chart_yr.waypoint_range()
        self.__intro = intro
        self.__code = waypoint.code()
        self.__name = waypoint.name()
        self.__url = waypoint.url()
        self.__id = id(waypoint)
        self.__velocity_array_file = mpm.env.velocity_folder().joinpath(self.__code + '_array')
        self.__download_folder = mpm.env.create_waypoint_folder(waypoint.code)
        umask(0)
