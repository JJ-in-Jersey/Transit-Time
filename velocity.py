import logging
from os import umask
from glob import glob
from os.path import join, getctime
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

#  VELOCITIES ARE DOWNLOADED, CALCULATED AND SAVE AS NAUTICAL MILES PER HOUR!

logging.getLogger('WDM').setLevel(logging.NOTSET)

def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value

class VelocityJob:

    @staticmethod
    def newest_file(folder):
        types = ['*.txt', '*.csv']
        files = []
        for t in types: files.extend(glob(join(folder, t)))
        return max(files, key=getctime) if len(files) else None

    def velocity_download(self, folder, wdw):
        newest_before = newest_after = self.newest_file(folder)
        wdw.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()
        while newest_before == newest_after:
            sleep(0.1)
            newest_after = self.newest_file(folder)
        return newest_after

    @staticmethod
    def velocity_page(year, code, driver, wdw):
        code_string = 'Annual?id=' + code
        wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
        Select(driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
        Select(driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
        dropdown = Select(driver.find_element(By.ID, 'year'))
        options = [int(o.text) for o in dropdown.options]
        dropdown.select_by_index(options.index(year))

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self.velo_path):
            return tuple([self.id, None, init_time])
        else:
            print(f'+     {self.intro} {self.code} {self.name}', flush=True)
            download_df = pd.DataFrame()

            self.driver = cd.get_driver(self.download_folder)
            for y in range(self.year - 1, self.year + 2):  # + 2 because of range behavior
                self.driver.get(self.url)
                self.wdw = WebDriverWait(self.driver, WDW)
                self.velocity_page(y, self.code, self.driver, self.wdw)
                file = self.velocity_download(self.download_folder, self.wdw)
                file_dataframe = pd.read_csv(file, usecols=[' Speed (knots)', 'Date_Time (LST/LDT)'],
                                             converters={' Speed (knots)': dash_to_zero,
                                                         'Date_Time (LST/LDT)': date_to_index})
                file_dataframe.rename(columns={'Date_Time (LST/LDT)': 'date_index', ' Speed (knots)': 'velocity'},
                                      inplace=True)
                download_df = pd.concat([download_df, file_dataframe])
            self.driver.quit()

            download_df = download_df[
                (self.start_index <= download_df['date_index']) & (download_df['date_index'] <= self.end_index)]
            rw.write_df(download_df, self.download_table_path, DF_FILE_TYPE)
            cs = CubicSpline(download_df['date_index'], download_df['velocity'])
            del download_df

            output_df = pd.DataFrame()
            output_df['date_index'] = self.__velo_range
            output_df['velocity'] = output_df['date_index'].apply(cs)
            velo_array = np.array(output_df['velocity'].to_list(), dtype=np.half)
            del output_df
            rw.write_arr(velo_array, self.velo_path)  # velocities are in knots
            return tuple([self.id, velo_array, init_time])

    def execute_callback(self, result):
        print(f'-     {self.intro} {self.code} {mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.intro} {self.code} process has raised an error: {result}', flush=True)

    def __init__(self, route_node, chart_yr, intro=''):
        self.wdw = self.driver = None
        self.year = chart_yr.year()
        self.start_index = chart_yr.waypoint_start_index()
        self.end_index = chart_yr.waypoint_end_index()
        self.__velo_range = chart_yr.waypoint_range()
        self.intro = intro
        self.code = route_node.code()
        self.name = route_node.name()
        self.url = route_node.url()
        self.id = id(route_node)
        self.download_table_path = route_node.download_table_path()
        self.velo_path = route_node.velocity_path()
        self.download_folder = route_node.download_folder()
        umask(0)

        if output_file_exists(self.velo_path):
            print(f':     {self.code} {self.name} reading data file', flush=True)
            route_node.velocities(rw.read_arr(self.velo_path))
