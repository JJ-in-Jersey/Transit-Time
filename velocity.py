import logging
from os import environ, umask
from glob import glob
from os.path import join, getctime
from time import sleep, perf_counter
import pandas as pd
from scipy.interpolate import CubicSpline

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from project_globals import TIMESTEP, dash_to_zero, output_file_exists
from project_globals import read_df, write_df, min_sec, date_to_index

#  VELOCITIES ARE DOWNLOADED, CALCULATED AND SAVE AS NAUTICAL MILES PER HOUR!

logging.getLogger('WDM').setLevel(logging.NOTSET)

df_type = 'hdf'

def newest_file(folder):
    types = ['*.txt', '*.csv']
    files = []
    for t in types: files.extend(glob(join(folder, t)))
    return max(files, key=getctime) if len(files) else None

def get_chrome_driver(download_dir):
    my_options = Options()
    environ['WDM_LOG'] = "false"
    environ['WDM_LOG_LEVEL'] = '0'
    my_options.add_argument('disable-notifications')
    my_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    my_options.add_experimental_option("prefs", {'download.default_directory': str(download_dir)})
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=my_options)
    driver.minimize_window()
    return driver

class VelocityJob:

    def velocity_download(self):
        newest_before = newest_after = newest_file(self.download_folder)
        self.wdw.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()
        while newest_before == newest_after:
            sleep(0.1)
            newest_after = newest_file(self.download_folder)
        return newest_after
    def velocity_page(self, year):
        code_string = 'Annual?id=' + self.code
        self.wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
        Select(self.driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
        Select(self.driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
        dropdown = Select(self.driver.find_element(By.ID, 'year'))
        options = [int(o.text) for o in dropdown.options]
        dropdown.select_by_index(options.index(year))

    def execute(self):
        init_time = perf_counter()
        if output_file_exists(self.velocity_table_path):
            print(f'+     {self.intro} {self.code} {self.name} reading data file', flush=True)
            return tuple([self.id, read_df(self.velocity_table_path), init_time])
        else:
            self.init_time = perf_counter()
            print(f'+     {self.intro} {self.code} {self.name}', flush=True)
            download_df = pd.DataFrame()

            self.driver = get_chrome_driver(self.download_folder)
            for y in range(self.year - 1, self.year + 2):  # + 2 because of range behavior2
                self.driver.get(self.url)
                self.wdw = WebDriverWait(self.driver, 1000)
                self.velocity_page(y)
                file = self.velocity_download()
                file_dataframe = pd.read_csv(file, usecols=[' Speed (knots)','Date_Time (LST/LDT)'], converters={' Speed (knots)': dash_to_zero, 'Date_Time (LST/LDT)': date_to_index})
                file_dataframe.rename(columns={'Date_Time (LST/LDT)': 'date_index', ' Speed (knots)': 'velocity'}, inplace=True)
                download_df = pd.concat([download_df, file_dataframe])
            self.driver.quit()

            download_df = download_df[(self.start_index <= download_df['date_index']) & (download_df['date_index'] <= self.end_index)]
            write_df(download_df, self.download_table_path, df_type)
            cs = CubicSpline(download_df['date_index'], download_df['velocity'])
            del download_df
            output_df = pd.DataFrame()
            output_df['date_index'] = range(self.start_index, self.end_index, TIMESTEP)
            output_df['velocity'] = output_df['date_index'].apply(cs)
            write_df(output_df, self.velocity_table_path, df_type)  # velocities are in knots
            return tuple([self.id, output_df, init_time])

    # noinspection PyUnusedLocal
    def execute_callback(self, result):
        print(f'-     {self.intro} {self.code} {min_sec(perf_counter() - result[2])} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} {self.code} process has raised an error: {result}', flush=True)

    def __init__(self, route_node, chart_yr, intro=''):
        self.wdw = self.driver = None
        self.year = chart_yr.year()
        self.start_index = date_to_index(chart_yr.first_day_minus_one())
        self.end_index = date_to_index(chart_yr.last_day_plus_three())
        self.intro = intro
        self.code = route_node.code()
        self.name = route_node.name()
        self.url = route_node.url()
        self.id = id(route_node)
        self.download_table_path = route_node.download_table_path()
        self.velocity_table_path = route_node.velocity_table_path()
        self.download_folder = route_node.download_folder()

        umask(0)
