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
from project_globals import read_df, write_df

#  VELOCITIES ARE DOWNLOADED, CALCULATED AND SAVE AS NAUTICAL MILES PER HOUR!

logging.getLogger('WDM').setLevel(logging.NOTSET)

df_type = 'pkl'

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
        if output_file_exists(self.velocity_table_path):
            print(f'+     {self.intro} {self.code} {self.name} reading data file', flush=True)
            return tuple([self.id, read_df(self.velocity_table_path)])
        else:
            print(f'+     {self.intro} {self.code} {self.name}', flush=True)
            year = self.date.year()
            download_df = pd.DataFrame()

            self.driver = get_chrome_driver(self.download_folder)
            for y in range(year - 1, year + 2):  # + 2 because of range behavior2
                self.driver.get(self.url)
                self.wdw = WebDriverWait(self.driver, 1000)
                self.velocity_page(y)
                file = self.velocity_download()
                file_dataframe = pd.read_csv(file, header='infer', converters={' Speed (knots)': dash_to_zero}, parse_dates=['Date_Time (LST/LDT)'])
                download_df = pd.concat([download_df, file_dataframe])
            self.driver.quit()

            download_df.rename(columns={'Date_Time (LST/LDT)': 'date', ' Event': 'event', ' Speed (knots)': 'velocity'}, inplace=True)
            download_df = download_df[(self.start <= download_df['date']) & (download_df['date'] <= self.end)]
            download_df['time_index'] = download_df['date'].apply(self.date.time_to_index)
            write_df(download_df, self.download_table_path, df_type)
            cs = CubicSpline(download_df['time_index'], download_df['velocity'])
            del download_df
            output_df = pd.DataFrame()
            output_df['time_index'] = range(self.date.time_to_index(self.start), self.date.time_to_index(self.end), TIMESTEP)
            output_df['date'] = pd.to_timedelta(output_df['time_index'], unit='seconds') + self.date.index_basis()
            output_df['velocity'] = output_df['time_index'].apply(cs)
            write_df(output_df, self.velocity_table_path, df_type)  # velocities are in knots
            return tuple([self.id, output_df])

    # noinspection PyUnusedLocal
    def execute_callback(self, result):
        print(f'-     {self.intro} {self.code} {round((perf_counter() - self.init_time)/60, 2)} minutes', flush=True)
    def error_callback(self, result):
        print(f'!     {self.intro} {self.code} process has raised an error: {result}', flush=True)

    def __init__(self, route_node, chart_yr, intro=''):
        self.init_time = perf_counter()
        self.wdw = self.driver = None
        self.date = chart_yr
        self.intro = intro
        self.code = route_node.code()
        self.name = route_node.name()
        self.url = route_node.url()
        self.id = id(route_node)
        self.download_table_path = route_node.download_table_path()
        self.velocity_table_path = route_node.velocity_table_path()
        self.download_folder = route_node.download_folder()
        self.start = self.date.first_day_minus_one()
        self.end = self.date.last_day_plus_three()
        umask(0)

    # def __del__(self):
    #     print(f'Deleting Velocity Job', flush=True)