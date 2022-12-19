import logging
from os import environ, umask
from glob import glob
from os.path import join, exists, getctime
from pathlib import Path
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

from project_globals import TIMESTEP, seconds, dash_to_zero

logging.getLogger('WDM').setLevel(logging.NOTSET)

def newest_file(folder):
    types = ['*.txt', '*.csv']
    files = []
    for t in types: files.extend(glob(join(folder, t)))
    return max(files, key=getctime) if len(files) else None

def get_chrome_driver(user_profile, download_dir):
    my_options = Options()
    environ['WDM_LOG'] = "false"
    environ['WDM_LOG_LEVEL'] = '0'
    my_options.add_argument('disable-notifications')
    my_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    my_options.add_experimental_option("prefs", {'download.default_directory': str(download_dir)})
    if 'bronfelj' in user_profile:
        work_path = Path(user_profile + '/.wdm/drivers/chromedriver/win32/107.0.5304.62/chromedriver.exe')
        driver = webdriver.Chrome(executable_path=work_path, options=my_options)
    else:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=my_options)
    driver.minimize_window()
    return driver

class VelocityJob:

    def __velocity_download(self):
        newest_before = newest_after = newest_file(self.__n_dir)
        self.__wdw.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()
        while newest_before == newest_after:
            sleep(0.1)
            newest_after = newest_file(self.__n_dir)
        return newest_after
    def __velocity_page(self, year):
        code_string = 'Annual?id=' + self.__code  # select annual predictions
        self.__wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
        Select(self.__driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
        Select(self.__driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
        dropdown = Select(self.__driver.find_element(By.ID, 'year'))  # set year
        options = [int(o.text) for o in dropdown.options]
        dropdown.select_by_index(options.index(year))

    def execute(self):
        if exists(self.__output_table_name):
            print(f'+     {self.__intro} {self.__code} {self.__name} reading data file', flush=True)
            # noinspection PyTypeChecker
            return tuple([self.__id, pd.read_csv(self.__output_table_name, header='infer')])
        else:
            print(f'+     {self.__intro} {self.__code} {self.__name} velocity (1st day - 1, last day + 3)', flush=True)
            year = self.__chart_yr.year()
            download_df = pd.DataFrame()

            self.__driver = get_chrome_driver(self.__user_profile, self.__n_dir)
            for y in range(year - 1, year + 2):  # + 2 because of range behavior2
                self.__driver.get(self.__url)
                self.__wdw = WebDriverWait(self.__driver, 1000)
                self.__velocity_page(y)
                file = self.__velocity_download()
                file_dataframe = pd.read_csv(file, header='infer', converters={' Speed (knots)': dash_to_zero}, parse_dates=['Date_Time (LST/LDT)'])
                download_df = pd.concat([download_df, file_dataframe])
            self.__driver.quit()

            download_df.rename(columns={'Date_Time (LST/LDT)': 'date', ' Event': 'event', ' Speed (knots)': 'velocity'}, inplace=True)
            download_df = download_df[(self.__start <= download_df['date']) & (download_df['date'] <= self.__end)]
            download_df['time_index'] = download_df['date'].apply(self.__chart_yr.time_to_index)
            download_df.to_csv(self.__download_table_name, index=False)
            cs = CubicSpline(download_df['time_index'], download_df['velocity'])
            del download_df
            output_df = pd.DataFrame()
            output_df['time_index'] = range(self.__chart_yr.time_to_index(self.__start), self.__chart_yr.time_to_index(self.__end), TIMESTEP)
            output_df['date'] = pd.to_timedelta(output_df['time_index'], unit='seconds') + self.__index_basis
            output_df['velocity'] = output_df['time_index'].apply(cs)
            output_df.to_csv(self.__output_table_name, index=False)
            return tuple([self.__id, output_df])

    # noinspection PyUnusedLocal
    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__code} {round((perf_counter() - self.__init_time), 2)} seconds', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__code} process has raised an error: {result}', flush=True)

    def __init__(self, route_node, chart_yr, env, intro=''):
        self.__init_time = perf_counter()
        self.__download_time = self.__spline_time = 0
        self.__wdw = self.__driver = None
        self.__chart_yr = chart_yr
        self.__intro = intro
        self.__name = route_node.name()
        self.__code = route_node.code()
        self.__url = route_node.url()
        self.__id = id(route_node)
        self.__n_dir = env.node_folder(route_node.code())
        self.__v_dir = env.velocity_folder()
        self.__user_profile = env.user_profile()
        self.__download_table_name = Path(str(self.__n_dir) + '/' + self.__code + '_download_table.csv')
        self.__output_table_name = Path(str(self.__v_dir) + '/' + self.__code + '_output_table.csv')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_three()
        self.__index_basis = chart_yr.index_basis()
        self.__expected_length = int(seconds(self.__start, self.__end) / TIMESTEP)
        umask(0)
