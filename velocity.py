import logging
from os import environ
from glob import glob
from os.path import join, exists, getctime
from pathlib import Path
from time import sleep
import pandas as pd
from scipy.interpolate import CubicSpline
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from project_globals import seconds, dash_to_zero, time_to_index, timestep

logging.getLogger('WDM').setLevel(logging.NOTSET)

def newest_file(folder):
    types = ['*.txt', '*.csv']
    files = []
    for t in types: files.extend(glob(join(folder, t)))
    return max(files, key=getctime) if len(files) else None

def get_chrome_driver(user_profile, download_dir):
    my_options = Options()
    environ['WDM_LOG'] = "false"
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
        if exists(self.__output_file):
            print(f'+     {self.__intro} {self.__code} {self.__name} reading data file', flush=True)
            # noinspection PyTypeChecker
            return tuple([self.__id, np.load(self.__output_file)])
        else:
            print(f'+     {self.__intro} {self.__code} {self.__name} velocity calculation starting', flush=True)
            year = self.__chart_yr.year()
            noaa_dataframe = pd.DataFrame()

            self.__driver = get_chrome_driver(self.__user_profile, self.__n_dir)
            for y in range(year - 1, year + 2):  # + 2 because of range behavior2
                self.__driver.get(self.__url)
                self.__wdw = WebDriverWait(self.__driver, 1000)
                self.__velocity_page(y)
                file = self.__velocity_download()
                file_dataframe = pd.read_csv(file, header='infer', converters={' Speed (knots)': dash_to_zero}, parse_dates=['Date_Time (LST/LDT)'])
                noaa_dataframe = pd.concat([noaa_dataframe, file_dataframe])
            self.__driver.quit()

            noaa_dataframe.rename(columns={'Date_Time (LST/LDT)': 'time', ' Event': 'event', ' Speed (knots)': 'velocity'}, inplace=True)
            noaa_dataframe = noaa_dataframe[(self.__start <= noaa_dataframe['time']) & (noaa_dataframe['time'] <= self.__end)]
            noaa_dataframe = noaa_dataframe.reset_index(drop=True)
            noaa_dataframe['seconds'] = noaa_dataframe['time'].apply(lambda time: time_to_index(self.__start, time)).to_numpy()  # time_to_index returns seconds from start
            noaa_dataframe.to_csv(Path(str(self.__v_dir)+'/'+self.__code+'_dataframe.csv'), index=False)
            cs = CubicSpline(noaa_dataframe['seconds'].to_numpy(), noaa_dataframe['velocity'].to_numpy())  # (time, velocity) v = cs(t)
            #  number of rows in v_range is the number of timesteps from start to end
            v_range = range(0, self.__seconds, timestep)
            result = np.fromiter([cs(t) for t in v_range], dtype=np.half)  # array of velocities at each timestep
            # noinspection PyTypeChecker
            np.save(self.__output_file, result)
            return tuple([self.__id, result])

    def execute_callback(self, result):
        print(f'-     {self.__intro} {self.__code} {"SUCCESSFUL" if isinstance(result[1], np.ndarray) and len(result[1]) == self.__expected_length else "FAILED"} {len(result[1])}', flush=True)
    def error_callback(self, result):
        print(f'!     {self.__intro} {self.__code} process has raised an error: {result}', flush=True)

    def __init__(self, route_node, chart_yr, env, intro=''):
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
        self.__output_file = Path(str(env.velocity_folder())+'/'+self.__code+'_array.npy')
        self.__start = chart_yr.first_day_minus_one()
        self.__end = chart_yr.last_day_plus_three()
        self.__seconds = seconds(self.__start, self.__end)
        self.__expected_length = int(self.__seconds / timestep)