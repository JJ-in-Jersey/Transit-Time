import logging
from os import environ
from glob import glob
from os.path import join, getctime
from pathlib import Path
from time import sleep
import pandas as pd
from scipy.interpolate import CubicSpline
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from project_globals import seconds, dash_to_zero, time_to_index, timestep

environ['WDM_LOG'] = "false"
logging.getLogger('WDM').setLevel(logging.NOTSET)
wdw = None

def newest_file(folder):
    types = ['*.txt', '*.csv']
    files = []
    for t in types: files.extend(glob(join(folder, t)))
    return max(files, key=getctime) if len(files) else None

def velocity_page(driver, year, code, wait):
    code_string = 'Annual?id='+code  # select annual predictions
    wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='"+code_string+"']"))).click()
    Select(driver.find_element(By.ID, 'fmt')).select_by_index(3)  # select format
    Select(driver.find_element(By.ID, 'timeunits')).select_by_index(1)  # select 24 hour time
    dropdown = Select(driver.find_element(By.ID, 'year'))  # set year
    options = [int(o.text) for o in dropdown.options]
    dropdown.select_by_index(options.index(year))

class VelocityJob():

    def get_driver(self):
        my_options = Options()
        environ['WDM_LOG'] = "false"
        my_options.add_argument('disable-notifications')
        my_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        my_options.add_experimental_option("prefs", {'download.default_directory': str(self.__download_dir)})
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=my_options)
        self.__wdw = WebDriverWait(driver, 1000)
        driver.minimize_window()
        return driver

    def velocity_download(self):
        newest_before = newest_after = newest_file(self.__download_dir)
        self.__wdw.until(ec.element_to_be_clickable((By.ID, 'generatePDF'))).click()
        while newest_before == newest_after:
            sleep(1)
            newest_after = newest_file(self.__download_dir)
        return newest_after

    def execute(self):
        print(f'+     (pool) {self.__point.name} velocity calculation starting', flush=True)
        start = self.__chart_year.first_day_minus_two()
        end = self.__chart_year.last_day_plus_three()
        year = self.__chart_year.year()
        v_range = range(0, seconds(start, end), timestep)
        noaa_dataframe = pd.DataFrame()

        driver = self.get_driver()
        for y in range(year - 1, year + 2):  # + 2 because of range behavior
            driver.get(self.__point.url)
            velocity_page(driver, y, self.__point.code, self.__wdw)
            file = self.velocity_download()
            file_dataframe = pd.read_csv(file, header='infer', converters={' Speed (knots)': dash_to_zero}, parse_dates=['Date_Time (LST/LDT)'])
            noaa_dataframe = pd.concat([noaa_dataframe, file_dataframe])
        driver.quit()

        noaa_dataframe.rename(columns={'Date_Time (LST/LDT)': 'time', ' Event': 'event', ' Speed (knots)': 'velocity'}, inplace=True)
        noaa_dataframe = noaa_dataframe[(start <= noaa_dataframe['time']) & (noaa_dataframe['time'] <= end)]
        noaa_dataframe = noaa_dataframe.reset_index(drop=True)
        noaa_dataframe.to_csv(Path(str(self.__DD.folder())+'/'+self.__point.code+'_dataframe.csv'), index=False)
        x = noaa_dataframe['time'].apply(lambda time: time_to_index(start, time)).to_numpy()
        y = noaa_dataframe['velocity'].to_numpy()
        cs = CubicSpline(x, y)
        self.__point.velocity_array = np.fromiter((cs(x) for x in v_range), dtype=np.half)
        pd.DataFrame(self.__point.velocity_array).to_csv(Path(str(self.__DD.folder())+'/'+self.__point.code+'_array.csv'))
        return True if len(self.__point.velocity_array) else False

    def execute_callback(self, result):
        boom = 'SUCCESSFUL' if result else 'FAILED'
        print(f'-     (pool) {self.__point.name} {len(self.__point.velocity_array)} calculation {boom}', flush=True)

    def __init__(self, point, chart_year, download_dir):
        self.__point = point
        self.__chart_year = chart_year
        self.__download_dir = download_dir.make_subfolder(self.__point.code)
        self.__DD = download_dir

