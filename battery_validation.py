from time import perf_counter
import pandas as pd


from tt_chrome_driver import chrome_driver as cd
# noinspection PyPep8Naming
from tt_file_tools import file_tools as ft
from tt_date_time_tools import date_time_tools as dtt
from tt_geometry.geometry import time_to_degrees

from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

from project_globals import WDW

#  VELOCITIES ARE DOWNLOADED, CALCULATED AND SAVE AS NAUTICAL MILES PER HOUR!


class TideXMLFile(ft.XMLFile):
    def __init__(self, filepath):
        super().__init__(filepath)

        self.dataframe = pd.DataFrame(columns=['date', 'time', 'HL'])
        for i in self.tree.find_all('item'):
            self.dataframe.loc[len(self.dataframe)] = [i.find('date').text, i.find('time').text, i.find('highlow').text]

        # need datetime so that when adding 4+ hours, it can switch to the next day
        self.dataframe['datetime'] = pd.to_datetime(self.dataframe['date'] + ' ' + self.dataframe['time'], format='%Y/%m/%d %I:%M %p')


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


def download_event(wdw): wdw[0].until(ec.element_to_be_clickable((By.ID, 'create_annual_tide_tables'))).click()


def set_up_download(year, driver):
    dropdown = Select(driver.find_element(By.ID, 'year'))
    options = [int(o.text) for o in dropdown.options]
    dropdown.select_by_index(options.index(year))
    Select(driver.find_element(By.ID, 'format')).select_by_index(2)


def index_arc_df(frame, name):
    date_time_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    date_angle_dict = {key: [] for key in sorted(list(set(frame['date'])))}
    columns = frame.columns.to_list()
    for i, row in frame.iterrows():
        date_time_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('time')])
        date_angle_dict[row.iloc[columns.index('date')]].append(row.iloc[columns.index('angle')])

    df = pd.DataFrame(columns=['date', 'name', 'time', 'angle'])
    for key in date_time_dict.keys():
        times = date_time_dict[key]
        angles = date_angle_dict[key]
        for i in range(len(times)):
            df.loc[len(df.name)] = [key, name + ' ' + str(i + 1), times[i], angles[i]]
    return df


class DownloadedDataframe:

    def __init__(self, year, waypoint, headless=False):
        self.headless = headless

        if ft.csv_npy_file_exists(waypoint.downloaded_data_filepath):
            self.downloaded_df = ft.read_df(waypoint.downloaded_data_filepath)
            self.downloaded_df['date'] = pd.to_datetime(self.downloaded_df['date'], format='%Y/%m/%d')
            self.downloaded_df['time'] = pd.to_datetime(self.downloaded_df['time'], format='%I:%M %p')
            self.downloaded_df['datetime'] = pd.to_datetime(self.downloaded_df['datetime'])
        else:
            self.downloaded_df = pd.DataFrame()
            driver = cd.get_driver(waypoint.folder, headless)
            wdw = WebDriverWait(driver, WDW)
            driver.get(waypoint.noaa_url)
            code_string = '/noaatideannual.html?id=' + waypoint.code
            wdw.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()

            for y in range(year - 1, year + 2):  # + 2 because of range behavior
                set_up_download(y, driver)
                downloaded_file = ft.wait_for_new_file(waypoint.folder, download_event, wdw)
                file_df = TideXMLFile(downloaded_file).dataframe
                self.downloaded_df = pd.concat([self.downloaded_df, file_df])

            driver.quit()
            ft.write_df(self.downloaded_df, waypoint.downloaded_data_filepath)


class TideStationJob:

    def execute(self):
        print(f'+     {self.waypoint.unique_name}', flush=True)
        ddf = DownloadedDataframe(self.year, self.waypoint, self.headless)
        dataframe = ddf.downloaded_df

        # northbound depart 4.5 hours after low water at the battery
        # southbound depart 4 hours after high water at the battery
        south_df = dataframe[dataframe['HL'] == 'H']
        south_df.insert(len(south_df.columns), 'best_time', south_df['datetime'] + pd.Timedelta(hours=4))
        north_df = dataframe[dataframe['HL'] == 'L']
        north_df.insert(len(north_df.columns), 'best_time', north_df['datetime'] + pd.Timedelta(hours=4.5))
        best_df = north_df.drop(['date', 'time', 'HL', 'datetime'], axis=1)
        best_df = pd.concat([best_df, south_df.drop(['date', 'time', 'HL', 'datetime'], axis=1)], ignore_index=True)

        best_df['date'] = best_df['best_time'].dt.date
        best_df['time'] = best_df['best_time'].dt.time
        best_df['angle'] = best_df['time'].apply(time_to_degrees)
        best_df = best_df.drop(['best_time'], axis=1)
        best_df = best_df[best_df['date'] >= self.first_day]
        best_df = best_df[best_df['date'] <= self.last_day]
        self.battery_lines = index_arc_df(best_df, 'Battery Line')

        ft.write_df(self.battery_lines, self.waypoint.final_data_filepath)

    def execute_callback(self, result):
        print(f'-     {self.waypoint.unique_name} {dtt.mins_secs(perf_counter() - result[2])} minutes', flush=True)

    def error_callback(self, result):
        print(f'!     {self.waypoint.unique_name} process has raised an error: {result}', flush=True)

    def __init__(self, cy, waypoint, timestep, headless=False):
        self.headless = headless
        self.year = cy.year()
        self.waypoint = waypoint
        self.first_day = cy.first_day.date()
        self.last_day = cy.last_day.date()
        self.timestep = timestep
        self.result_key = id(waypoint)
        self.battery_lines = None
