from pathlib import Path

from tt_file_tools import file_tools as ft
from tt_job_manager.job_manager import Job
from tt_noaa_data.noaa_data import noaa_tide_dataframe
from tt_gpx.gpx import TideStationWP


def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value


class DownloadedTideCSV:

    def __init__(self, f_day, l_day, folder, code):

        self.filepath = folder.joinpath('tide.csv')

        if not self.filepath.exists():
            frame = noaa_tide_dataframe(f_day, l_day, code)
            ft.write_df(frame, self.filepath)


class DownloadTideJob(Job):

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, f_day, l_day, waypoint: TideStationWP):
        result_key = id(waypoint)
        arguments = tuple([f_day, l_day, waypoint.folder, waypoint.code])
        super().__init__(waypoint.unique_name, result_key, DownloadedTideCSV, arguments)


# from tt_chrome_driver import chrome_driver as cd
# from selenium.webdriver.support.ui import Select
# from selenium.webdriver.support import expected_conditions as ec
# from selenium.webdriver.common.by import By
#
# class DownloadedTideDataframe:
#
#     @staticmethod
#     def click_sequence(year, code):
#         code_string = '/noaatideannual.html?id=' + code
#         cd.WDW.until(ec.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='" + code_string + "']"))).click()
#         dropdown = Select(cd.driver.find_element(By.ID, 'year'))
#         options = [int(o.text) for o in dropdown.options]
#         dropdown.select_by_index(options.index(year))
#         Select(cd.driver.find_element(By.ID, 'clock')).select_by_index(1)  # select 24 hour time
#         Select(cd.driver.find_element(By.ID, 'format')).select_by_index(2)
#
#     @staticmethod
#     def download_event():
#         cd.WDW.until(ec.element_to_be_clickable((By.ID, 'create_annual_tide_tables'))).click()
#
#     def __init__(self, year, download_path, folder, url, code, start_index, end_index):
#         self.frame = None
#
#         if download_path.exists():
#             self.frame = ft.read_df(download_path)
#         else:
#             frame = pd.DataFrame()
#             cd.set_driver(folder)
#
#             for y in range(year - 1, year + 2):  # + 2 because of range behavior
#                 cd.driver.get(url)
#                 self.click_sequence(y, code)
#                 downloaded_file = ft.wait_for_new_file(folder, self.download_event)
#                 file_df = TideXMLDataframe(downloaded_file).frame
#                 ft.write_df(frame, folder.joinpath(str(code) + '_' + str(y)))
#                 frame = pd.concat([frame, file_df])
#
#             cd.driver.quit()
#
#             frame['date_index'] = frame['date_time'].apply(dtt.int_timestamp)
#             frame = frame[(start_index <= frame['date_index']) & (frame['date_index'] <= end_index)]
#
#             ft.write_df(frame, download_path)
#             self.frame = frame
