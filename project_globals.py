from pathlib import Path
from os import makedirs
import shutil
import pandas as pd
import dateparser as dp
import warnings
from datetime import timedelta as td

from tt_date_time_tools import date_time_tools as dtt
from tt_os_abstraction.os_abstraction import user_profile

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

TIMESTEP = 60  # seconds
TIME_RESOLUTION = 15  # rounded to minutes
WINDOW_MARGIN = 30  # minutes
TIMESTEP_MARGIN = int(WINDOW_MARGIN * 60 / TIMESTEP)  # number of timesteps to add to minimum to find edges of time windows
FIVE_HOURS_OF_TIMESTEPS = int(5*3600 / TIMESTEP)  # only consider windows of transit times less than the midline that are at least 5 ours long (6 hour tide change)
WDW = 100

boat_speeds = [v for v in range(-7, -2, 2)]+[v for v in range(3, 8, 2)]  # knots
# boat_speeds = [3, 5]
def sign(value): return value/abs(value)


class Environment:

    def __init__(self, args):
        self.user_profile = user_profile()
        project_folder = Path(self.user_profile + '/Developer Workspace/' + args['project_name'] + '_' + str(args['year']) + '/')
        self.velo_folder = project_folder.joinpath('Velocity')
        self.elapsed_folder = project_folder.joinpath('Elapsed Time')
        self.transit_folder = project_folder.joinpath('Transit Time')

        if args['delete_data']: shutil.rmtree(project_folder, ignore_errors=True)

        makedirs(project_folder, exist_ok=True)
        makedirs(self.velo_folder, exist_ok=True)
        makedirs(self.elapsed_folder, exist_ok=True)
        makedirs(self.transit_folder, exist_ok=True)

    def velocity_folder(self): return self.velo_folder
    def elapsed_time_folder(self): return self.elapsed_folder
    def transit_time_folder(self): return self.transit_folder

    def edge_folder(self, name):
        path = self.elapsed_folder.joinpath(name)
        makedirs(path, exist_ok=True)
        return path

    def speed_folder(self, name):
        tt_folder = self.transit_folder.joinpath(name)
        makedirs(tt_folder, exist_ok=True)
        return tt_folder


class ChartYear:

    def year(self): return self._year  # underscore _year to differentiate it from the method year()

    def waypoint_start_index(self): return dtt.int_timestamp(self.first_day_minus_one)
    def waypoint_end_index(self): return dtt.int_timestamp(self.last_day_plus_four)

    def edge_range(self): return range(dtt.int_timestamp(self.first_day_minus_one), dtt.int_timestamp(self.last_day_plus_three), TIMESTEP)

    def transit_start_index(self): return dtt.int_timestamp(self.first_day_minus_one)
    def transit_end_index(self): return dtt.int_timestamp(self.last_day_plus_two)
    def transit_range(self): return range(self.transit_start_index(), self.transit_end_index(), TIMESTEP)

    def first_day_index(self): return dtt.int_timestamp(self.first_day)
    def last_day_index(self): return dtt.int_timestamp(self.last_day + td(days=1))  # need last full day - date + 24 hours

    def __init__(self, args):
        self._year = args['year']  # underscore _year to differentiate it from the method year()
        self.first_day = dp.parse('1/1/'+str(self._year))
        self.first_day_minus_one = self.first_day - td(days=1)
        self.last_day = dp.parse('12/31/' + str(self._year))
        self.last_day_plus_one = self.last_day + td(days=1)
        self.last_day_plus_two = self.last_day + td(days=2)
        self.last_day_plus_three = self.last_day + td(days=3)
        self.last_day_plus_four = self.last_day + td(days=4)
        self.first_date = None
        self.last_date = None
