from pathlib import Path
from os import environ, makedirs, umask
import shutil
import pandas as pd
import time
import dateparser as dp
from datetime import timedelta as td
from datetime import datetime as dt
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

TIMESTEP = 15  # seconds
TIME_RESOLUTION = 15  # rounded to minutes
WINDOW_MARGIN = 10  # minutes
TIMESTEP_MARGIN = WINDOW_MARGIN * 60 / TIMESTEP  # number of timesteps to add to minimum to find edges of time windows
FIVE_HOURS_OF_TIMESTEPS = 5*3600 / TIMESTEP  # only consider windows of transit times less than the midline that are at least 5 ours long (6 hour tide change)
WDW = 1000
DF_FILE_TYPE = 'hdf'  # csv, hdf, pkl

boat_speeds = [v for v in range(-9, -1, 2)]+[v for v in range(3, 10, 2)]  # knots

def sign(value): return value/abs(value)
def rounded_to_minutes(index):
    total_minutes = index // 60
    rounded_seconds = round(total_minutes/TIME_RESOLUTION)*TIME_RESOLUTION*60
    return rounded_seconds

def date_to_index(date_time):
    if isinstance(date_time, dt): return int(time.mktime(date_time.timetuple()))
    elif isinstance(date_time, str): return int(time.mktime(dp.parse(date_time).timetuple()))
def index_to_date(index): return time.strftime("%a %d %b %Y %H:%M", time.localtime(index))

def output_file_exists(path): return True if path.with_suffix('.csv').exists() or path.with_suffix('.pkl').exists() or path.with_suffix('.hdf').exists() or path.with_suffix('.npy').exists() else False

def hours_mins(secs): return "%d:%02d" % (secs // 3600, secs % 3600 // 60)
def mins_secs(secs): return "%d:%02d" % (secs // 60, secs % 60)

class Environment:

    def edge_folder(self, name):
        edge_folder = self.elapsed_time_folder().joinpath(name)
        makedirs(edge_folder, exist_ok=True)
        return edge_folder
    def project_folder(self, args=None):
        if args:
            self.__project_folder = Path(self.__user_profile + '/Developer Workspace/' + args['project_name']+'/')
            if args['delete_data']:
                shutil.rmtree(self.__project_folder, ignore_errors=True)
                makedirs(self.__project_folder, exist_ok=True)
        return self.__project_folder
    def velocity_folder(self):
        if self.__velocity_folder is None and self.project_folder() is not None:
            self.__velocity_folder = self.project_folder().joinpath('Velocity')
            makedirs(self.__velocity_folder, exist_ok=True)
        return self.__velocity_folder
    def interpolation_folder(self):
        if self.__interpolation_folder is None and self.project_folder() is not None:
            self.__interpolation_folder = self.project_folder().joinpath('Interpolation')
            makedirs(self.__interpolation_folder, exist_ok=True)
        return self.__interpolation_folder
    def elapsed_time_folder(self):
        if self.__elapsed_time_folder is None and self.project_folder() is not None:
            self.__elapsed_time_folder = self.project_folder().joinpath('Elapsed Time')
            makedirs(self.__elapsed_time_folder, exist_ok=True)
        return self.__elapsed_time_folder
    def transit_time_folder(self):
        if self.__transit_time_folder is None and self.project_folder() is not None:
            self.__transit_time_folder = self.project_folder().joinpath('Transit Time')
            makedirs(self.__transit_time_folder, exist_ok=True)
        return self.__transit_time_folder
    def speed_folder(self, name):
        tt_folder = self.transit_time_folder().joinpath(name)
        makedirs(tt_folder, exist_ok=True)
        return tt_folder
    def user_profile(self): return self.__user_profile

    def __init__(self):
        self.__project_folder = None
        self.__velocity_folder = None
        self.__interpolation_folder = None
        self.__elapsed_time_folder = None
        self.__transit_time_folder = None
        self.__user_profile = environ['USERPROFILE']
        umask(0)

class ChartYear:

    def year(self): return self.__year
    def waypoint_start_index(self): return date_to_index(self.__first_day_minus_one)
    def waypoint_end_index(self): return date_to_index(self.__last_day_plus_three)
    def waypoint_range(self): return range(self.waypoint_start_index(), self.waypoint_end_index(), TIMESTEP)
    def edge_start_index(self): return date_to_index(self.__first_day_minus_one)
    def edge_end_index(self): return date_to_index(self.__last_day_plus_two)
    def edge_range(self): return range(self.edge_start_index(), self.edge_end_index(), TIMESTEP)
    def transit_start_index(self): return date_to_index(self.__first_day_minus_one)
    def transit_end_index(self): return date_to_index(self.__last_day_plus_one)
    def transit_range(self): return range(self.transit_start_index(), self.transit_end_index(), TIMESTEP)
    def first_day_index(self): return date_to_index(self.__first_day)
    def last_day_index(self): return date_to_index(self.__last_day)

    def initialize(self, args):
        self.__year = args['year']
        self.__first_day = dp.parse('1/1/'+str(self.__year))
        self.__last_day = dp.parse('12/31/' + str(self.__year))
        self.__first_day_minus_one = self.__first_day - td(days=1)
        self.__last_day_plus_one = self.__last_day + td(days=1)
        self.__last_day_plus_two = self.__last_day + td(days=2)
        self.__last_day_plus_three = self.__last_day + td(days=3)

    def __init__(self):
        self.__year = None
        self.__first_date = None
        self.__last_date = None
        self.__first_day = None
        self.__first_day_minus_one = None
        self.__last_day = None
        self.__last_day_plus_one = None
        self.__last_day_plus_two = None
        self.__last_day_plus_three = None
