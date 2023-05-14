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
WDW = 5
DF_FILE_TYPE = 'csv'  # csv, hdf, pkl

boat_speeds = [v for v in range(-7, -2, 2)]+[v for v in range(3, 8, 2)]  # knots
def sign(value): return value/abs(value)
def file_exists(path): return True if path.with_suffix('.csv').exists() or path.with_suffix('.pkl').exists() or path.with_suffix('.hdf').exists() or path.with_suffix('.npy').exists() else False

def hours_mins(secs): return "%d:%02d" % (secs // 3600, secs % 3600 // 60)
def mins_secs(secs): return "%d:%02d" % (secs // 60, secs % 60)
def int_timestamp(date): return int(pd.Timestamp(date).timestamp())

class Environment:

    def __init__(self, args):
        self.user_profile = environ['USERPROFILE']
        project_folder = Path(self.user_profile + '/Developer Workspace/' + args['project_name']+'/')
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

    def waypoint_start_index(self): return int_timestamp(self.first_day_minus_one)
    def waypoint_end_index(self): return int_timestamp(self.last_day_plus_three)

    # def edge_start_index(self): return int(pd.Timestamp(self.first_day_minus_one).timestamp())
    # def edge_end_index(self): return int(pd.Timestamp(self.last_day_plus_two).timestamp())
    def edge_range(self): return range(int_timestamp(self.first_day_minus_one), int_timestamp(self.last_day_plus_two), TIMESTEP)

    def transit_start_index(self): return pd.Timestamp(self.first_day_minus_one).timestamp()
    def transit_end_index(self): return pd.Timestamp(self.last_day_plus_one).timestamp()
    def transit_range(self): return range(int(self.transit_start_index()), int(self.transit_end_index()), TIMESTEP)

    def first_day_index(self): return pd.Timestamp(self.first_day, unit='s').timestamp()
    def last_day_index(self): return pd.Timestamp(self.last_day, unit='s').timestamp()

    def __init__(self, args):
        self._year = args['year']  # underscore _year to differentiate it from the method year()
        self.first_day = dp.parse('1/1/'+str(self._year))
        self.first_day_minus_one = self.first_day - td(days=1)
        self.last_day = dp.parse('12/31/' + str(self._year))
        self.last_day_plus_one = self.last_day + td(days=1)
        self.last_day_plus_two = self.last_day + td(days=2)
        self.last_day_plus_three = self.last_day + td(days=3)
        self.first_date = None
        self.last_date = None
