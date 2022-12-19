from pathlib import Path
from os import environ, makedirs, umask
import shutil
import dateparser as dp
from datetime import timedelta as td
from warnings import filterwarnings as fw

fw("ignore", message="The localize method is no longer necessary, as this time zone supports the fold attribute",)

TIMESTEP = 15  # seconds
WINDOW_SIZE = 15  # minutes
boat_speeds = [v for v in range(-9, -1, 2)]+[v for v in range(3, 10, 2)]  # knots

def sign(value): return value/abs(value)
def seconds(start, end): return int((end-start).total_seconds())
def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value
def rounded_to_minutes(time, rounded_to_num_minutes):
    basis = dp.parse('1/1/2020')
    total_minutes = int((time - basis).total_seconds())/60
    rounded_seconds = round(total_minutes/rounded_to_num_minutes)*rounded_to_num_minutes*60
    return basis + td(seconds=rounded_seconds)

class Environment:

    def node_folder(self, name):
        node_path = Path(str(self.velocity_folder()) + '/' + name + '/')
        makedirs(node_path, exist_ok=True)
        return node_path
    def velocity_folder(self):
        v_path = Path(str(self.__project_folder) + '/Velocity/')
        makedirs(v_path, exist_ok=True)
        return v_path
    def elapsed_time_folder(self):
        et_path = Path(str(self.__project_folder) + '/Elapsed Time/')
        makedirs(et_path, exist_ok=True)
        return et_path
    def transit_time_folder(self):
        tt_path = Path(str(self.__project_folder) + '/Transit Time/')
        makedirs(tt_path, exist_ok=True)
        return tt_path
    def user_profile(self): return self.__user_profile
    def project_folder(self, args=None):
        if args:
            self.__project_folder = Path(self.__user_profile + '/Downloads/' + args['project_name']+'/')
            if args['delete_data']:
                shutil.rmtree(self.__project_folder, ignore_errors=True)
                makedirs(self.__project_folder, exist_ok=True)
                return self.__project_folder
        else:
            return self.__project_folder

    def __init__(self):
        self.__project_folder = ''
        self.__user_profile = environ['USERPROFILE']
        umask(0)

class ChartYear:

    def set_year(self, args):
        if not self.__year:
            self.__year = args['year']
            self.__first_date = '1/1/'+str(self.__year)
            self.__last_date = '12/31/' + str(self.__year)
            self.__first_day = dp.parse(self.__first_date)
            self.__first_day_minus_one = self.__first_day - td(days=1)
            self.__last_day = dp.parse(self.__last_date)
            self.__last_day_plus_one = self.__last_day + td(days=1)
            self.__last_day_plus_two = self.__last_day + td(days=2)
            self.__last_day_plus_three = self.__last_day + td(days=3)
            self.__index_basis = self.__first_day_minus_one

    def year(self): return self.__year
    def first_day_minus_one(self): return self.__first_day_minus_one
    def first_day(self): return self.__first_day
    def last_day(self): return self.__last_day
    def last_day_plus_one(self): return self.__last_day_plus_one
    def last_day_plus_two(self): return self.__last_day_plus_two
    def last_day_plus_three(self): return self.__last_day_plus_three
    def time_to_index(self, time): return seconds(self.__index_basis, time)
    def index_to_time(self, index): return self.__index_basis + td(seconds=index)
    def index_basis(self): return self.__index_basis

    def __init__(self):
        self.__year = None
        self.__first_date = None
        self.__last_date = None
        self.__first_day_minus_one = None
        self.__first_day = None
        self.__last_day = None
        self.__last_day_plus_one = None
        self.__last_day_plus_two = None
        self.__last_day_plus_three = None
        self.__index_basis = None
