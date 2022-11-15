from pathlib import Path
from os import environ, makedirs
import shutil
import dateparser as dp
from datetime import timedelta as td
from warnings import filterwarnings as fw
fw("ignore", message="The localize method is no longer necessary, as this time zone supports the fold attribute",)

timestep = 60
def sign(value): return value/abs(value)
def seconds(start, end): return int((end-start).total_seconds())
def time_to_index(start, time): return seconds(start, time)
def dash_to_zero(value): return 0.0 if str(value).strip() == '-' else value

class DownloadDirectory:

    def make_subfolder(self, name):
        folderpath = Path(str(self.__folder) + '/' + name)
        makedirs(folderpath, exist_ok=True)
        return folderpath

    def set_project_name(self, name):
        if not self.__project_name:
            self.__project_name = name
            self.__folder = Path(environ['USERPROFILE']+'/Downloads/'+name+'/')
            shutil.rmtree(self.__folder, ignore_errors=True)
            makedirs(self.__folder, exist_ok=True)

    def project_name(self): return self.__project_name
    def folder(self): return self.__folder
    # project_name = property(fget=project_name, fset=set_project_name)
    # folder = property(fget=folder, fset=None)

    def __init__(self):
        self.__project_name = ''
        self.__folder = ''

class ChartYear:

    def set_year(self, year):
        if not self.__year:
            self.__year = year
            self.__first_day = dp.parse('1/1/' + str(self.__year))
            self.__last_day = dp.parse('12/31/' + str(self.__year))
            self.__first_day_minus_two = self.__first_day - td(days=2)
            self.__first_day_minus_one = self.__first_day - td(days=1)
            self.__last_day_plus_one = self.__last_day + td(days=1)
            self.__last_day_plus_two = self.__last_day + td(days=2)
            self.__last_day_plus_three = self.__last_day + td(days=3)
    def year(self): return self.__year
    def first_day(self): return self.__first_day
    def first_day_minus_one(self): return self.__first_day_minus_one
    def first_day_minus_two(self): return self.__first_day_minus_two
    def last_day(self): return self.__last_day
    def last_day_plus_one(self): return self.__last_day_plus_one
    def last_day_plus_two(self): return self.__last_day_plus_two
    def last_day_plus_three(self): return self.__last_day_plus_three
    # year = property(fset=set_year, fget=year)
    # first_day = property(fget=first_day)
    # first_day_minus_one = property(fget=first_day_minus_one)
    # first_day_minus_two = property(fget=first_day_minus_two)
    # last_day = property(fget=last_day)
    # last_day_plus_one = property(fget=last_day_plus_one)
    # last_day_plus_two = property(fget=last_day_plus_two)
    # last_day_plus_three = property(fget=last_day_plus_three)

    def __init__(self):
        self.__year = None
        self.__first_day = None
        self.__last_day = None
        self.__first_day_minus_two = None
        self.__first_day_minus_one = None
        self.__last_day_plus_one = None
        self.__last_day_plus_two = None
        self.__last_day_plus_three = None
