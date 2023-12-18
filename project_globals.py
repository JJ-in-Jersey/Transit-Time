from os import makedirs
from num2words import num2words
import shutil
import pandas as pd
import warnings
from datetime import timedelta as td

from tt_globals.globals import Globals
from tt_date_time_tools import date_time_tools as dtt
from tt_os_abstraction.os_abstraction import env

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


class Environment:

    def __init__(self, args):
        folder_suffix = 'Developer Workspace/' + args['project_name'] + '_' + str(args['year']) + '/'
        self.project_folder = env('user_profile').joinpath(folder_suffix)
        self.waypoint_folder = self.project_folder.joinpath('Waypoints')
        self.elapsed_time_folder = self.project_folder.joinpath('Elapsed Time')
        self.transit_time_folder = self.project_folder.joinpath('Transit Time')

        if args['delete_data']:
            shutil.rmtree(self.project_folder, ignore_errors=True)

        makedirs(self.project_folder, exist_ok=True)
        makedirs(self.waypoint_folder, exist_ok=True)
        makedirs(self.elapsed_time_folder, exist_ok=True)
        makedirs(self.transit_time_folder, exist_ok=True)

        for s in Globals.BOAT_SPEEDS:
            makedirs(self.transit_time_folder.joinpath(num2words(s)), exist_ok=True)

    def edge_folder(self, name):
        path = self.elapsed_time_folder.joinpath(name)
        makedirs(path, exist_ok=True)
        return path

    def speed_folder(self, name):
        tt_folder = self.transit_time_folder.joinpath(name)
        makedirs(tt_folder, exist_ok=True)
        return tt_folder


class ChartYear:

    def edge_range(self): return range(dtt.int_timestamp(self.first_day_minus), dtt.int_timestamp(self.last_day_plus_three), Globals.TIMESTEP)

    def transit_range(self): return range(dtt.int_timestamp(self.first_day_minus), dtt.int_timestamp(self.last_day_plus_two), Globals.TIMESTEP)

    def __init__(self):
        self.first_day_minus = Globals.FIRST_DAY - td(days=5)
        self.last_day_plus_one = Globals.LAST_DAY + td(days=1)
        self.last_day_plus_two = Globals.LAST_DAY + td(days=2)
        self.last_day_plus_three = Globals.LAST_DAY + td(days=3)
        self.last_day_plus_four = Globals.LAST_DAY + td(days=4)
