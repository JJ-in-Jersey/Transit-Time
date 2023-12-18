from os import makedirs
from num2words import num2words
import shutil
import pandas as pd
import warnings

from tt_globals.globals import Globals
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
