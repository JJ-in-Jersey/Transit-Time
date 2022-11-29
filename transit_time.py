from os.path import exists
from pathlib import Path
import numpy as np
import pandas as pd

from project_globals import boat_speeds

class TransitTimeJob:

    def __init__(self, route, d_dir, intro=''):
        self.__edges = route.route_edges()
        self.__id = id(route)
        self.__intro = intro
        self.__elapsed_times_by_speed = None
        self.__output_file = Path(str(d_dir.transit_time_folder())+'/TT_'+self.__speed+'_array.npy')
        for s in boat_speeds:
            speed_df = pd.DataFrame()
            for e in self.__edges:
                col_name = e.name() + ' ' + str(s)
                speed_df[col_name] = e.elapsed_time_dataframe()[col_name]
            # noinspection PyTypeChecker
            self.__elapsed_times_by_speed[str(s)] = speed_df

    def execute(self):
        if exists(self.__output_file):
            print(f'+     {self.__intro} Transit time ({self.__speed}) reading data file', flush=True)
            # noinspection PyTypeChecker
            return tuple([self.__id, np.load(self.__output_file)])
        else:
            print(f'+     {self.__intro} Transit time ({self.__speed}) calculation starting', flush=True)
            for s in boat_speeds:
                speed_df = pd.DataFrame()
                for re in self.__edges:
                    col_name = re.name() + ' ' + str(s)
                    speed_df[col_name] = re.elapsed_time_dataframe()[col_name]
                speed_df.to_csv(outputfile, index=False)
                # noinspection PyTypeChecker
                self.__elapsed_times_by_speed[str(s)] = speed_df

    def execute_callback(self):
        pass
    def error_callback(self):
        pass

# def ttSum(row, columns, timeStepInMinutes, inputDF):
#     tt = 0
#     rowIncrement = 0
#     for col in columns:
#         val = inputDF.loc[row + rowIncrement, col]
#         rowIncrement += int(inputDF.loc[row + rowIncrement, col]/timeStepInMinutes)
#         if val < 0:
#             print('transit time: negative elapsed time value')
#         else:
#             tt += val
#     return tt
#
