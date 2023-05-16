import pandas as pd
# import numpy as np
from functools import reduce
from time import perf_counter

from project_globals import boat_speeds
from FileTools import FileTools as ft
from GPX import Route, Edge
from MemoryHelper import MemoryHelper as mh
from DateTimeTools import DateTimeTools as dtt

def elapsed_time_reduce(folder, route):

    init_time = perf_counter()
    elapsed_times_file = folder.joinpath('elapsed_times')
    if ft.file_exists(elapsed_times_file):
        print(f':     elapsed time reduce - reading data file', flush=True)
        et_reduce_df = ft.read_df(elapsed_times_file)
        print(f':     elapsed time reduce ({dtt.mins_secs(perf_counter() - init_time)} minutes)', flush=True)
    else:
        print(f':     elapsed time reduce - reducing elapsed times', flush=True)
        elapsed_time_tables = [edge.output_data for edge in route.elapsed_time_path.edges]
        et_reduce_df = reduce(lambda left, right: pd.merge(left, right, on='departure_index'), elapsed_time_tables)
        ft.write_df(et_reduce_df, elapsed_times_file)
        print(f':     elapsed time reduce ({dtt.mins_secs(perf_counter() - init_time)} minutes)', flush=True)

    for speed in boat_speeds:
        speed_file = folder.joinpath('speed' + str(speed))
        if ft.file_exists(speed_file):
            print(f'      reading {speed} data file', flush=True)
            speed_df = ft.read_df(speed_file)
        else:
            print(f':     elapsed time reduce - processing speed {speed}', flush=True)
            speed_columns = [str(speed) + ' ' + edge.unique_name for edge in route.elapsed_time_path.edges]
            speed_df = et_reduce_df[speed_columns]
            ft.write_df(speed_df, speed_file)
        route.elapsed_time_lookup(speed, speed_df)
    del et_reduce_df
    print(f':     elapsed time reduce ({mins_secs(perf_counter() - init_time)} minutes)', flush=True)
