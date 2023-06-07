import pandas as pd
from functools import reduce
from time import perf_counter

from project_globals import boat_speeds
from FileTools import FileTools as ft
from DateTimeTools import DateTimeTools as dtt

def transit_time_reduce(folder, route):

    init_time = perf_counter()
    transit_times_file = folder.joinpath('transit_times')
    if ft.file_exists(transit_times_file):
        print(f':     transit time reduce - reading data file', flush=True)
        et_reduce_df = ft.read_df(transit_times_file)
        print(f':     transit time reduce ({dtt.mins_secs(perf_counter() - init_time)} minutes)', flush=True)
    else:
        print(f':     transit time reduce - reducing elapsed times', flush=True)
        tt_reduce_df = reduce(lambda left, right: pd.merge(left, right, on='departure_index'), elapsed_time_tables)
        ft.write_df(et_reduce_df, transit_times_file)
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
    print(f':     elapsed time reduce ({dtt.mins_secs(perf_counter() - init_time)} minutes)', flush=True)
