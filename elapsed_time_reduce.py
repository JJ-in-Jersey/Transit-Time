import pandas as pd
from functools import reduce
from time import perf_counter

from project_globals import DF_FILE_TYPE, mins_secs, output_file_exists, boat_speeds
from ReadWrite import ReadWrite as rw

# noinspection PyProtectedMember
def elapsed_time_reduce(mpm, route):

    init_time = perf_counter()
    elapsed_times_path = mpm.env.elapsed_time_folder().joinpath('elapsed_times')
    if output_file_exists(elapsed_times_path):
        print(f':     elapsed time reduce - reading data file', flush=True)
        et_reduce_df = rw.read_df(elapsed_times_path)
    else:
        print(f':     elapsed time reduce', flush=True)
        elapsed_time_tables = [segment.elapsed_times_df() for segment in route.elapsed_time_segments]
        et_reduce_df = reduce(lambda left, right: pd.merge(left, right, on='departure_index'), elapsed_time_tables)
        rw.write_df(et_reduce_df, elapsed_times_path, DF_FILE_TYPE)

    for speed in boat_speeds:
        speed_path = mpm.env.elapsed_time_folder().joinpath('speed' + str(speed))
        speed_columns = [str(speed) + ' ' + segment._name for segment in route.elapsed_time_segments]
        speed_df = et_reduce_df[speed_columns]
        rw.write_df(speed_df, speed_path, DF_FILE_TYPE)
        route.elapsed_time_lookup(speed, speed_df)
    del et_reduce_df

    print(f':     elapsed time reduce {mins_secs(perf_counter() - init_time)} minutes', flush=True)
