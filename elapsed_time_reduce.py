import pandas as pd
from functools import reduce
from time import perf_counter

from project_globals import read_df, write_df, min_sec, output_file_exists, shared_columns

df_type = 'hdf'

def elapsed_time_reduce(route, env):
    init_time = perf_counter()
    elapsed_times_path = env.transit_time_folder().joinpath('elapsed_times')
    if output_file_exists(elapsed_times_path):
        print(f':     elapsed time reduce reading data file', flush=True)
        et_reduce_df = read_df(elapsed_times_path)
    else:
        print(f':     elapsed time reduce', flush=True)
        elapsed_time_tables = [segment.elapsed_time_df() for segment in route.route_segments()]
        et_reduce_df = reduce(lambda left, right: pd.merge(left, right, on=shared_columns), elapsed_time_tables)
        write_df(et_reduce_df, elapsed_times_path, df_type)
    print(f':     elapsed time reduce {min_sec(perf_counter() - init_time)} minutes', flush=True)
    return et_reduce_df