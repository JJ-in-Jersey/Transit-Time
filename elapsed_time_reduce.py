import pandas as pd
from functools import reduce
from time import perf_counter

from project_globals import write_df

df_type = 'csv'
shared_columns = ['departure_index', 'departure_time']

def elapsed_time_reduce(route, env):
    init_time = perf_counter()
    print(f'      elapsed time reduce', flush=True)
    elapsed_times_path = env.transit_time_folder().joinpath('elapsed_times')
    elapsed_time_tables = [segment.elasped_time_df() for segment in route.route_segments()]
    et_reduce_df = reduce(lambda left, right: pd.merge(left, right, on=shared_columns), elapsed_time_tables)
    write_df(et_reduce_df, elapsed_times_path, df_type)
    print(f'      elapsed time reduce {round((perf_counter() - init_time) / 60, 2)} minutes', flush=True)
    return et_reduce_df
