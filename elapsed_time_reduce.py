import pandas as pd
from functools import reduce

from project_globals import write_df

df_type = 'csv'

def elapsed_time_reduce(route, speed, env):
    print(f'      elapsed_time_reduce {speed} {env}', flush=True)
    elapsed_times_path = env.transit_time_folder().joinpath('et_reduce_' + str(speed))
    shared_columns = ['departure_index', 'departure_time']
    elapsed_time_tables = [segment.elapsed_time_table_path() for segment in route.route_segments()]
    et_reduce_df = reduce(lambda left, right: pd.merge(left, right, on=shared_columns), elapsed_time_tables)
    write_df(et_reduce_df, elapsed_times_path, df_type)
    return et_reduce_df
