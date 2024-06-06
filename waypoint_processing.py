from tt_globals.globals import Globals
from tt_gpx.gpx import CurrentStationWP, InterpolatedWP, SurrogateWP
from tt_file_tools.file_tools import print_file_exists
from velocity import DownloadVelocityJob, SplineFitNormalizedVelocityJob


def waypoint_processing(route, job_manager):

    # ---------- TIDE STATION WAYPOINTS ----------

    # print(f'\nDownloading tide data for TIDE STATION WAYPOINTS', flush=True)
    # keys = [job_manager.put(DownloadTideJob(Globals.FIRST_DOWNLOAD_DAY, Globals.LAST_DOWNLOAD_DAY, wp)) for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints)]
    # job_manager.wait()
    # for path in [job_manager.get(key).filepath for key in keys]:
    #     print_file_exists(path)

    # ---------- INTERPOLATION WAYPOINTS ----------Ï

    for iwp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints):
        print(f'\nDownloading data for interpolated waypoint "{iwp.name}"', flush=True)
        keys = [job_manager.put(DownloadVelocityJob(Globals, wp)) for wp in iwp.data_waypoints]
        job_manager.wait()
        for path in [job_manager.get(key).filepath for key in keys]:
            print_file_exists(path)

        print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS', flush=True)
        iwp.interpolate(job_manager)
        # output_filepath = iwp.folder.joinpath('interpolated_velocity.csv')
        # velocity_data = iwp.get_velocity_data()
        #
        # keys = [job_manager.put(InterpolatePointJob(interpolation_pt, velocity_data, i)) for i in range(len(velocity_data[0]))]
        # job_manager.wait()
        # for i in range(len(velocity_data[0])):
        #     job = InterpolatePointJob(interpolation_pt, velocity_data, i)
        #     output = job.execute()

        # result_array = tuple([job_manager.get(key).date_velocity for key in keys])
        # frame = pd.DataFrame(result_array, columns=['date_index', 'velocity'])
        # frame.sort_values('date_index', inplace=True)
        # frame['date_time'] = frame['date_index'].apply(index_to_date)
        # frame.reset_index(drop=True, inplace=True)
        # # interpolation_pt.downloaded_data = frame
        # ft.write_df(frame, output_filepath)

    # for group in route.interpolation_groups:
    #     interpolate_group(group, job_manager)
    #     if group.folder.joinpath('interpolated_velocity.csv').exists():
    #         pass

    # print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    # keys = [job_manager.put(SplineFitHarmonicVelocityJob(Globals.DOWNLOAD_INDEX_RANGE, wp)) for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints)]
    # job_manager.wait()
    # for path in [job_manager.get(key).filepath for key in keys]:
    #     print_file_exists(path)

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(DownloadVelocityJob(Globals, wp)) for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    # print(f'\nAdjust SUBORDINATE CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    # keys = [job_manager.put(SubordinateVelocityAdjustmentJob(wp)) for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)) and w.type == 'Subordinate', route.waypoints)]
    # job_manager.wait()
    # for path in [job_manager.get(key).filepath for key in keys]:
    #     print_file_exists(path)

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(SplineFitNormalizedVelocityJob(Globals.DOWNLOAD_INDEX_RANGE, wp)) for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)
