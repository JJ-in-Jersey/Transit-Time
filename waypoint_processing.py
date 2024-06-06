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
        print_file_exists(iwp.folder.joinpath('normalized_velocity.csv'))

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(DownloadVelocityJob(Globals, wp)) for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(SplineFitNormalizedVelocityJob(Globals.DOWNLOAD_INDEX_RANGE, wp)) for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)
