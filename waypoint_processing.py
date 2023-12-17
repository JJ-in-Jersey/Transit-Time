# import pandas as pd
from tt_gpx.gpx import InterpolatedDataWP, CurrentStationWP, InterpolatedWP, TideStationWP, SurrogateWP
from tt_file_tools.file_tools import print_file_exists
from velocity import DownloadVelocityJob, SubordinateVelocityAdjustmentJob, SplineFitHarmonicVelocityJob, interpolate_group
from tide import DownloadTideJob


def waypoint_processing(route, cy, job_manager):

    # ---------- TIDE STATION WAYPOINTS ----------

    print(f'\nDownloading tide data for TIDE STATION WAYPOINTS (1st day-1 to last day+4)', flush=True)
    keys = [job_manager.put(DownloadTideJob(cy.year, wp)) for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    # ---------- INTERPOLATION WAYPOINTS ----------Ï

    print(f'\nDownloading current data for INTERPOLATED DATA WAYPOINTS', flush=True)
    keys = [job_manager.put(DownloadVelocityJob(wp, cy.year)) for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    print(f'\nAdjust SUBORDINATE INTERPOLATED DATA WAYPOINTS', flush=True)
    keys = [job_manager.put(SubordinateVelocityAdjustmentJob(wp, cy.year)) for wp in filter(lambda w: isinstance(w, InterpolatedDataWP) and w.type == 'Subordinate', route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS', flush=True)
    for group in route.interpolation_groups:
        interpolate_group(group, job_manager)

    print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    keys = [job_manager.put(SplineFitHarmonicVelocityJob(wp, cy.year)) for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(DownloadVelocityJob(wp, cy.year)) for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    print(f'\nAdjust SUBORDINATE CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(SubordinateVelocityAdjustmentJob(wp, cy.year)) for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)) and w.type == 'Subordinate', route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    keys = [job_manager.put(SplineFitHarmonicVelocityJob(wp, cy.year)) for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints)]
    job_manager.wait()
    for path in [job_manager.get(key).filepath for key in keys]:
        print_file_exists(path)

