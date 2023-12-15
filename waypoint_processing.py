# import pandas as pd
from tt_gpx.gpx import InterpolatedDataWP, CurrentStationWP, InterpolatedWP, TideStationWP, SurrogateWP
from velocity import DownloadVelocityJob, SplineFitVelocityJob, interpolate_group
from tide import DownloadTideJob
from project_globals import CHECKMARK
# from tt_noaa_data.noaa_data import noaa_current_datafile


def waypoint_processing(route, cy, job_manager):

    # ---------- TIDE STATION WAYPOINTS ----------

    # print(f'\nDownloading tide data for TIDE STATION WAYPOINTS (1st day-1 to last day+4)', flush=True)
    # for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints):
    #     job = DownloadTideJob(wp, cy.year(), cy.waypoint_start_index(), cy.waypoint_end_index())
    #     # job_manager.put(job)
    #     result = job.execute()
    # job_manager.wait()
    #
    # print(f'\nSuccessfully downloaded tide data for TIDE STATION WAYPOINTS', flush=True)
    # for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints):  # clear the result queue
    #     job_manager.get(id(wp))
    #     print(f'{CHECKMARK}     {wp.unique_name}', flush=True)

    # ---------- INTERPOLATION WAYPOINTS ----------

    print(f'\nDownloading current data for INTERPOLATED DATA WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        job = DownloadVelocityJob(wp, cy.year())
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for group in route.interpolation_groups:
        interpolate_group(group, job_manager)

    print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints):
        job = SplineFitVelocityJob(wp)
        job_manager.put(job)
    job_manager.wait()

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)), route.waypoints):
        job = DownloadVelocityJob(wp, cy.year())
        job_manager.put(job)
    job_manager.wait()

    print(f'\nAdding downloaded data to CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints):  # clear the result queue
        result = job_manager.get(id(wp))
        wp.downloaded_data = result.frame
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints):
        job = SplineFitVelocityJob(wp)
        job_manager.put(job)
    job_manager.wait()

    print(f'\nAdding spline data to CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints):  # clear the result queue
        result = job_manager.get(id(wp))
        wp.spline_fit_data = result.frame
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)
