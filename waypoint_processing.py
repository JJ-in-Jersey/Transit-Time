# import pandas as pd
from tt_gpx.gpx import InterpolatedDataWP, CurrentStationWP, InterpolatedWP, TideStationWP, SurrogateWP
from velocity import DownloadVelocityJob, SubordinateVelocityAdjustmentJob, SplineFitHarmonicVelocityJob, interpolate_group
from tide import DownloadTideJob


def waypoint_processing(route, cy, job_manager):

    # ---------- TIDE STATION WAYPOINTS ----------

    # subordinate_index_range = range(cy.velocity_start_index, cy.velocity_end_index, 3600)
    # harmonic_index_range = range(cy.velocity_start_index, cy.velocity_end_index, TIMESTEP)
    # print(f'\nDownloading tide data for TIDE STATION WAYPOINTS (1st day-1 to last day+4)', flush=True)
    # for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints):
    #     job = DownloadTideJob(wp, cy.year, cy.waypoint_start_index(), cy.waypoint_end_index())
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
        job = DownloadVelocityJob(wp, cy.year)
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    print(f'\nSpline fit data from INTERPOLATED DATA WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP) and w.type == 'Subordinate', route.waypoints):
        job = SubordinateVelocityAdjustmentJob(wp, cy.year)
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        job = SplineFitHarmonicVelocityJob(wp)
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for group in route.interpolation_groups:
        interpolate_group(group, job_manager)

    print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints):
        print(wp.type)
        job = SplineFitHarmonicVelocityJob(wp)
        job_manager.put(job)
    job_manager.wait()

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)), route.waypoints):
        job = DownloadVelocityJob(wp, cy.year)
        job_manager.put(job)
    job_manager.wait()

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints):
        job = SplineFitHarmonicVelocityJob(wp)
        job_manager.put(job)
    job_manager.wait()
