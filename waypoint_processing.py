# import pandas as pd
from tt_gpx.gpx import InterpolatedDataWP, CurrentStationWP, InterpolatedWP, TideStationWP, SurrogateWP
from velocity import DownloadVelocityJob, SubordinateVelocityAdjustmentJob, SplineFitHarmonicVelocityJob, interpolate_group
from tide import DownloadTideJob


def waypoint_processing(route, cy, job_manager):

    # ---------- TIDE STATION WAYPOINTS ----------

    print(f'\nDownloading tide data for TIDE STATION WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints):
        job = DownloadTideJob(cy.year, wp)
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    # ---------- INTERPOLATION WAYPOINTS ----------Ï

    print(f'\nDownloading current data for INTERPOLATED DATA WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        job = DownloadVelocityJob(wp, cy.year)
        job_manager.put(job)
        # result = job.execute()
    job_manager.wait()

    print(f'\nAdjust SUBORDINATE INTERPOLATED DATA WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP) and w.type == 'Subordinate', route.waypoints):
        job = SubordinateVelocityAdjustmentJob(wp, cy.year)
        job_manager.put(job)
    job_manager.wait()

    print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS', flush=True)
    for group in route.interpolation_groups:
        interpolate_group(group, job_manager)

    print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints):
        job = SplineFitHarmonicVelocityJob(wp, cy.year)
        job_manager.put(job)
    job_manager.wait()

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)), route.waypoints):
        job = DownloadVelocityJob(wp, cy.year)
        job_manager.put(job)
    job_manager.wait()

    print(f'\nAdjust SUBORDINATE CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: (isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP)) and w.type == 'Subordinate', route.waypoints):
        job = SubordinateVelocityAdjustmentJob(wp, cy.year)
        job_manager.put(job)
    job_manager.wait()

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, CurrentStationWP) or isinstance(w, SurrogateWP), route.waypoints):
        job = SplineFitHarmonicVelocityJob(wp, cy.year)
        job_manager.put(job)
    job_manager.wait()
