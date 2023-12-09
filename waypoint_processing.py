from tt_gpx.gpx import InterpolatedDataWP, CurrentStationWP, InterpolatedWP, TideStationWP, SurrogateWP
from velocity import DownloadVelocityJob, SplineFitVelocityJob, interpolate_group
from tide import DownloadTideJob
from project_globals import CHECKMARK


def waypoint_processing(route, cy, job_manager):

    # ---------- TIDE STATION WAYPOINTS ----------

    print(f'\nDownloading tide data for TIDE STATION WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: isinstance(w, TideStationWP), route.waypoints):
        job_manager.put(DownloadTideJob(wp, cy.year(), cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    # ---------- INTERPOLATION WAYPOINTS ----------

    print(f'\nDownloading current data for INTERPOLATED DATA WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        job_manager.put(DownloadVelocityJob(wp, cy.year(), cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding downloaded data to INTERPOLATED DATA WAYPOINTS', flush=True)

    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        result = job_manager.get(id(wp))
        wp.downloaded_data = result.dataframe
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)

    print(f'\nSpline fitting data for INTERPOLATED DATA WAYPOINTS', flush=True)
    # normalizing the time points for interpolation, don't want too many points, so using 3 hour timestep
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        job_manager.put(SplineFitVelocityJob(wp, cy.waypoint_start_index(), cy.waypoint_end_index(), 10800))  # 3 hour timestep
    job_manager.wait()

    print(f'\nAdding spline data to INTERPOLATED DATA WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedDataWP), route.waypoints):
        result = job_manager.get(id(wp))
        wp.spline_fit_data = result.dataframe
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)

    print(f'\nInterpolating the data to approximate velocity for INTERPOLATED WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for group in route.interpolation_groups:
        interpolate_group(group, job_manager)

    print(f'\nSpline fit data from INTERPOLATED WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints):
        job_manager.put(SplineFitVelocityJob(wp, cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding spline data to INTERPOLATED WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(w, InterpolatedWP), route.waypoints):
        result = job_manager.get(id(wp))
        wp.spline_fit_data = result.dataframe
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)

    # ---------- CURRENT STATION and SURROGATE WAYPOINTS ----------

    print(f'\nDownloading current data for CURRENT STATION and SURROGATE WAYPOINTS (1st day-1 to last day+4)', flush=True)
    for wp in filter(lambda w: isinstance(wp, CurrentStationWP) or isinstance(wp, SurrogateWP), route.waypoints):
        job_manager.put(DownloadVelocityJob(wp, cy.year(), cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding downloaded data to CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(wp, CurrentStationWP) or isinstance(wp, SurrogateWP), route.waypoints):
        result = job_manager.get(id(wp))
        wp.downloaded_data = result.dataframe
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)

    print(f'\nSpline fit data from CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(wp, CurrentStationWP) or isinstance(wp, SurrogateWP), route.waypoints):
        job_manager.put(SplineFitVelocityJob(wp, cy.waypoint_start_index(), cy.waypoint_end_index()))
    job_manager.wait()

    print(f'\nAdding spline data to CURRENT STATION and SURROGATE WAYPOINTS', flush=True)
    for wp in filter(lambda w: isinstance(wp, CurrentStationWP) or isinstance(wp, SurrogateWP), route.waypoints):
        result = job_manager.get(id(wp))
        wp.spline_fit_data = result.dataframe
        print(f'{CHECKMARK}     {wp.unique_name}', flush=True)
