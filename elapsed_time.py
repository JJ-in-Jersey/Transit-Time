import numpy as np
import pandas as pd
from project_globals import boat_speeds, timestep, seconds, index_to_time

def distance(water_vf, water_vi, boat_speed, time): return ((water_vf+water_vi)/2+boat_speed)*time
def elapsed_time(starting_index, distances, length):
    index = starting_index
    count = total = 0
    while total < length:
        total += distances[index]
        count += 1
        index += 1
    return count


class ElapsedTimeJob:

    def __init__(self, edge, chart_yr):
        self.__df_forward = pd.DataFrame()
        self.__df_forward['vi'] = edge.start().velocity_array()[:-1]
        self.__df_forward['vf'] = edge.end().velocity_array()[1:]
        self.__df_forward['dist'] = distance(self.__df_forward['vf'], self.__df_forward['vi'], 3, timestep/3600)  # timestep/3600 because velocities are per hour, not second

        start = chart_yr.first_day_minus_two()
        end = chart_yr.last_day()
        e_range = range(0, int(seconds(start, end)/timestep), 1)
        result = np.fromiter([elapsed_time(i, self.__df_forward['dist'].to_numpy(), edge.length()) for i in e_range], dtype=int)

        for i in e_range:
            print(index_to_time(start, i), result[i])

        print(self.__df_forward)


# def elapsedTime(index, viList, vfList, timeInHours, speed, distanceToTraverse):
#     distanceTraveled, timeElapsed, delta, priorDelta = 0.0, 0.0, 0.0, 0.0
#     maxIndex = len(vfList)-2  # len is num of rows, index is range from 0 to len-1, need len-2 because of vfSeries[index+1]
#     while abs(distanceTraveled) <= distanceToTraverse and index <= maxIndex:
#         distanceTraveled += ((vfList[index+1]+viList[index])/2+speed)*timeInHours
#         timeElapsed += timeInHours
#         index += 1
#         priorDelta = delta
#         delta = abs(distanceToTraverse - distanceTraveled)
#
#     if index > maxIndex:
#         return -1
#     elif delta < priorDelta:
#         return timeElapsed
#     else:
#         return timeElapsed-timeInHours
#
# def elapsed_time(edge):
#     # velocities are in nautical miles per hours
#     start_node = edge.start()
#     end_node = edge.end()
#     #while abs(total_distance) <
#     print(len(start_node.velocity_array()), len(end_node.velocity_array()), edge.length())
#     pass
#
#
#
# def timeToNextStation(colName, viCol, vfCol, speed, distance, eRange, timestepInHours, qs):
#     qs.pQ.put(current_process().name)
#     print(' + ' + current_process().name + '    (pid: ' + str(getpid()) + ')')
#     results = {'pid': getpid(), 'pName': current_process().name, 'colName': colName, 'values': None}
#
#     results['values'] = np.fromiter((elapsedTime(row, viCol, vfCol, timestepInHours, speed, distance) for row in eRange), dtype=np.half)
#
#     qs.rQ.put(results)
#     del results
#     print(' - ' + current_process().name)
#     qs.pQ.get()
#
# class ElapsedTimeJob:
#
#     def __init__(self, edge):
#         self.__edge = edge
#         velo_arrays = [edge.start().velocity_array(), edge.end().velocity_array()]
#
#     def execute(self):
#         for dir in range(-1, 1, 2):
#             if dir < 0:
#                 velo_init_array = self.__edge.start().velocity_array()
#                 velo_final_array = self.__edge.end().velocity_array()
#             for speed*dir in boat_speeds:
#
#         pass
#
#     def execute_callback(self, result):
#         pass
#
#     def error_callback(self, result):
#         pass