from project_globals import boat_speeds, timestep

ts_hours = timestep/3600  # 3600 seconds per hour

def distance(water_vf, water_vi, boat_speed, elapsed_time): return ((water_vf+water_vi)/2+boat_speed)*elapsed_time

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