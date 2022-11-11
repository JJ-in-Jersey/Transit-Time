from warnings import filterwarnings as fw
from bs4 import BeautifulSoup as Soup
from haversine import haversine as hvs, Unit
from project_globals import sign
import project_globals

from velocity import VelocityJob

fw("ignore", message="The localize method is no longer necessary, as this time zone supports the fold attribute",)

class RoutePoint:

    def calculate_velocity(self):
        project_globals.job_queue.put(VelocityJob(self, project_globals.chart_year, project_globals.download_dir))

    def g_next(self): return self.__next
    def s_next(self, pt=None): self.__next = pt if not self.__next and pt else self.__next  # can be set only once
    def g_prev(self): return self.__prev
    def s_prev(self, pt=None): self.__prev = pt if not self.__prev and pt else self.__prev  # can be set only once
    def g_dtn(self): return self.__dist_to_next
    def s_dtn(self, dist=0): self.__dist_to_next = dist if not self.__dist_to_next and dist else self.__dist_to_next  # can be set only once
    def g_dtp(self): return self.__dist_to_prev
    def s_dtp(self, dist=0): self.__dist_to_prev = dist if not self.__dist_to_prev and dist else self.__dist_to_prev  # can be set only once
    def g_ttn(self): return self.__times_to_next
    def s_ttn(self, array=None): self.__times_to_next = array if not self.__times_to_next and array else self.__times_to_next  # can be set only once
    def g_ttp(self): return self.__times_to_prev
    def s_ttp(self, array=0): self.__times_to_prev = array if not self.__times_to_prev and array else self.__times_to_prev  # can be set only once
    def g_coords(self): return self.__coords
    def g_name(self): return self.__name
    def g_url(self): return self.__url
    def g_velocity_array(self): return self.__velocity_array
    def s_velocity_array(self, arr): self.__velocity_array = arr
    next = property(fget=g_next, fset=s_next)
    prev = property(fget=g_prev, fset=s_prev)
    dist_to_next = property(fget=g_dtn, fset=s_dtn)
    dist_to_prev = property(fget=g_dtp, fset=s_dtp)
    times_to_next = property(fget=g_ttn, fset=s_ttn)
    times_to_prev = property(fget=g_ttp, fset=s_ttp)
    coords = property(fget=g_coords)
    name = property(fget=g_name)
    url = property(fget=g_url)
    velocity_array = property(fget=g_velocity_array, fset=s_velocity_array)

    def __init__(self, tag):
        self.__next = None
        self.__prev = None
        self.__dist_to_next = 0
        self.__dist_to_prev = 0
        self.__times_to_next = None
        self.__times_to_prev = None
        self.__url = ''
        self.__velocity_array = None
        self.__name = tag.find('name').text
        self.__coords = (round(float(tag.attrs['lat']), 4), round(float(tag.attrs['lon']), 4))
        if tag.desc:
            self.__url = tag.link.attrs['href']

class GpxRoute:

    directionLookup = {'SN': 'South to North', 'NS': 'North to South', 'EW': 'East to West', 'WE': 'West to East'}

    def calculate_velocities(self):
        pt = self.__first
        while pt:
            pt.calculate_velocity()
            pt = pt.next
        project_globals.job_queue.join()

    def g_first(self): return self.__first
    def g_last(self): return self.__last
    def g_length(self): return self.__length
    def g_direction(self): return GpxRoute.directionLookup[self.__direction]
    first = property(fget=g_first)
    last = property(fget=g_last)
    length = property(fget=g_length)
    direction = property(fget=g_direction)

    def __init__(self, filepath):
        self.__first = None
        self.__last = None
        self.__length = 0
        self.__direction = None

        with open(filepath, 'r') as f:
            gpxfile = f.read()

        tree = Soup(gpxfile, 'xml')
        raw_points =[]
        # raw_points = tuple([RoutePoint(rp) for rp in tree.find_all('rtept')])  # tuple of all waypoint objects
        for p in tree.find_all('rtept'):
            raw_points.append(RoutePoint(p))
        route_points = tuple([rp for rp in raw_points if rp.url])  # tuple of current station waypoint objects
        self.__first = route_points[0]
        self.__last = route_points[-1]

        # calculate distances
        distances = [[bool(pt.url), hvs(pt.coords, raw_points[i+1].coords, unit=Unit.NAUTICAL_MILES)] for i, pt in enumerate(raw_points[:-1])]
        current_node = None
        for i, node in enumerate(distances):
            if node[0]: current_node = node
            else: current_node[1] += node[1]
        station_distances = [node[1] for node in distances if node[0]]

        # update prev/next links to current station waypoints to create linked list
        for i, pt in enumerate(route_points[:-1]): pt.next = route_points[i+1]
        reverse = route_points[::-1]
        for i, pt in enumerate(reverse[:-1]): pt.prev = reverse[i+1]

        # add distances to prev and next to waypoint
        for i, pt in enumerate(route_points[:-1]): pt.dist_to_next = station_distances[i]
        pt = self.__last
        while pt.prev:
            pt.dist_to_prev = pt.prev.dist_to_next
            pt = pt.prev

        # calculate route length and direction
        for d in station_distances: self.__length += d
        corner = (self.__last.coords[0], self.__first.coords[1])
        lat_sign = sign(self.__last.coords[1]-self.__first.coords[1])
        lon_sign = sign(self.__last.coords[0]-self.__first.coords[0])
        lat_dist = hvs(corner, self.__first.coords, unit=Unit.NAUTICAL_MILES)
        lon_dist = hvs(self.__last.coords, corner, unit=Unit.NAUTICAL_MILES)
        if (lat_sign > 0 and lon_sign > 0 and not lon_dist >= lat_dist) or (lat_sign < 0 < lon_sign and not lon_dist >= lat_dist): self.__direction = 'SN'
        elif (lat_sign > 0 > lon_sign and not lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and not lon_dist >= lat_dist): self.__direction = 'NS'
        elif (lat_sign < 0 < lon_sign and lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and lon_dist >= lat_dist): self.__direction = 'EW'
        elif (lat_sign > 0 and lon_sign > 0 and lon_dist >= lat_dist) or (lat_sign > 0 > lon_sign and lon_dist >= lat_dist): self.__direction = 'WE'
