from warnings import filterwarnings as fw
from bs4 import BeautifulSoup as Soup
from haversine import haversine as hvs, Unit
from project_globals import sign
import project_globals

from velocity import VelocityJob

fw("ignore", message="The localize method is no longer necessary, as this time zone supports the fold attribute",)

class GpxRoute:

    __first = None
    __last = None
    __length = 0
    __direction = None

    __dictionary = {'SN': 'South to North', 'NS': 'North to South', 'EW': 'East to West', 'WE': 'West to East'}

    def __gfirst(self): return self.__first
    def __glast(self): return self.__last
    def __glength(self): return self.__length
    def __gdirection(self): return self.lookup(self.__direction)
    first = property(fget=__gfirst, fset=None, fdel=None)
    last = property(fget=__glast, fset=None, fdel=None)
    length = property(fget=__glength, fset=None, fdel=None)
    direction = property(fget=__gdirection, fset=None, fdel=None)
    def lookup(self, key): return self.__dictionary[key]

    def __init__(self, filepath):
        with open(filepath, 'r') as f:
            gpxfile = f.read()

        tree = Soup(gpxfile, 'xml')
        raw_points =[]
        # raw_points = tuple([RoutePoint(rp) for rp in tree.find_all('rtept')])  # tuple of all waypoint objects
        for p in tree.find_all('rtept'):
            raw_points.append(RoutePoint(p))
        route_points = tuple([rp for rp in raw_points if rp.url])  # tuple of current station waypoint objects
        self.__dictionary.update({pt.name: pt for pt in route_points})  # add waypoints lookup by name
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

class RoutePoint:

    def __gnext(self): return self.__next
    def __snext(self, pt=None): self.__next = pt if not self.__next and pt else self.__next  # can be set only once
    def __gprev(self): return self.__prev
    def __sprev(self, pt=None): self.__prev = pt if not self.__prev and pt else self.__prev  # can be set only once
    def __gdtn(self): return self.__dist_to_next
    def __sdtn(self, dist=0): self.__dist_to_next = dist if not self.__dist_to_next and dist else self.__dist_to_next  # can be set only once
    def __gdtp(self): return self.__dist_to_prev
    def __sdtp(self, dist=0): self.__dist_to_prev = dist if not self.__dist_to_prev and dist else self.__dist_to_prev  # can be set only once
    def __gttn(self): return self.__times_to_next
    def __sttn(self, array=None): self.__times_to_next = array if not self.__times_to_next and array else self.__times_to_next  # can be set only once
    def __gttp(self): return self.__times_to_prev
    def __sttp(self, array=0): self.__times_to_prev = array if not self.__times_to_prev and array else self.__times_to_prev  # can be set only once
    def __gcoords(self): return self.__coords
    def __gname(self): return self.__name
    def __gurl(self): return self.__url
    def __svelo(self, velo_arr): self.__velocity_array = velo_arr  # if not self.__velocity_array and len(velo_arr) else self.__velocity_array  # can be set only once
    def __gvelo(self): return self.__velocity_array
    next = property(fget=__gnext, fset=__snext, fdel=None)
    prev = property(fget=__gprev, fset=__sprev, fdel=None)
    dist_to_next = property(fget=__gdtn, fset=__sdtn, fdel=None)
    dist_to_prev = property(fget=__gdtp, fset=__sdtp, fdel=None)
    times_to_next = property(fget=__gttn, fset=__sttn, fdel=None)
    times_to_prev = property(fget=__gttp, fset=__sttp, fdel=None)
    coords = property(fget=__gcoords, fset=None, fdel=None)
    name = property(fget=__gname, fset=None, fdel=None)
    url = property(fget=__gurl, fset=None, fdel=None)
    velocity_array = property(fget=__gvelo, fset=__svelo, fdel=None)

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
            project_globals.job_queue.put(VelocityJob(self, project_globals.chart_year, project_globals.download_dir))
