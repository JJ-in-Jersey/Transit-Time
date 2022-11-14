from warnings import filterwarnings as fw
from bs4 import BeautifulSoup as Soup
from haversine import haversine as hvs, Unit
import project_globals
import numpy

from project_globals import sign

from velocity import VelocityJob

fw("ignore", message="The localize method is no longer necessary, as this time zone supports the fold attribute",)


class Node:

    def next_edge(self, edge=None):
        self.__next_edge = edge if edge and not self.__next_edge else self.__next_edge  # can be set only once
        return self.__next_edge
    def prev_edge(self, edge=None):
        self.__prev_edge = edge if edge and not self.__prev_edge else self.__prev_edge  # can be set only once
        return self.__prev_edge
    def next_node(self, point=None):
        self.__next_node = point if point and not self.__next_node else self.__next_node  # can be set only once
        return self.__next_node
    def prev_node(self, point=None):
        self.__prev_node = point if point and not self.__prev_node else self.__prev_node  # can be set only once
        return self.__prev_node
    def coords(self): return self.__coords
    def name(self): return self.__name

    def __init__(self, gpxtag):
        self.__gpxtag = gpxtag
        self.__name = gpxtag.find('name').text
        self.__coords = (round(float(gpxtag.attrs['lat']), 4), round(float(gpxtag.attrs['lon']), 4))
        self.__next_edge = self.__prev_edge = self.__next_node = self.__prev_node = None

class RouteNode(Node):

    def next_route_edge(self, edge=None):
        self.__next_route_edge = edge if edge and not self.__next_route_edge else self.__next_route_edge  # can be set only once
        return self.__next_route_edge
    def prev_route_edge(self, edge=None):
        self.__prev_route_edge = edge if edge and not self.__prev_route_edge else self.__prev_route_edge  # can be set only once
        return self.__prev_route_edge
    def next_route_node(self, node=None):
        self.__next_route_node = node if node and not self.__next_route_node else self.__next_route_node  # can be set only once
        return self.__next_route_node
    def prev_route_node(self, node=None):
        self.__prev_route_node = node if node and not self.__prev_route_node else self.__prev_route_node  # can be set only once
        return self.__prev_route_node
    def url(self): return self.__url
    def code(self): return self.__code
    def velocity_array(self, array=None):
        self.__velo_array = array if isinstance(array, numpy.ndarray) and not self.__velo_array else self.__velo_array  # can be set only once
        return self.__velo_array

    def __init__(self, gpxtag):
        super().__init__(gpxtag)
        self.__url = gpxtag.link.attrs['href']
        self.__code = self.__url.split('=')[1].split('_')[0]
        self.__next_route_edge = self.__prev_route_edge = self.__next_route_node = self.__prev_route_node = self.__velo_array = None

class Edge:

    def start(self): return self.__start
    def end(self): return self.__end
    def length(self): return self.__length
    def calc_length(self, start, end): return hvs(start.coords(), end.coords(), unit=Unit.NAUTICAL_MILES)

    def __init__(self, start, end):
        self.__start = start
        self.__end = end
        self.__length = self.calc_length(start, end)
        start.next_edge(self)
        end.prev_edge(self)
        start.next_node(end)
        end.prev_node(start)

class RouteEdge:

    def length(self): return self.__length
    @staticmethod
    def calc_length(start, end):
        edges = []
        node = start
        while node != end:
            edges.append(node.next_edge().length())
            node = node.next_node()
        return sum(edges)

    def __init__(self, start, end):
        self.__start = start
        self.__end = end
        self.__length = self.calc_length(start, end)
        start.next_route_edge(self)
        end.prev_route_edge(self)
        start.next_route_node(end)
        end.prev_route_node(start)

class GpxRoute:

    directionLookup = {'SN': 'South to North', 'NS': 'North to South', 'EW': 'East to West', 'WE': 'West to East'}

    def nodes(self): return self.__nodes
    def route_nodes(self): return self.__route_nodes
    def length(self): return self.__length
    def direction(self): return GpxRoute.directionLookup[self.__direction]

    def __init__(self, filepath):
        self.__nodes = self.__route_nodes = self.__edges = self.__route_edges = self.__direction = None
        self.__length = 0

        with open(filepath, 'r') as f:
            gpxfile = f.read()
        tree = Soup(gpxfile, 'xml')

        # create graph nodes
        self.__nodes = [RouteNode(point) if point.desc else Node(point) for point in tree.find_all('rtept')]
        self.__route_nodes = [node for node in self.__nodes if isinstance(node, RouteNode)]
        # create graph edges - instantiating edges updates next and prev references nodes
        for i, node in enumerate(self.__nodes[:-1]): Edge(node, self.__nodes[i+1])
        for i, node in enumerate(self.__route_nodes[:-1]): RouteEdge(node, self.__route_nodes[i+1])
        self.__edges = [node.next_edge() for node in self.__nodes[:-1]]
        self.__route_edges = [node.next_route_edge() for node in self.__route_nodes[:-1]]
        self.__length = sum([node.next_edge().length() for node in self.__nodes[:-1]])

        # calculate direction
        corner = (self.__nodes[-1].coords()[0], self.__nodes[0].coords()[1])
        lat_sign = sign(self.__nodes[-1].coords()[1]-self.__nodes[0].coords()[1])
        lon_sign = sign(self.__nodes[-1].coords()[0]-self.__nodes[0].coords()[0])
        lat_dist = hvs(corner, self.__nodes[0].coords(), unit=Unit.NAUTICAL_MILES)
        lon_dist = hvs(self.__nodes[-1].coords(), corner, unit=Unit.NAUTICAL_MILES)
        if (lat_sign > 0 and lon_sign > 0 and not lon_dist >= lat_dist) or (lat_sign < 0 < lon_sign and not lon_dist >= lat_dist): self.__direction = 'SN'
        elif (lat_sign > 0 > lon_sign and not lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and not lon_dist >= lat_dist): self.__direction = 'NS'
        elif (lat_sign < 0 < lon_sign and lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and lon_dist >= lat_dist): self.__direction = 'EW'
        elif (lat_sign > 0 and lon_sign > 0 and lon_dist >= lat_dist) or (lat_sign > 0 > lon_sign and lon_dist >= lat_dist): self.__direction = 'WE'

class Route:

    directionLookup = {'SN': 'South to North', 'NS': 'North to South', 'EW': 'East to West', 'WE': 'West to East'}

    def calculate_velocities(self):
        pt = self.__first
        while pt:
            pt.calculate_velocity()
            pt = pt.next()
        project_globals.job_queue.join()

    def first(self): return self.__first
    def last(self): return self.__last
    def length(self): return self.__length
    def direction(self): return GpxRoute.directionLookup[self.__direction]

    def __init__(self, filepath):
        self.__first = None
        self.__last = None
        self.__length = 0
        self.__direction = None

        with open(filepath, 'r') as f:
            gpxfile = f.read()

        tree = Soup(gpxfile, 'xml')
        all_nodes =[]
        # all_nodes = tuple([RoutePoint(rp) for rp in tree.find_all('rtept')])  # tuple of all waypoint objects
        for p in tree.find_all('rtept'): all_nodes.append(RouteNode(p))
        all_edges = [RouteEdge(node, node.next()) for node in all_nodes[:-1]]
        route_nodes = tuple([rp for rp in all_nodes if rp.url()])  # tuple of current station waypoint objects
        self.__first = route_nodes[0]
        self.__last = route_nodes[-1]

        # calculate distances
        current_edge = None
        for i, edge in enumerate(distances):
            if edge[0]: current_edge = edge
            else: current_edge[1] += edge[1]
        station_distances = [edge[1] for edge in distances if edge[0]]

        # update prev/next links to current station waypoints to create linked list
        for i, pt in enumerate(route_nodes[:-1]): pt.next(route_nodes[i+1])
        reverse = route_nodes[::-1]
        for i, pt in enumerate(reverse[:-1]): pt.prev(reverse[i+1])

        # add distances to prev and next to waypoint
        for i, pt in enumerate(route_nodes[:-1]): pt.dist_to_next(station_distances[i])
        pt = self.__last
        while pt.prev():
            pt.dist_to_prev(pt.prev().dist_to_next())
            pt = pt.prev()

        # calculate route length and direction
        for d in station_distances: self.__length += d
        corner = (self.__last.coords()[0], self.__first.coords()[1])
        lat_sign = sign(self.__last.coords()[1]-self.__first.coords()[1])
        lon_sign = sign(self.__last.coords()[0]-self.__first.coords()[0])
        lat_dist = hvs(corner, self.__first.coords(), unit=Unit.NAUTICAL_MILES)
        lon_dist = hvs(self.__last.coords(), corner, unit=Unit.NAUTICAL_MILES)
        if (lat_sign > 0 and lon_sign > 0 and not lon_dist >= lat_dist) or (lat_sign < 0 < lon_sign and not lon_dist >= lat_dist): self.__direction = 'SN'
        elif (lat_sign > 0 > lon_sign and not lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and not lon_dist >= lat_dist): self.__direction = 'NS'
        elif (lat_sign < 0 < lon_sign and lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and lon_dist >= lat_dist): self.__direction = 'EW'
        elif (lat_sign > 0 and lon_sign > 0 and lon_dist >= lat_dist) or (lat_sign > 0 > lon_sign and lon_dist >= lat_dist): self.__direction = 'WE'
