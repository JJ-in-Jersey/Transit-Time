from warnings import filterwarnings as fw
from bs4 import BeautifulSoup as Soup
from haversine import haversine as hvs, Unit
import numpy

from project_globals import sign
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
        if isinstance(array, numpy.ndarray) and not self.__velo_array:
            self.__velo_array = array
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

    @staticmethod
    def calc_length(start, end): return hvs(start.coords(), end.coords(), unit=Unit.NAUTICAL_MILES)

    def __init__(self, start, end):
        super().__init__()
        self.__start = start
        self.__end = end
        self.__length = self.calc_length(start, end)
        start.next_edge(self)
        end.prev_edge(self)
        start.next_node(end)
        end.prev_node(start)

class RouteEdge:

    def length(self): return self.__length
    def start(self): return self.__start
    def end(self): return self.__end

    @staticmethod
    def calc_length(start, end):
        edges = []
        node = start
        while node != end:
            edges.append(node.next_edge().length())
            node = node.next_node()
        return sum(edges)

    def __init__(self, start, end):
        super().__init__()
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
    def first_node(self): return self.__first_node
    def last_node(self): return self.__last_node
    def first_route_node(self): return self.__first_route_node
    def last_route_node(self): return self.__last_route_node
    def length(self): return self.__length
    def direction(self): return GpxRoute.directionLookup[self.__direction]
    def edges(self): return self.__edges
    def route_edges(self): return self.__route_edges

    def __init__(self, filepath):
        super().__init__()
        self.__nodes = self.__route_nodes = self.__edges = self.__route_edges = None
        self.__first_node = self.__last_node = self.__first_route_node = self.__last_route_node = None
        self.__first_edge = self.__last_edge = self.__first_route_edge = self.__last_route_edge = None
        self.__direction = None
        self.__length = 0

        with open(filepath, 'r') as f:
            gpxfile = f.read()
        tree = Soup(gpxfile, 'xml')

        # create graph nodes
        self.__nodes = [RouteNode(point) if point.desc else Node(point) for point in tree.find_all('rtept')]
        self.__route_nodes = [node for node in self.__nodes if isinstance(node, RouteNode)]
        self.__first_node = self.__nodes[0]
        self.__last_node = self.__nodes[-1]
        self.__first_route_node = self.__route_nodes[0]
        self.__last_route_node = self.__route_nodes[-1]
        # create graph edges - instantiating edges updates next and prev references nodes
        for i, node in enumerate(self.__nodes[:-1]): Edge(node, self.__nodes[i+1])
        for i, node in enumerate(self.__route_nodes[:-1]): RouteEdge(node, self.__route_nodes[i+1])
        self.__edges = [node.next_edge() for node in self.__nodes[:-1]]
        self.__route_edges = [node.next_route_edge() for node in self.__route_nodes[:-1]]
        self.__first_edge = self.__edges[0]
        self.__last_edge = self.__edges[-1]
        self.__first_route_edge = self.__route_edges[0]
        self.__last_route_edge = self.__route_edges[-1]
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
