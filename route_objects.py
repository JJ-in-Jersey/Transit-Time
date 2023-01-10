from bs4 import BeautifulSoup as Soup
from haversine import haversine as hvs, Unit
from project_globals import sign, TIMESTEP

class Node:

    def next_edge(self, edge=None):
        self.__next_edge = edge if edge and not self.__next_edge else self.__next_edge  # can be set only once
        return self.__next_edge
    def coords(self): return self.__coords
    def name(self): return self.__name
    def code(self): return self.__code

    def __init__(self, gpxtag):
        self.__gpxtag = gpxtag
        self.__name = gpxtag.find('name').text
        self.__coords = (round(float(gpxtag.attrs['lat']), 4), round(float(gpxtag.attrs['lon']), 4))
        self.__code = gpxtag.link.attrs['href'].split('=')[1].split('_')[0] if gpxtag.link else '0000'
        self.__next_edge = None

    # def __del__(self):
    #     print(f'Deleting Node', flush=True)

class RouteNode(Node):

    def next_route_edge(self, edge=None):
        self.__next_route_edge = edge if edge and not self.__next_route_edge else self.__next_route_edge  # can be set only once
        return self.__next_route_edge
    def url(self): return self.__url
    def velocities(self, velo=None):
        self.__velocities = velo if velo is not None and self.__velocities is None else self.__velocities  # can be set only onece
        return self.__velocities
    def velocity_path(self): return self.__velo_path
    def download_table_path(self): return self.__download_table_path
    def download_folder(self): return self.__download_folder
    def start_index(self): return self.__start_index
    def end_index(self): return self.__end_index

    def __init__(self, gpxtag, env, chart_yr):
        super().__init__(gpxtag)
        self.__next_route_edge = None
        self.__url = gpxtag.link.attrs['href']
        self.__velocities = None
        self.__velo_path = env.velocity_folder().joinpath(self.code()+'_table')
        self.__download_table_path = env.create_node_folder(self.code()).joinpath(self.code()+'_download_table')
        self.__download_folder = env.create_node_folder(self.code())
        self.__next_route_edge = None

class Edge:

    def start(self): return self.__start
    def end(self): return self.__end
    def name(self): return self.__name
    def length(self): return self.__length

    def __init__(self, start, end):
        self.__start = start
        self.__end = end
        self.__name = start.code()+'-'+end.code()
        self.__length = hvs(start.coords(), end.coords(), unit=Unit.NAUTICAL_MILES)
        start.next_edge(self)

    # def __del__(self):
    #     print(f'Deleting Edge', flush=True)

class RouteSegment:

    def elapsed_time_df(self, df=None):
        if self.__elapsed_time_df is None and df is not None: self.__elapsed_time_df = df
        return self.__elapsed_time_df

    def start(self): return self.__start
    def end(self): return self.__end
    def name(self): return self.__name
    def length(self): return self.__length
    def edge_folder(self): return self.__edge_folder
    def elapsed_time_table_path(self): return self.__elapsed_time_table_path

    @staticmethod
    def calc_length(start, end):
        edge_lengths = 0
        node = start
        while node != end:
            edge_lengths += node.next_edge().length()
            node = node.next_edge().end()
        return edge_lengths

    def __init__(self, start, end, env):
        self.__start = start
        self.__end = end
        self.__name = start.code()+'-'+end.code()
        self.__length = self.calc_length(start, end)
        self.__edge_folder = env.create_edge_folder(self.__name)
        self.__elapsed_time_df = None
        self.__elapsed_time_table_path = env.elapsed_time_folder().joinpath(self.__name+'_table')

    # def __del__(self):
    #     print(f'Deleting Route Segment', flush=True)

class GpxRoute:

    directionLookup = {'SN': 'South to North', 'NS': 'North to South', 'EW': 'East to West', 'WE': 'West to East'}

    def route_nodes(self): return self.__route_nodes
    def route_segments(self): return self.__route_segments
    def length(self): return self.__length
    def direction(self): return GpxRoute.directionLookup[self.__direction]
    def transit_time_lookup(self, key, array=None):
        if key not in self.transit_time_dict and array is not None:
            self.transit_time_dict[key] = array
        else:
            return self.transit_time_dict[key]
    def elapsed_time_lookup(self, key, array=None):
        if key not in self.elapsed_time_dict and array is not None:
            self.elapsed_time_dict[key] = array
        else:
            return self.elapsed_time_dict[key]

    def __init__(self, filepath, env, chart_yr):
        self.transit_time_dict = {}
        self.elapsed_time_dict = {}
        self.__route_nodes = self.__route_edges = self.__elapsed_times = None
        self.__direction = self.__length = 0

        with open(filepath, 'r') as f: gpxfile = f.read()
        tree = Soup(gpxfile, 'xml')

        # create graph nodes
        nodes = [RouteNode(waypoint, env, chart_yr) if waypoint.desc else Node(waypoint) for waypoint in tree.find_all('rtept')]
        self.__route_nodes = [node for node in nodes if isinstance(node, RouteNode)]

        # create graph edges and segments
        for i, node in enumerate(nodes[:-1]): Edge(node, nodes[i+1])
        self.__route_segments = [RouteSegment(node, self.__route_nodes[i+1], env) for i, node in enumerate(self.__route_nodes[:-1])]

        # calculate route length
        self.__length = sum([node.next_edge().length() for node in nodes[:-1]])

        # calculate direction
        corner = (nodes[-1].coords()[0], nodes[0].coords()[1])
        lat_sign = sign(nodes[-1].coords()[1]-nodes[0].coords()[1])
        lon_sign = sign(nodes[-1].coords()[0]-nodes[0].coords()[0])
        lat_dist = hvs(corner, nodes[0].coords(), unit=Unit.NAUTICAL_MILES)
        lon_dist = hvs(nodes[-1].coords(), corner, unit=Unit.NAUTICAL_MILES)
        if (lat_sign > 0 and lon_sign > 0 and not lon_dist >= lat_dist) or (lat_sign < 0 < lon_sign and not lon_dist >= lat_dist): self.__direction = 'SN'
        elif (lat_sign > 0 > lon_sign and not lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and not lon_dist >= lat_dist): self.__direction = 'NS'
        elif (lat_sign < 0 < lon_sign and lon_dist >= lat_dist) or (lat_sign < 0 and lon_sign < 0 and lon_dist >= lat_dist): self.__direction = 'EW'
        elif (lat_sign > 0 and lon_sign > 0 and lon_dist >= lat_dist) or (lat_sign > 0 > lon_sign and lon_dist >= lat_dist): self.__direction = 'WE'

