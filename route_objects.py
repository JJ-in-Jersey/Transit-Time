from bs4 import BeautifulSoup as Soup
from Navigation import Navigation as nav

class Waypoint:
    # get everything that is available from the tag

    type = {'CurrentStationWP': 'Symbol-Spot-Orange', 'PlaceHolderWP': 'Symbol-Spot-Green', 'InterpolationWP': 'Symbol-Spot-Blue'}
    ordinal_number = 0
    number_lookup = {}

    def prev_edge(self, path, edge=None):
        if edge is not None: self.__prev_edges[path] = edge
        else: return self.__prev_edges[path]
    def next_edge(self, path, edge=None):
        if edge is not None: self.__next_edges[path] = edge
        else: return self.__next_edges[path]
    def has_velocity(self): return False

    def __init__(self, gpxtag):
        lat = round(float(gpxtag.attrs['lat']), 4)
        lon = round(float(gpxtag.attrs['lon']), 4)
        self.number = Waypoint.ordinal_number
        self.coords = tuple([lat, lon])
        self.symbol = gpxtag.sym.text
        self.name = gpxtag.find('name').text.strip('\n')
        self.short_name = self.name.split(',')[0].split('(')[0].replace('.', '').strip()
        self.__prev_edges = {}
        self.__next_edges = {}
        self.velo_array = None

        Waypoint.number_lookup[Waypoint.ordinal_number] = self
        Waypoint.ordinal_number += 1

class PlaceHolderWP(Waypoint):

    def __init__(self, gpxtag):
        super().__init__(gpxtag)


class InterpolationPoint():

    def __init__(self, link):
        self.code = link.find('text').text
        self.url = link.attrs['href']
        self.coords = tuple([float(string) for string in link.find('type').contents[0].split()])
        self.distance_from_waypoint = 0
        self.velo_arr = None

class InterpolationWP(Waypoint):

    def has_velocity(self): return True

    def __init__(self, gpxtag):
        super().__init__(gpxtag)
        self.interpolation_points = [InterpolationPoint(link) for link in gpxtag.find_all('link')]
        self.velo_arr = None

class CurrentStationWP(Waypoint):

    def has_velocity(self): return True

    def __init__(self, gpxtag):
        super().__init__(gpxtag)
        self.noaa_url = gpxtag.find('link').attrs['href'] if gpxtag.link else None
        self.noaa_code = gpxtag.find('link').find('text').text
        self.velo_arr = None

class Edge:

    def name(self): return '[' + str(self.start.number) + '-' + str(self.end.number) + ']'
    def length(self): return round(nav.distance(self.start.coords, self.end.coords), 4)

    def __init__(self, path, start, end):
        self.path = path
        self.start = start
        self.end = end
        start.next_edge(path, self)
        end.prev_edge(path, self)

class ElapsedTimeSegment:

    def update(self):
        self.start_velo = self.start.velo_array()
        self.end_velo = self.end.velo_array()

    def __init__(self, path, start, end):
        self.start = start
        self.end = end
        self.path = path
        self.name = 'segment ' + str(start.number) + '-' + str(end.number)
        self.start_velo = None
        self.end_velo = None
        self.length = path.length(start, end)
        self.elapsed_times_df = None


class Path:

    def name(self): return '{' + str(self.waypoints[0].number) + '-' + str(self.waypoints[-1].number) + '}'
    def total_length(self): return round(sum([edge.length() for edge in self.edges]), 4)
    def direction(self): return nav.direction(self.waypoints[0].coords, self.waypoints[-1].coords)
    def edges(self): return self.edges

    def __init__(self, waypoints):
        self.waypoints = waypoints
        self.edges = []
        for i, waypoint in enumerate(self.waypoints[:-1]):
            self.edges.append(Edge(self, waypoint, self.waypoints[i+1]))

    def print_path(self, direction=None):
        print(self.name(), self.total_length(), self.direction())
        if direction == -1:
            for waypoint in reversed(self.waypoints):
                print(f'({waypoint.number} {type(waypoint).__name__})', end='')
                print(f' {waypoint.prev_edge(self).name()} {waypoint.prev_edge(self).length()} ', end='')
        else:
            for waypoint in self.waypoints:
                print(f'({waypoint.number} {type(waypoint).__name__})', end='')
                print(f' {waypoint.next_edge(self).name()} {waypoint.next_edge(self).length()} ', end='')

    def length(self, start_wp, end_wp):
        length = 0
        if start_wp == end_wp: return length
        wp_range = range(start_wp.number, end_wp.number) if start_wp.number < end_wp.number else range(end_wp.number, start_wp.number)
        for i in wp_range:
            length += Waypoint.number_lookup[i].next_edge(self).length()
        return length

class Route:

    def transit_time_lookup(self, key, array=None):
        if key not in self.__transit_time_dict and array is not None:
            self.__transit_time_dict[key] = array
        else:
            return self.__transit_time_dict[key]
    def elapsed_time_lookup(self, key, array=None):
        if key not in self.__elapsed_time_dict and array is not None:
            self.__elapsed_time_dict[key] = array
        else:
            return self.__elapsed_time_dict[key]

    def __init__(self, filepath):
        self.__transit_time_dict = {}
        self.__elapsed_time_dict = {}
        self.waypoints = []
        self.path = None
        self.elapsed_time_segments = []
        self.velocity_waypoints = []

        with open(filepath, 'r') as f: gpxfile = f.read()
        tree = Soup(gpxfile, 'xml')

        # build ordered list of all waypoints
        for waypoint in tree.find_all('rtept'):
            if waypoint.sym.text == Waypoint.type['CurrentStationWP']: self.waypoints.append(CurrentStationWP(waypoint))
            elif waypoint.sym.text == Waypoint.type['PlaceHolderWP']: self.waypoints.append(PlaceHolderWP(waypoint))
            elif waypoint.sym.text == Waypoint.type['InterpolationWP']: self.waypoints.append(InterpolationWP(waypoint))

        self._path = Path(self.waypoints)
        # base_path.print_path()

        vwps = list(filter(lambda wp: wp.has_velocity(), self.waypoints))
        self._elapsed_time_segments = [ElapsedTimeSegment(self._path, wp, vwps[i+1]) for i, wp in enumerate(vwps[:-1])]
        self._velocity_waypoints = vwps
