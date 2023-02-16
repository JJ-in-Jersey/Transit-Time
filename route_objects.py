from bs4 import BeautifulSoup as Soup
from haversine import haversine as hvs, Unit
from Navigation import Navigation as nav

class Waypoint:
    # get everything that is available from the tag
    #
    # arguments
    #   gpxtag
    # data members
    #   num, name, lat, lon, symbol, code, url, prev_edges, next_edges
    # methods
    #   coords, prev_edges, next_edge

    type = {'CurrentStationWP': 'Symbol-Spot-Orange', 'PlaceHolderWP': 'Symbol-Spot-Green', 'InterpolationWP': 'Symbol-Spot-Blue'}
    ordinal_number = 0
    number_lookup = {}

    def coords(self): return tuple((self._lat, self._lon))
    def prev_edge(self, path, edge=None):
        if edge: self._prev_edges[path] = edge
        else: return self._prev_edges[path]
    def next_edge(self, path, edge=None):
        if edge: self._next_edges[path] = edge
        else: return self._next_edges[path]
    def has_velocity(self): return False
    def velo_array(self, v_array=None):
        if v_array is not None: self._velo_array = v_array
        else: return self._velo_array

    def __init__(self, gpxtag):
        self._number = Waypoint.ordinal_number
        self._lat = round(float(gpxtag.attrs['lat']), 4)
        self._lon = round(float(gpxtag.attrs['lon']), 4)
        self._symbol = gpxtag.sym.text
        self._noaa_name = gpxtag.find('name').text
        self._noaa_url = gpxtag.link.attrs['href'] if gpxtag.link else None
        self._noaa_code = gpxtag.link.text.strip('\n') if gpxtag.link else None
        self._name = str(self._number) + ' ' + self._noaa_name.split(',')[0].split('(')[0].replace('.', '').strip()
        self._prev_edges = {}
        self._next_edges = {}
        self._velo_array = None

        Waypoint.number_lookup[Waypoint.ordinal_number] = self
        Waypoint.ordinal_number += 1

class PlaceHolderWP(Waypoint):

    def __init__(self, gpxtag):
        super().__init__(gpxtag)

class InterpolationWP(Waypoint):

    def has_velocity(self): return True

    def __init__(self, gpxtag):
        super().__init__(gpxtag)

class CurrentStationWP(Waypoint):

    def has_velocity(self): return True

    def __velo_arr(self, velo=None):
        self.__velo_arr = velo if velo is not None and self.__velo_arr is None else self.__velo_arr
        return self.__velo_arr

    # def velo_path(self): return self.__velo_path
    # def chrome_download_path(self): return self.__chrome_download_path
    # def start_index(self): return self.__calc_start_index
    # def end_index(self): return self.__calc_end_index

    def __init__(self, gpxtag):
        super().__init__(gpxtag)
        self.__velo_arr = None


# noinspection PyProtectedMember
class Edge:
    # arguments
    #   path, start, end
    # data members
    #   start waypoint, end waypoint
    # methods
    #   name, length

    def name(self): return '[' + str(self._start._number) + '-' + str(self._end._number) + ']'
    def length(self): return round(hvs(self._start.coords(), self._end.coords(), unit=Unit.NAUTICAL_MILES), 4)

    def __init__(self, path, start, end):
        self._path = path
        self._start = start
        self._end = end
        start.next_edge(path, self)
        end.prev_edge(path, self)

class ElapsedTimeSegment:
    # arguments
    #   path, start, end
    # data members
    #   start velocity, end velocity
    # methods
    #   elapsed_times_df

    def elapsed_times_df(self, elapsed_times_df=None):
        if elapsed_times_df is not None: self._elapsed_times_df = elapsed_times_df
        else: return self._elapsed_times_df

    def update(self):
        self._start_velo = self.__start.velo_array()
        self._end_velo = self.__end.velo_array()

    # noinspection PyProtectedMember
    def __init__(self, path, start, end):
        self.__start = start
        self.__end = end
        self._path = path
        self._name = 'segment ' + str(start._number) + '-' + str(end._number)
        self._start_velo = None
        self._end_velo = None
        self._length = path.length(start, end)
        self._elapsed_times_df = None


# noinspection PyProtectedMember
class Path:
    # data members:
    #   waypoints
    # methods
    #   name, length

    def name(self): return '{' + str(self._waypoints[0]._number) + '-' + str(self._waypoints[-1]._number) + '}'
    def total_length(self): return round(sum([edge.length() for edge in self._edges]), 4)
    def direction(self): return nav.direction(self._waypoints[0].coords(), self._waypoints[-1].coords())
    def edges(self): return self._edges

    def __init__(self, waypoints):
        self._waypoints = waypoints
        self._edges = []
        for i, waypoint in enumerate(self._waypoints[:-1]):
            self._edges.append(Edge(self, waypoint, self._waypoints[i+1]))

    def print_path(self, direction=None):
        print(self.name(), self.total_length(), self.direction())
        if direction == -1:
            for waypoint in reversed(self._waypoints):
                print(f'({waypoint._number} {type(waypoint).__name__})', end='')
                print(f' {waypoint.prev_edge(self).name()} {waypoint.prev_edge(self).length()} ', end='')
        else:
            for waypoint in self._waypoints:
                print(f'({waypoint._number} {type(waypoint).__name__})', end='')
                print(f' {waypoint.next_edge(self).name()} {waypoint.next_edge(self).length()} ', end='')

    def length(self, start_wp, end_wp):
        length = 0
        if start_wp == end_wp: return length
        wp_range = range(start_wp._number, end_wp._number) if start_wp._number < end_wp._number else range(end_wp._number, start_wp._number)
        for i in wp_range:
            length += Waypoint.number_lookup[i].next_edge(self).length()
        return length

class Route:

    def transit_time_lookup(self, key, array=None):
        if key not in self._transit_time_dict and array is not None:
            self._transit_time_dict[key] = array
        else:
            return self._transit_time_dict[key]
    def elapsed_time_lookup(self, key, array=None):
        if key not in self._elapsed_time_dict and array is not None:
            self._elapsed_time_dict[key] = array
        else:
            return self._elapsed_time_dict[key]

    def __init__(self, filepath):
        self._transit_time_dict = {}
        self._elapsed_time_dict = {}
        self._waypoints = []
        self._path = None
        self._elapsed_time_segments = []
        self._velocity_waypoints = []

        with open(filepath, 'r') as f: gpxfile = f.read()
        tree = Soup(gpxfile, 'xml')

        # build ordered list of all waypoints
        for waypoint in tree.find_all('rtept'):
            if waypoint.sym.text == Waypoint.type['CurrentStationWP']: self._waypoints.append(CurrentStationWP(waypoint))
            elif waypoint.sym.text == Waypoint.type['PlaceHolderWP']: self._waypoints.append(PlaceHolderWP(waypoint))
            elif waypoint.sym.text == Waypoint.type['InterpolationWP']: self._waypoints.append(InterpolationWP(waypoint))

        self._path = Path(self._waypoints)
        # base_path.print_path()

        # noinspection SpellCheckingInspection
        vwps = list(filter(lambda wp: wp.has_velocity(), self._waypoints))
        self._elapsed_time_segments = [ElapsedTimeSegment(self._path, wp, vwps[i+1]) for i, wp in enumerate(vwps[:-1])]
        self._velocity_waypoints = vwps
