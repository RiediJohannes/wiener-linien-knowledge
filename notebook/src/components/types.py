from enum import Enum


class Stop:
    def __init__(self, stop_id: str, latitude: float, longitude: float, name: str):
        self.id: str = stop_id
        self.lat: float = latitude
        self.lon: float = longitude
        self.name: str = name
        self.is_root = False

    def display_lon(self) -> float:
        return self.lon
    def display_lat(self) -> float:
        return self.lat

class ClusterStop(Stop):
    def __init__(self, stop_id: str, latitude: float, longitude: float, name: str, cluster_lat: float, cluster_lon: float, cluster_points: list[list[float]]):
       super().__init__(stop_id, latitude, longitude, name)
       self.is_root = True
       self.cluster_lat: float = cluster_lat
       self.cluster_lon: float = cluster_lon
       if cluster_points:
           self.cluster_points: list[tuple[float, float]] = [(point[0], point[1]) for point in cluster_points]
       else:
           self.cluster_points = []

    def display_lon(self) -> float:
        return self.cluster_lon
    def display_lat(self) -> float:
        return self.cluster_lat

class SubDistrict:
    def __init__(self, district_num: int, subdistrict_num: int, name: str, population: int, area: float, shape: str):
        self.id: str = f"{district_num}-{subdistrict_num}"
        self.name: str = name
        self.population: int = population
        self.area: float = area
        self.shape: str = shape


class ComparableEnum(Enum):
    def __eq__(self, other):
        if hasattr(other, "name") and hasattr(other, "value"):
            return self.name == other.name and self.value == other.value
        return False

    def __hash__(self):
        return 3 * hash(self.name) + 7 * hash(self.value)

class ModeOfTransport(ComparableEnum):
    BUS = 1
    TRAM = 2
    SUBWAY = 3
    ANY = 0

class Frequency(ComparableEnum):
    NONSTOP_TO = 1
    VERY_FREQUENTLY_TO = 2
    FREQUENTLY_TO = 3
    REGULARLY_TO = 4
    OCCASIONALLY_TO = 5
    RARELY_TO = 6
    UNKNOWN = 0


def parse_mode_of_transport(connection_label: str) -> ModeOfTransport:
    match connection_label:
        case "BUS_CONNECTS_TO": return ModeOfTransport.BUS
        case "TRAM_CONNECTS_TO": return ModeOfTransport.TRAM
        case "SUBWAY_CONNECTS_TO": return ModeOfTransport.SUBWAY
        case _: return ModeOfTransport.ANY

def parse_frequency(frequency_label: str) -> Frequency:
    if frequency_label in [member.name for member in Frequency]:
        return Frequency[frequency_label]
    else:
        return Frequency.UNKNOWN


class Connection:
    def __init__(self, from_stop: Stop, to_stop: Stop, mode_of_transport: ModeOfTransport = ModeOfTransport.ANY, frequency: Frequency = Frequency.UNKNOWN):
        self.from_stop: Stop = from_stop
        self.to_stop: Stop = to_stop
        self.mode_of_transport: ModeOfTransport = mode_of_transport
        self.frequency: Frequency = frequency

    def __str__(self):
        if self.mode_of_transport != ModeOfTransport.ANY:
            return f"{self.from_stop.name} --[{self.mode_of_transport}]--> {self.to_stop.name}"
        elif self.frequency != Frequency.UNKNOWN:
            return f"{self.from_stop.name} --[{self.frequency}]--> {self.to_stop.name}"
        else:
            return f"{self.from_stop.name} --[UNKNOWN_RELATION]--> {self.to_stop.name}"