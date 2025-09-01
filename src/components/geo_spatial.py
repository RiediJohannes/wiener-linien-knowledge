import geopandas as gpd
from shapely.wkt import loads
from shapely.geometry import Point

from src.components.graph import SubDistrict, Stop


def match_stops_to_subdistricts(stops: list[Stop], subdistricts: list[SubDistrict], crs="EPSG:4326"):
    """
    Match Stop objects (points) to SubDistrict objects (polygons).

    Args:
        stops (list[Stop]): list of Stop objects
        subdistricts (list[SubDistrict]): list of SubDistrict objects
        crs (str): coordinate reference system (default EPSG:4326)

    Returns:
        GeoDataFrame: each stop with its matched subdistrict ID (if any)
    """
    return []
