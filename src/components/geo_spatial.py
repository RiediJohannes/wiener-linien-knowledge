from argparse import ArgumentError

import geopandas as gpd
from shapely.wkt import loads
from shapely.geometry import Point

from src.components.graph import SubDistrict, Stop


def match_stops_to_subdistricts(stops: list[Stop], subdistricts: list[SubDistrict], buffer_metres: int = 0, crs="EPSG:4326"):
    """
    Match each Stops (points) to all SubDistricts (polygons) that either surround the stop or are within buffer_metres
    of the point. The function drops stops that have no matching subdistricts.

    Args:
        stops (list[Stop]): list of Stop objects
        subdistricts (list[SubDistrict]): list of SubDistrict objects
        buffer_metres (int): The tolerance distance that a stop might be outside a district and still be counted as within it
        crs (str): coordinate reference system (default EPSG:4326)

    Returns:
        GeoDataFrame: each stop with its matched subdistrict ID.
        Stops that don't correspond to any subdistrict are dropped.
    """

    if buffer_metres < 0:
        raise ValueError("The buffer distance must be positive!")

    subdistricts_geoframe = gpd.GeoDataFrame(
        {"subdistrict_id": [sd.id for sd in subdistricts]},
        geometry=[loads(sd.shape) for sd in subdistricts],
        crs=crs
    ).to_crs("EPSG:3857") # convert to 'Web Mercator' projection where coordinates are in metres

    stop_geoframe = gpd.GeoDataFrame(
        {"stop_id": [s.id for s in stops]},
        geometry=[Point(s.lon, s.lat) for s in stops],  # Point(x=lon, y=lat)
        crs=crs
    ).to_crs("EPSG:3857") # convert to 'Web Mercator' projection where coordinates are in metres

    # Spatial join: Match each stop with all subdistricts that either surround the stop or are within buffer_metres.
    # Beware: inner join -> drops points that are within no district
    matches = gpd.sjoin(stop_geoframe, subdistricts_geoframe, how="inner", predicate="dwithin", distance=buffer_metres)

    # Group into (stop_id, [list of subdistricts])
    grouped = matches.groupby("stop_id", sort=False)["subdistrict_id"].agg(list)
    return grouped.items()
