import geopandas as gpd
import numpy as np
from shapely.wkt import loads
from shapely.geometry import MultiPoint, Point
from sklearn.cluster import DBSCAN

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


def find_stop_clusters(stops: list[Stop], cluster_distance_metres: int = 50, max_diameter_meters: int = 250) -> list[list[str]]:
    # Step 1: Initial clustering
    stops_geoframe = _cluster_stops(stops, cluster_distance_metres)

    # Step 2: Enforce diameter constraint
    stops_geoframe = _enforce_diameter_constraint(stops_geoframe, cluster_distance_metres, max_diameter_meters)

    # Step 3: Resolve overlaps
    stops_geoframe = _resolve_overlaps(stops_geoframe)

    # Group stop IDs by cluster
    clusters = stops_geoframe.groupby("cluster")["stop_id"].agg(list).tolist()
    return clusters

def _cluster_stops(stops: list[Stop], tolerance_metres: int, crs="EPSG:4326"):
    if tolerance_metres < 0:
        raise ValueError("The cluster tolerance must be positive!")

    stop_geoframe = gpd.GeoDataFrame(
        {"stop_id": [s.id for s in stops]},
        geometry=[Point(s.lon, s.lat) for s in stops],  # Point(x=lon, y=lat)
        crs=crs
    ).to_crs("EPSG:3857") # convert to 'Web Mercator' projection where coordinates are in metres

    coords = np.array([(geom.x, geom.y) for geom in stop_geoframe.geometry])
    cluster_assignment = DBSCAN(eps=tolerance_metres, min_samples=2, algorithm="ball_tree", metric="euclidean").fit(coords)

    # Assign cluster labels back to the GeoDataFrame
    stop_geoframe["cluster"] = cluster_assignment.labels_
    return stop_geoframe

def _enforce_diameter_constraint(gdf_stops: gpd.GeoDataFrame, cluster_distance: int, max_diameter_meters: int) -> gpd.GeoDataFrame:
    # Drop noise clusters and then group stops by cluster
    clusters = gdf_stops[gdf_stops["cluster"] != -1].groupby("cluster")

    for cluster_id, cluster_group in clusters:
        diameter = _approximate_diameter(cluster_group.geometry.tolist())
        max_distance = cluster_distance

        # If the diameter exceeds the limit, split the cluster iteratively
        while diameter > max_diameter_meters:
            max_distance = max_distance / 2

            # Split the cluster into smaller sub-clusters using DBSCAN with a smaller eps
            coords = np.array([(geom.x, geom.y) for geom in cluster_group.geometry])
            new_cluster_assignment = DBSCAN(eps=max_distance, min_samples=2, algorithm="ball_tree", metric="euclidean").fit(coords)

            # Update the cluster_group with new sub-cluster labels
            cluster_group = cluster_group.copy()
            for idx, label in zip(cluster_group.index, new_cluster_assignment.labels_):
                if label != -1:
                    cluster_group.at[idx, "cluster"] = cluster_id * 1000 + label
                else:
                    cluster_group.at[idx, "cluster"] = -1

            # Find the largest diameter among all sub-clusters
            sub_clusters = cluster_group[cluster_group["cluster"] != -1].groupby("cluster")
            diameter = 0
            for sub_cluster_id, sub_cluster_group in sub_clusters:
                sub_diameter = _approximate_diameter(sub_cluster_group.geometry.tolist())
                if sub_diameter > diameter:
                    diameter = sub_diameter

        # Assign final sub-cluster labels back to gdf_stops, excluding noise
        valid_sub_clusters = cluster_group[cluster_group["cluster"] != -1]
        gdf_stops.loc[valid_sub_clusters.index, "cluster"] = valid_sub_clusters["cluster"]

    return gdf_stops

def _resolve_overlaps(gdf_stops: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Group stops by their final cluster labels
    cluster_sizes = gdf_stops["cluster"].value_counts()

    # For each stop, keep only the largest cluster it belongs to
    gdf_stops["cluster_size"] = gdf_stops["cluster"].map(cluster_sizes)
    gdf_stops = gdf_stops.sort_values("cluster_size", ascending=False).drop_duplicates("stop_id", keep="first")

    # Since we may have produced some orphans, remove clusters with only one stop
    cluster_counts = gdf_stops["cluster"].value_counts()
    valid_clusters = cluster_counts[cluster_counts >= 2].index
    gdf_stops = gdf_stops[gdf_stops["cluster"].isin(valid_clusters)]

    return gdf_stops[gdf_stops["cluster"] != -1]

def _approximate_diameter(points: list[Point]) -> float:
    """
    Approximates the diameter of the point cloud by calculating the diagonal of its bounding box
    """

    point_cloud = MultiPoint(points)
    bounds = point_cloud.bounds  # (minx, miny, maxx, maxy)
    # Calculate diameter of bounding box using the Pythagorean theorem
    return ((bounds[2] - bounds[0]) ** 2 + (bounds[3] - bounds[1]) ** 2) ** 0.5
