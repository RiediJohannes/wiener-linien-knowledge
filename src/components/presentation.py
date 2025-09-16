from enum import Flag, auto

import folium
from shapely import MultiPoint

from src.components.types import Stop, ClusterStop, Connection, ModeOfTransport, Frequency


# noinspection PyTypeChecker
class VisibleLayers(Flag):
    STOPS = auto()
    CLUSTERS = auto()
    CONNECTIONS = auto()
    ALL = STOPS | CLUSTERS | CONNECTIONS

class TransportMap:
    connection_colours: dict[ModeOfTransport, str] = {
        ModeOfTransport.BUS: "#1c2185",
        ModeOfTransport.TRAM: "#b81818",
        ModeOfTransport.SUBWAY: "#e88f00",
        ModeOfTransport.ANY: "#7a7671",
    }

    frequency_colours: dict[Frequency, str] = {
        Frequency.NONSTOP_TO: "#047a04",
        Frequency.VERY_FREQUENTLY_TO: "#59ad10",
        Frequency.FREQUENTLY_TO: "#c8d124",
        Frequency.REGULARLY_TO: "#bd8408",
        Frequency.OCCASIONALLY_TO: "#a83605",
        Frequency.RARELY_TO: "#8c0404",
        Frequency.UNKNOWN: "#7a7671",
    }

    # noinspection PyTypeChecker
    def __init__(self, lat: float, lon: float, zoom: int, *,
                 name: str = None, custom_tile_source: str = None, custom_attribution: str = None,
                 visible_layers: VisibleLayers = VisibleLayers.STOPS | VisibleLayers.CLUSTERS):
        # Create a folium map centered on the mean of the coordinates
        self.base = folium.Map(
            tiles=None,
            location=[lat, lon],
            zoom_start=zoom,
        )

        # Add map as base layer
        if custom_tile_source:
            folium.TileLayer(custom_tile_source, attr=custom_attribution,
                         name=name if name else "Basemap", overlay=False).add_to(self.base)
        else:
            # Default base map provider
            folium.TileLayer("https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",
                         attr='&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a> &copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                         name="Stadiamaps", overlay=False).add_to(self.base)

        # Add layers to show/hide markers
        stops_visible = VisibleLayers.STOPS in visible_layers
        self.stop_marks = folium.FeatureGroup(name="Stop markers", control=stops_visible, show=stops_visible).add_to(self.base)
        clusters_visible = VisibleLayers.CLUSTERS in visible_layers
        self.cluster_marks = folium.FeatureGroup(name="Cluster markers", control=clusters_visible, show=clusters_visible).add_to(self.base)
        connections_visible = VisibleLayers.CONNECTIONS in visible_layers
        self.connections = folium.FeatureGroup(name="Transit connections", control=connections_visible, show=connections_visible).add_to(self.base)

        folium.LayerControl().add_to(self.base)
        # Keep stop markers in front so they remain clickable
        self.base.keep_in_front(self.stop_marks)

        # Create panes to put different markers on different z-indexes
        folium.map.CustomPane("clusters", z_index=600).add_to(self.base)
        folium.map.CustomPane("stops", z_index=800).add_to(self.base)
        folium.map.CustomPane("connections", z_index=700).add_to(self.base)


    def add_stops(self, stops: list[Stop]) -> None:
        # Add markers for each stop
        for stop in stops:
            # Add a small circle for a stop
            folium.CircleMarker(
                location=[stop.lat, stop.lon],
                radius=2,
                color="red",
                fill=True,
                fill_opacity=0.4,
                opacity=0.6,
                tooltip=stop.name,
                popup=stop.id,
                pane="stops"
            ).add_to(self.stop_marks)

            # Additionally, add a big translucent circle for a cluster
            if isinstance(stop, ClusterStop):
                # Compute convex hull
                cluster_hull = MultiPoint(stop.cluster_points).convex_hull
                # Grow the polygon by a very small buffer zone
                # This is especially important for two-point clusters (thick line)
                buffered_hull = cluster_hull.buffer(0.00004, cap_style="round", join_style="round")
                hull_points = [(lat, lon) for lat, lon in buffered_hull.exterior.coords]

                folium.Polygon(
                    locations=hull_points,
                    color="violet",
                    weight=1,
                    fill=True,
                    fill_opacity=0.35,
                    opacity=0.5,
                    pane="clusters",
                    interactive=False
                ).add_to(self.cluster_marks)

    def add_transit_nodes(self, nodes: list[Stop]) -> None:
        for node in nodes:
            folium.CircleMarker(
                location=[node.display_lat(), node.display_lon()],
                radius=1,
                color="#222222",
                opacity=0.55,
                fill=True,
                fill_opacity=0.55,
                tooltip=node.name,
                popup=node.id,
                pane="stops"
            ).add_to(self.stop_marks)

    def add_transit_connections(self, connections: list[Connection], include_nodes = False) -> None:
        for conn in connections:
            line_coords = [
                [conn.from_stop.display_lat(), conn.from_stop.display_lon()],
                [conn.to_stop.display_lat(), conn.to_stop.display_lon()]
            ]

            if conn.mode_of_transport != ModeOfTransport.ANY:
                colour = self.connection_colours[conn.mode_of_transport]
                thickness = {
                    ModeOfTransport.SUBWAY: 3,
                    ModeOfTransport.TRAM: 2,
                    ModeOfTransport.BUS: 1
                }.get(conn.mode_of_transport, 1)
                opacity = 0.7
            else:
                colour = self.frequency_colours[conn.frequency]
                thickness = {
                    Frequency.NONSTOP_TO: 3,
                    Frequency.VERY_FREQUENTLY_TO: 3,
                    Frequency.FREQUENTLY_TO: 2,
                    Frequency.REGULARLY_TO: 2
                }.get(conn.frequency, 1)
                opacity = 0.85

            # Add the line to the map
            folium.PolyLine(
                locations=line_coords,
                color=colour,
                weight=thickness,
                opacity=opacity,
                pane="connections"
            ).add_to(self.connections)

            if include_nodes:
                self.add_transit_nodes([conn.from_stop, conn.to_stop])

    def as_html(self) -> str:
        # Save the map to an HTML file
        # self.base.save("stops_map.html")

        return self.base._repr_html_()