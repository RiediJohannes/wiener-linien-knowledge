import folium
from src.components.graph import Stop, ClusterStop


class TransportMap:

    def __init__(self, lat: float, lon: float, zoom: int, *,
                 name: str = None, custom_tile_source: str = None, custom_attribution: str = None):
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
        self.stop_marks = folium.FeatureGroup(name="Stop markers", control=True, show=True).add_to(self.base)
        self.cluster_marks = folium.FeatureGroup(name="Cluster markers", control=True, show=False).add_to(self.base)
        folium.LayerControl().add_to(self.base)

        # Create panes to put different markers on different z-indexes
        folium.map.CustomPane("stops", z_index=600).add_to(self.base)
        folium.map.CustomPane("clusters", z_index=450).add_to(self.base)


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
                popup=stop.name,
                tooltip=stop.id,
                pane="stops"
            ).add_to(self.stop_marks)

            # Additionally, add a big translucent circle for a cluster
            if isinstance(stop, ClusterStop):
                folium.CircleMarker(
                    location=[stop.cluster_lat, stop.cluster_lon],
                    radius=15,
                    color="violet",
                    fill=True,
                    fill_opacity=0.2,
                    opacity=0.1,
                    pane="clusters"
                ).add_to(self.cluster_marks)

    def as_html(self) -> str:
        # Save the map to an HTML file
        # self.base.save("stops_map.html")

        return self.base._repr_html_()