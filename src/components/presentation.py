import html
import io
from contextlib import contextmanager, redirect_stdout
from enum import Flag, auto
from typing import Callable, Any

import folium
import marimo as mo
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

    def add_legend(self, title: str, entries: list[tuple[str, str]]):
        legend_html = self._create_color_map_legend(title, entries)
        self.base.get_root().html.add_child(folium.Element(legend_html))

    def as_html(self) -> str:
        # Save the map to an HTML file
        # self.base.save("stops_map.html")

        return self.base._repr_html_()

    @staticmethod
    def _create_color_map_legend(title: str, entries: list[tuple[str, str]]) -> str:
        legend_items = [f"<li><span style='background:{colour};opacity:0.85;'></span>{name}</li>"
                        for name, colour in entries]
        legend_items_html = '\n'.join(legend_items)

        return f'''
        <div id='maplegend' class='maplegend' 
            style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.9);
             border-radius:6px; padding: 10px; font-size:12px; font-family: Arial; right: 20px; bottom: 20px;'>

        <div class='legend-title'><b>{title}</b></div>
        <div class='legend-scale'>
          <ul class='legend-labels'>
            {legend_items_html}
          </ul>
        </div>
        </div>

        <style type='text/css'>
          .maplegend .legend-title {{
            text-align: left;
            margin-bottom: 5px;
          }}
          .maplegend .legend-scale ul {{
            margin: 0;
            margin-bottom: 5px;
            padding: 0;
            float: left;
            list-style: none;
          }}
          .maplegend .legend-scale ul li {{
            font-size: 10px;
            list-style: none;
            margin-left: 0;
            line-height: 18px;
            margin-bottom: 2px;
          }}
          .maplegend ul.legend-labels li span {{
            display: block;
            float: left;
            height: 16px;
            width: 30px;
            margin-right: 5px;
            margin-left: 0;
            border: 1px solid #999;
          }}
          .maplegend .legend-source {{
            font-size: 8px;
            color: #777;
            clear: both;
          }}
          .maplegend a {{
            color: #777;
          }}
        </style>
        '''

class MarimoHtmlOutput(io.StringIO):
    """
    Captures stdout and streams it to marimo through marimo.output as an HTML object.
    """

    Container_Html_Template: str = """
    <ul class="{container_class}">
        {lines}
    </ul>
    """

    Line_Html_Template: str = """
    <li class={line_class}>{content}</li>
    """

    def __init__(self, container_css_class="code-output-area", line_css_class="code-output-line"):
        super().__init__()
        self.container_css_class = container_css_class
        self.line_css_class = line_css_class
        self.lines = []
        self.current_line = ""  # Buffer for incomplete lines

    def write(self, text):
        # Extend the current line by the newly arrived text
        self.current_line += text

        parts = self.current_line.split('\n')
        # All parts except the last are complete lines
        complete_lines = parts[:-1]
        # The last part is either empty (if text ended with \n) or incomplete
        self.current_line = parts[-1]

        self.lines.extend(complete_lines)
        self._update_html()

        return len(text)

    def flush(self):
        # If there's remaining content in current_line, treat it as a complete line
        if self.current_line:
            self.lines.append(self.current_line)
            self.current_line = ""
            self._update_html()

    def _update_html(self):
        html_lines = []

        # Complete lines
        for line in self.lines:
            html_line = self.Line_Html_Template.format(line_class=self.line_css_class, content=html.escape(line))
            html_lines.append(html_line)

        # Add current incomplete line if it exists (for real-time feedback)
        if self.current_line:
            extended_class = f"{self.line_css_class} current-line"
            extended_content = f'{html.escape(self.current_line)}<span class="ellipse">...</span>'
            html_line = self.Line_Html_Template.format(line_class=extended_class, content=extended_content)
            html_lines.append(html_line)

        html_content = self.Container_Html_Template.format(
            container_class=self.container_css_class,
            lines='\n'.join(html_lines)
        )

        # Use mo.output to stream the updated HTML object to marimo
        mo.output.replace(mo.Html(html_content))

    def getvalue(self):
        # Return all lines joined with newlines, plus current incomplete line
        all_content = '\n'.join(self.lines)
        if self.current_line:
            all_content += '\n' + self.current_line if all_content else self.current_line
        return all_content


@contextmanager
def in_output_area(container_css_class="code-output-area", line_css_class="code-output-line"):
    """
    Use this context manager in a with statement to redirect all STDOUT writes to custom HTML
    output area that is live-streamed to marimo using marimo.output.

    Example:
        with in_output_area():
            print("This gets redirected to a custom output area!")
    """

    output = MarimoHtmlOutput(container_css_class, line_css_class)
    with redirect_stdout(output):
        try:
            yield
        finally:
            output.flush()


def run_code(run_signal: bool, task: Callable[[], Any], container_css_class="code-output-area", line_css_class="code-output-line"):
    """
    Executed the given task once the run_signal changes to True. During the execution of the task, all STDOUT write
    operations are redirected to a custom HTML output area that is live-streamed to marimo using marimo.output.

    Example:
        ```
            # Cell 1
            run_button = marimo.ui.run_button(label="Run code")

            # Cell 2
            def my_task():
                print("Hello world")

            run_code(run_button.value, my_task)
        ```
    """
    if run_signal:
        output = MarimoHtmlOutput(container_css_class, line_css_class)
        with redirect_stdout(output):
            try:
                task()
            finally:
                output.flush()
    else:
        # Add the presentation of a code output area before it is run here, if desired
        #mo.output.append("^ Press to run ^")
        pass


def create_run_button(label="Execute query", extra_classes: str = "", **kwargs) -> mo.Html:
    button = mo.ui.run_button(label=label, kind="neutral", **kwargs)
    extended_button_html = mo.Html(f"""
    <div class="run-code {extra_classes}">
        {button}
    <div>
    """)

    mo.output.append(extended_button_html)
    return button

def snake_to_title_case(snake_str: str, remove_words: list[str] = None) -> str:
    if remove_words:
        return " ".join(x.capitalize() for x in snake_str.lower().split("_") if x not in remove_words)
    else:
        return " ".join(x.capitalize() for x in snake_str.lower().split("_"))