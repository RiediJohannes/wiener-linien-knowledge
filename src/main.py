import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium", app_title="", css_file="styles/notebook.css")


@app.cell
def imports():
    import marimo as mo

    import src.components.graph as graph
    import src.components.geo_spatial as geo
    import src.components.presentation as present
    return geo, graph, mo


@app.cell
def project_heading(mo):
    mo.callout(mo.md("""
    # 🚊 Wiener Linien Knowledge Graph 🚋
    """), kind='danger')
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    # Creating the knowledge graph
    First, we need to load the data about Vienna's registration districts into our knowledge graph. This includes information about the registration districts' naming, their population and area, as well as their geographic coordinates.
    """
    )
    return


@app.cell
def import_city_data(graph):
    graph.import_city_data()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Knowledge graph evolution
    Next, we obtain new knowledge from our data.

    ## Merging nearby stops
    In the first step, we detect all groups of nearby stops that practically function like one unified stop.
    """
    )
    return


@app.cell
def merging_nearby_stops(geo, graph):
    _stops = graph.get_stops()
    print(f"Queried {len(_stops)} stops from the graph")

    _stop_clusters = geo.find_stop_clusters(_stops, 200, 400)
    print(f"Detected {len(_stop_clusters)} clusters of stops")
    _summary = graph.cluster_stops(_stop_clusters)
    print(f"Created {_summary.counters.relationships_created} relationships")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    This bundles lots of stops that are geographically very close to each other, while still enforcing some upper limits on the diameter of such clusters to prevent the formation of long chains. However, these constraints might still rip apart some stations whose individual exits or platforms are particularly far apart.

    Luckily, the Wiener Linien used somewhat of a hierarchichal structure when assigning unique IDs to their stops. Using the semantics of these stop IDs, we can further improve our stop clustering and stitch together all platforms/exits of any station.
    """
    )
    return


@app.cell
def detect_station_exits(hi):
    hi
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Matching stops to districts
    Our next goal is to match each stop to Viennese registration districts they serve.  
    First, let's delete stops entirely that are located outside the Vienna city boundary. Most notably, this includes many stops of the _Badner Bahn_ that reach all the way to _Baden bei Wien_.

    This is a simple task by relying again on the semantics of the stop IDs. Stops whose ID starts with `at:49` are within Vienna whereas stops with `at:43` at the beginning of their ID are located outside Vienna. Thus, we find and delete the latter.
    """
    )
    return


@app.cell
def _(graph):
    _operation = """
    MATCH (s:Stop)
    WHERE s.id STARTS WITH 'at:43:'
    DETACH DELETE s
    """

    _summary = graph.execute_operation(_operation)
    print(f"Deleted {_summary.counters.nodes_deleted} nodes")
    return


@app.cell
def _(mo):
    mo.md(r"""Next, we use the geographic coordinates of the stops to match each stop to the Viennese registration districts that either contain the stop or are reasonably close to it.""")
    return


@app.cell
def match_stops_with_districts(geo, graph, subdistricts):
    _stops = graph.get_stops()
    print(f"Queried {len(_stops)} stops from the graph")
    _subdistricts = graph.get_subdistricts()
    print(f"Queried {len(subdistricts)} subdistricts from the graph")

    _stops_within_districts = geo.match_stops_to_subdistricts(_stops, _subdistricts, buffer_metres = 20)
    _summary = graph.connect_stop_to_subdistricts(_stops_within_districts, 'LOCATED_IN')
    print(f"Created {_summary.counters.relationships_created} LOCATED_IN relationships")

    _stops_close_to_districts = geo.match_stops_to_subdistricts(_stops, _subdistricts, buffer_metres = 500)
    _summary = graph.connect_stop_to_subdistricts(_stops_close_to_districts, 'LOCATED_NEARBY')
    print(f"Created {_summary.counters.relationships_created} LOCATED_NEARBY relationships")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    Additionally, we define:
    > For any stop $s$, if there is another stop $s'$ such that $s'$ is close/nearby to a subdistrict $d$ and $s$ functions as $s'$, then
    then $s$ is also close/nearby to d.

    This can be solved entirely with a cypher query:
    ```cypher
    MATCH (s:Stop)-[f:FUNCTIONS_AS]->(t:Stop)-[l:LOCATED_IN|LOCATED_NEARBY]->(d:SubDistrict)
    WHERE NOT (s)-[:LOCATED_NEARBY]->(d)
    MERGE (s)-[:LOCATED_NEARBY]->(d);
    ```
    """
    )
    return


@app.cell
def functions_entails_vicinity(graph):
    _operation = """
    MATCH (s:Stop)-[f:FUNCTIONS_AS]->(t:Stop)-[l:LOCATED_IN|LOCATED_NEARBY]->(d:SubDistrict)
    WHERE NOT (s)-[:LOCATED_NEARBY]->(d)
    MERGE (s)-[:LOCATED_NEARBY]->(d);
    """

    _summary = graph.execute_operation(_operation)
    print(f"Created {_summary.counters.relationships_created} LOCATED_NEARBY relationships")
    return


@app.cell
def _(graph, mo):
    import folium
    import pandas as pd

    _stops = graph.get_stops()

    # Create a folium map centered on the mean of the coordinates
    m = folium.Map(
        tiles='https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png',
        attr='&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a> &copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        location=[48.2202331, 16.3796424],
        zoom_start=11
    )

    # Add markers for each stop
    for stop in _stops:
        folium.CircleMarker(
            location=[stop.lat, stop.lon],
            radius=2,
            color="red",
            fill=True,
            fill_opacity=0.4,
            opacity=0.6,
            popup=stop.name,
            tooltip=stop.id
        ).add_to(m)


    # Save the map to an HTML file
    #m.save("stops_map.html")

    mo.iframe(m._repr_html_(), height=600)
    return


if __name__ == "__main__":
    app.run()
