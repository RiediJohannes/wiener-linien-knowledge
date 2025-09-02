import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def imports():
    import marimo as mo

    import components.graph as graph
    import components.geo_spatial as geo
    return geo, graph, mo


@app.cell
def _(mo):
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
    In the first step, we detect all groups of nearby stops that practically function like one single stop.
    """
    )
    return


@app.cell
def _(geo, graph):
    stops = graph.get_stops()
    print(f"Queried {len(stops)} stops from the graph")

    result = geo.find_stop_clusters(stops, 100, 250)
    for label in result:
        print(label)
    print(len(result))
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Matching stops to districts
    Next, we match each stop to the Viennese registration districts that either contain the stop or are reasonably close to the stop.
    """
    )
    return


@app.cell
def match_stops_with_districts(geo, graph):
    stops = graph.get_stops()
    print(f"Queried {len(stops)} stops from the graph")
    subdistricts = graph.get_subdistricts()
    print(f"Queried {len(subdistricts)} subdistricts from the graph")

    stops_within_districts = geo.match_stops_to_subdistricts(stops, subdistricts, buffer_metres = 50)
    for stop in stops_within_districts:
        print(stop)

    stops_close_to_districts = geo.match_stops_to_subdistricts(stops, subdistricts, buffer_metres = 500)
    for stop in stops_close_to_districts:
        print(stop)
    return


if __name__ == "__main__":
    app.run()
