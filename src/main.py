import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def imports():
    import components.graph as graph
    import components.geo_spatial as geo

    return graph, geo

@app.cell
def import_city_data(graph):
    graph.import_city_data()
    return

@app.cell
def match_stops_with_districts(graph, geo):
    stops = graph.get_stops()
    print(f"Queried {len(stops)} stops from the graph")
    subdistricts = graph.get_subdistricts()
    print(f"Queried {len(subdistricts)} subdistricts from the graph")

    matched_stops = geo.match_stops_to_subdistricts(stops, subdistricts)
    print(matched_stops)


# @app.cell
# def _():
#     import pathlib as pl
#
#     # import pandas as pd
#     # import geopandas as gp
#     # import folium as fl
#
#     import gtfs_kit as gtfs
#
#     DATA = pl.Path("./data")
#     return DATA, gtfs


# @app.cell
# def _(DATA, gtfs):
#     path = DATA / "wiener_linien_gtfs.zip"
#     gtfs.list_feed(path)
#     return (path,)

if __name__ == "__main__":
    app.run()
