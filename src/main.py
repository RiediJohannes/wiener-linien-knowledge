import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib as pl

    # import pandas as pd
    # import geopandas as gp
    # import folium as fl

    import gtfs_kit as gtfs

    DATA = pl.Path("./data")
    return DATA, gtfs


# @app.cell
# def _(DATA, gtfs):
#     path = DATA / "wiener_linien_gtfs.zip"
#     gtfs.list_feed(path)
#     return (path,)


@app.cell
def _():
    import components.graph as graph

    graph.import_population_data()
    return


if __name__ == "__main__":
    app.run()
