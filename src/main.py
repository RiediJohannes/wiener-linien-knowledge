import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib as pl

    import pandas as pd
    import geopandas as gp
    import folium as fl

    import gtfs_kit as gtfs

    DATA = pl.Path("./data")
    return DATA, gtfs


@app.cell
def _(DATA, gtfs):
    path = DATA / "wiener_linien_gtfs.zip"
    gtfs.list_feed(path)
    return (path,)


@app.cell
def _(gtfs, path):
    # Read feed and describe

    feed = gtfs.read_feed(path, dist_units="m")
    feed.describe()
    return


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo
    return


if __name__ == "__main__":
    app.run()
