import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib as pl

    import pandas as pd
    import geopandas as gp
    import folium as fl

    import gtfs_kit as gk

    DATA = pl.Path("./data")
    return DATA, gk


@app.cell
def _(DATA, gk):
    path = DATA / "wiener_linien_gtfs.zip"
    gk.list_feed(path)
    return (path,)


@app.cell
def _(gk, path):
    # Read feed and describe

    feed = gk.read_feed(path, dist_units="m")
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
