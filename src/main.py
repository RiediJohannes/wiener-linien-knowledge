import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def imports():
    import components.graph as graph
    import components.geo_spatial as geo
    return geo, graph


@app.cell
def import_city_data(graph):
    graph.import_city_data()
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
