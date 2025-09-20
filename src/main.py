import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium", app_title="", css_file="styles/notebook.css")


@app.cell(hide_code=True)
def imports():
    import marimo as mo

    import src.components.graph as graph
    import src.components.geo_spatial as geo
    import src.components.presentation as present
    import src.components.learning as learning
    import src.components.prediction as prediction

    def print_raw(message: str):
        mo.output.append(mo.plain_text(message))
    return graph, mo, present


@app.cell(hide_code=True)
def project_heading(mo):
    mo.callout(mo.md("""
    # 🚊 Wiener Linien Knowledge Graph 🚋
    """), kind='danger')
    return


@app.cell(hide_code=True)
def _(mo):
    operation_cluster_position = """
    MATCH (s:Stop)-[:IN_CLUSTER]->(c:ClusterStop)
    WITH c, avg(s.lat) AS cluster_lat, avg(s.lon) AS cluster_lon
    SET c.cluster_lat = cluster_lat,
        c.cluster_lon = cluster_lon
    """

    mo.md(fr"""
    Lastly, we calculate the average position of all stops in a cluster and store that as the position of the overall cluster in the cluster stop for display purposes.
    ```cypher
    {operation_cluster_position}
    ```
    """)
    return (operation_cluster_position,)


@app.cell
def _(graph, operation_cluster_position):
    print("Calculating the average position")
    _summary = graph.execute_operation(operation_cluster_position)
    print(f"Set {_summary.counters.properties_set} properties")

    print(4)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ### Exploring Stop Clusters

    The following interactive map displays all stops we parsed from the GTFS data as well as the clusters we formed from geographically close stops. You can also explore which stops are considered nearby a specific subdistrict.
    """
    )
    return


@app.cell
def _(graph, mo):
    # Foreground: Create the user inputs such that the user can select what data to show on the map

    # Function to retrieve the available subdistricts for a given district
    def get_subdistricts_per_district():
        subdistrict_query = """
        MATCH (s:SubDistrict)
        WITH s.district_num as dist, s.sub_district_num as subdist
        ORDER BY dist, subdist
        RETURN dist as district, collect(subdist) as subdistricts
        """

        result = graph.execute_query(subdistrict_query)

        # Convert to dictionary
        district_mapping = {}
        for record in result:
            district = record['district']
            subdistricts = sorted(record['subdistricts'])
            district_mapping[district] = subdistricts

        return district_mapping

    # Create UI elements for each tab
    stops_map_stop_list_input = mo.ui.text_area(
        rows=1,
        placeholder="at:49:1000:0:1, Karlsplatz, ..."
    )

    stops_map_stop_input = mo.ui.text(
        label="Enter a single stop ID/name: ",
        placeholder="Karlsplatz"
    )

    subdistricts_per_dist = get_subdistricts_per_district()
    stops_map_district_num_combobox = mo.ui.dropdown(
        options=sorted(subdistricts_per_dist.keys()),
        label="District:"
    )
    return (
        stops_map_district_num_combobox,
        stops_map_stop_input,
        stops_map_stop_list_input,
        subdistricts_per_dist,
    )


@app.cell
def _(mo, stops_map_district_num_combobox, subdistricts_per_dist):
    _available_subdistricts = subdistricts_per_dist.get(stops_map_district_num_combobox.value, [1])
    stops_map_subdistrict_num_combobox = mo.ui.dropdown(
        options=_available_subdistricts,
        label="Subdistrict:"
    )

    stops_map_search_button = mo.ui.run_button(
        kind="success",
        label="Search"
    )
    return stops_map_search_button, stops_map_subdistrict_num_combobox


@app.cell
def _(mo):
    get_stops_map_tab, set_stops_map_tab = mo.state("All stops")
    return get_stops_map_tab, set_stops_map_tab


@app.cell
def _(
    mo,
    set_stops_map_tab,
    stops_map_district_num_combobox,
    stops_map_stop_input,
    stops_map_stop_list_input,
    stops_map_subdistrict_num_combobox,
):
    stops_map_tabs = mo.ui.tabs({
        "All Stops": mo.vstack([
            mo.md("**All Stops with Clusters**"),
            mo.md("""_Shows all transit stops in Vienna as well as the clusters we built  
            from them._"""),
        ]),
        "Specific Stops": mo.vstack([
            mo.md("**View Specific Stops**"),
            mo.md("Enter a list of stop IDs or names (comma-separated):"),
            stops_map_stop_list_input,
        ]),
        "Specific Cluster": mo.vstack([
            mo.md("**View a Specific Stop Cluster**"),
            mo.md("*Find all stops belonging to the same cluster as a given station.*"),
            stops_map_stop_input,
        ]),
        "Near Subdistrict": mo.vstack([
            mo.md("**Stops by District/Subdistrict**"),
            mo.md("*Vienna has 23 districts, split further into a total of 250 subdistricts.*"),
            mo.hstack([stops_map_district_num_combobox, stops_map_subdistrict_num_combobox], justify="start", gap=1.5),
        ])
    }, on_change=set_stops_map_tab)
    return (stops_map_tabs,)


@app.cell
def _(mo, stops_map_search_button, stops_map_tabs):
    #print(get_stops_map_tab())
    #print(stops_map_tabs.value)

    # Display the tabs
    mo.hstack([stops_map_tabs, stops_map_search_button], justify="start", gap=2)
    return


@app.cell(hide_code=True)
def _(
    get_stops_map_tab,
    graph,
    stops_map_district_num_combobox,
    stops_map_stop_input,
    stops_map_stop_list_input,
    stops_map_subdistrict_num_combobox,
):
    # Behind the scenes: Query the respective data based on the user-selection
    def stops_map_get_data():
        active_tab = get_stops_map_tab()
        stops = []
        description = "No data selected"

        if active_tab == "All Stops":
            stops = graph.get_stops(with_clusters=True)
            description = "All Stops with Clusters"
        elif active_tab == "Specific Stops":
            # Parse the comma-separated stop IDs
            stop_inputs = [input.strip() for input in stops_map_stop_list_input.value.split(',') if len(input.strip()) > 0]
            if stop_inputs:
                stops = graph.get_stops(id_list=stop_inputs, name_list=stop_inputs) # Check inputs both for ID and name matches
                description = f"Specific Stops ({len(stops)} requested)"
        elif active_tab == "Specific Cluster":
            stop_identifier = stops_map_stop_input.value.strip()
            if stop_identifier:
                stops = graph.get_stop_cluster(stop_identifier)
                description = f"Found {len(stops)} stop(s) in {sum(1 for node in stops if node.is_root)} cluster(s)"
        elif active_tab == "Near Subdistrict":
            district = stops_map_district_num_combobox.value
            subdistrict = stops_map_subdistrict_num_combobox.value
            stops = graph.get_stops_for_subdistrict(district, subdistrict)
            description = f"District {district}, Subdistrict {subdistrict}"


        return stops, description

    #stops_map_tabs.value
    return (stops_map_get_data,)


@app.cell
def _(mo, present, stops_map_get_data, stops_map_search_button):
    _transport_map = present.TransportMap(lat=48.2102331, lon=16.3796424, zoom=11)

    _stack = [mo.md(f"**Press 'Search' to run query**")]
    # Add the stops to the map
    if (stops_map_search_button.value):
        _stops, _description = stops_map_get_data()
        _transport_map.add_stops(_stops)
        _heading = mo.md(f"### **{_description}**")

        # Display the results
        _iframe = mo.iframe(_transport_map.as_html(), height=650)
        _stack = [_heading, _iframe]

    mo.vstack(_stack)
    return


if __name__ == "__main__":
    app.run()
