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
    return geo, graph, learning, mo, prediction, present, print_raw


@app.cell(hide_code=True)
def _(mo):
    mo.Html("""
    <div id="title">
    🚊 Wiener Linien Knowledge Graph 🚋
    </div
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # The Knowledge Graph

    If you started this project as instructed (using docker compose), you should already have a running instance of the **Neo4j graph database** filled with (or currently being filled with) **geographic and demographic data** about the city of Vienna as well as a vast dataset of the city's **public transport schedule** for 2025.

    In particular, the initial data encompasses a general transit feed specification (GTFS) dataset by Wiener Linien GmbH & Co KG that accurately describes all public transport operations by Wiener Linien from 15.12.2024 to 13.12.2025. Additionally, the database contains rich information about Vienna's registration districts (subdistricts), which includes the subdistricts' naming, their population and area, as well as their geographic coordinates. This collection of data has been mapped from various CSV files (7 files for GTFS, 3 files for the subdistricts) into a graph structure to form the **Wiener Linien Knowledge Graph**.

    You can check the availability and status of the knowledge graph below:
    """
    )
    return


@app.cell(hide_code=True)
def _(graph, mo, print_raw):
    _labels = [
        ("Agencies", "agencies"), 
        ("Routes", "routes"), 
        ("Trips", "trips"), 
        ("Stops", "stops"), 
        ("Services", "services"), 
        ("Service exceptions", "exceptions")
    ]

    _node_count_query = """
    MATCH (n)
    RETURN
      count(CASE WHEN n:Agency THEN 1 END) AS agencies,
      count(CASE WHEN n:Route THEN 1 END) AS routes,
      count(CASE WHEN n:Trip THEN 1 END) AS trips,
      count(CASE WHEN n:Stop THEN 1 END) AS stops,
      count(CASE WHEN n:Service THEN 1 END) AS services,
      count(CASE WHEN n:ServiceException THEN 1 END) AS exceptions,
      count(CASE WHEN n:SubDistrict THEN 1 END) AS subdistricts
    """

    _stop_times_query = """
    MATCH ()-[r:STOPS_AT]->()
    RETURN count(r) as count
    """

    _city_data_query = """
    OPTIONAL MATCH (n:SubDistrict)
    WHERE n.name IS NULL
    OPTIONAL MATCH (s:SubDistrict)
    WHERE s.shape IS NULL
    RETURN count(n) AS no_name_count, count(s) AS no_shape_count
    """

    def _update_graph_status(_event):
        _kg_available = graph.is_available()
        _availability_text = "✅ Graph database is available" if _kg_available else "❌ Graph database is currently not available"
        _status_header = mo.hstack([mo.plain_text(_availability_text), graph_status_refresh])
        mo.output.append(_status_header)

        if _kg_available:
            print_raw("Verifying GTFS data presence:")
            _node_counts = graph.execute_query(_node_count_query)[0]
            for name, key in _labels:
                _count = _node_counts[key] if key in _node_counts.keys() else 0
                print_raw(f"\t✅ {name}: {_count}" if _count > 0 else f"\t❌ No {name.lower()}")

            _stop_times = graph.execute_query(_stop_times_query)
            print_raw(f"\t✅ Stop times: {_stop_times[0]["count"]}" if _stop_times else f"\t❌ No stop times")

            print_raw("Verifying geographic/demographic data presence:")
            _count = _node_counts["subdistricts"] if key in _node_counts.keys() else 0
            print_raw(f"\t✅ Subdistricts: {_count}" if _count > 0 else f"\t❌ No subdistricts")
            _city_data = graph.execute_query(_city_data_query)[0]
            print_raw(f"\t✅ Subdistricts names" if _city_data['no_name_count'] == 0 else f"\t❌ Some subdistricts have no name")
            print_raw(f"\t✅ Subdistricts shapes" if _city_data['no_shape_count'] == 0 else f"\t❌ Some subdistricts have no shape")


    graph_status_refresh = mo.ui.refresh(
      label="Refresh status",
      options=["5s", "1m", "10m"],
      on_change=_update_graph_status
    )

    _update_graph_status(None)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""**Important:** If you are missing some of the green check marks above, this indicates that the knowledge graph initialization did not succeed or is not yet complete. Please start the project only through the docker compose file and wait for the completion of the knowledge graph initialization (might take a few minutes) _before proceeding any further_ on this page!""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Knowledge graph evolution
    Next, we obtain new knowledge from our data.

    ## Merging geographically related stops
    In the first step, we detect all groups of nearby stops that practically function like one unified stop.
    """
    )
    return


@app.cell
def merge_nearby_stops_runner(present):
    button_merge_nearby_stops = present.create_run_button(label="Merge Nearby Stops")
    return (button_merge_nearby_stops,)


@app.cell
def _(button_merge_nearby_stops, graph, mo):
    merging_nearby_stops_is_safe: bool = False
    button_continue_merging_nearby_stops = mo.ui.run_button(label="Continue Merging", kind="danger")

    if button_merge_nearby_stops.value:
        query_check_if_clusters_exist = """
        OPTIONAL MATCH (c:ClusterStop)
        OPTIONAL MATCH ()-[r:IN_CLUSTER]->()
        RETURN count(DISTINCT c) + count(DISTINCT r)
        """
        _num_cluster_elements: int = graph.execute_operation_returning_count(query_check_if_clusters_exist)

        if _num_cluster_elements > 0:
            mo.output.append(mo.md("**WARNING:** The knowledge graph already contains some clusters. To avoid data inconsistencies, this operation will delete all existing clusters before creating new ones from scratch.\nDo you want to continue?"))
            mo.output.append(button_continue_merging_nearby_stops)
        else:
            merging_nearby_stops_is_safe = True
    return button_continue_merging_nearby_stops, merging_nearby_stops_is_safe


@app.cell
def merge_nearby_stops(
    button_continue_merging_nearby_stops,
    geo,
    graph,
    merging_nearby_stops_is_safe: bool,
    present,
):
    def _merge_nearby_stops():
        # First, delete existing clusters
        print("Removing existing clusters...")
        operation_delete_existing_clusters = """
        MATCH cluster=()-[r:IN_CLUSTER]->()
        DELETE r
        """
        _summary = graph.execute_operation(operation_delete_existing_clusters)
        print(f"Deleted {_summary.counters.relationships_deleted} 'IN_CLUSTER' relationships.")

        operation_delete_clusterstop_labels = """
        MATCH (c:ClusterStop)
        REMOVE c:ClusterStop
        """
        _summary = graph.execute_operation(operation_delete_clusterstop_labels)
        print(f"Removed {_summary.counters.labels_removed} 'ClusterStop' labels from nodes.")

        # Next, we detect and create new clusters
        print("\nCreating new clusters...")
        _stops = graph.get_stops()
        print(f"Queried {len(_stops)} stops from the graph")

        _stop_clusters = geo.find_stop_clusters(_stops, 200, 400)
        print(f"Detected {len(_stop_clusters)} clusters of stops")

        _summary = graph.cluster_stops(_stop_clusters)
        print(f"""\nOperation successful:
        - Created {_summary.counters.relationships_created} relationships
        - Added {_summary.counters.labels_added} labels""")

        # Important: Reset this boolean flag, since now it's not safe anymore to create new clusters
        merging_nearby_stops_is_safe = False

    present.run_code(merging_nearby_stops_is_safe or button_continue_merging_nearby_stops.value, _merge_nearby_stops)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    This bundles lots of stops that are geographically very close to each other, while still enforcing some upper limits on the diameter of such clusters to prevent the formation of long chains. However, these constraints might consider some stops (exits/platforms) separate that officially belong to the same station but are particularly far apart.

    ### Merging hierarchichally related stops

    Luckily, the Wiener Linien used somewhat of a hierarchichal structure when assigning unique IDs to their stops. In particular, the IDs consist of five components delimited by a colon, where the last (fifth) component denotes the respective exit/platform of a station. Using these semantics, we can further improve our stop clustering and stitch together all platforms/exits of each station.
    """
    )
    return


@app.cell
def _(present):
    button_merge_related_stops = present.create_run_button(label="Merge Related Stops")
    return (button_merge_related_stops,)


@app.cell
def detect_station_exits(button_merge_related_stops, graph, present):
    def _merge_related_stops():
        print("Merging related clusters...")
        _updated_clusters: int = graph.merge_related_clusters()
        print(f"Updated {_updated_clusters} stop clusters")

        # Since this is such a complex operation, we verify that everything worked as expected
        print("Verifying integrity...")
        _query = """
        MATCH (s:Stop)-[:IN_CLUSTER]->(a), (s:Stop)-[:IN_CLUSTER]->(b)
        WHERE a.id <> b.id
        RETURN s.id, a.id, b.id
        """
        _result = graph.execute_query(_query)
        if not _result:
            print("✅ No node is in two clusters")
        else:
            print("❌ WARNING: Detected some stops that are in more than one cluster!")

        """
        MATCH (s:Stop)-[:IN_CLUSTER]->(p:Stop)
        WHERE NOT apoc.label.exists(p, "ClusterStop")
        RETURN s.id, p.id
        """
        _result = graph.execute_query(_query)
        if not _result:
            print("✅ Every cluster parent has the label 'ClusterStop'")   
        else:
            print("❌ WARNING: Detected some stops that are the root of a cluster but are missing the `ClusterStop` label!")

        """
        MATCH (s:ClusterStop)-[:IN_CLUSTER*1..5]-(p:ClusterStop)
        WHERE s.id <> p.id
        RETURN s.id, p.id
        """
        _result = graph.execute_query(_query)
        if not _result:
            print("✅ No cluster has more than one ClusterStop")
        else:
            print("❌ WARNING: Detected some clusters that have more than one ClusterStop member!")

    present.run_code(button_merge_related_stops.value, _merge_related_stops)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_assign_cluster_root = """
    // For each cluster, rank members by usage
    MATCH (stop:Stop)-[:IN_CLUSTER]->(parent:ClusterStop)
    OPTIONAL MATCH (stop)<-[:STOPS_AT]-(t:Trip)
    WITH parent, stop, count(t) AS tripCount
    ORDER BY parent, tripCount DESC, stop.name DESC, stop.id ASC
    WITH parent, collect(stop) AS clusterMembers
    WITH parent, clusterMembers, clusterMembers[0] AS mainStop
    WHERE parent.id <> mainStop.id

    // Assign a new parent for the cluster
    REMOVE parent:ClusterStop // Demote old cluster stop
    SET mainStop:ClusterStop  // Promote the busiest stop
    WITH parent, mainStop
    MATCH (n)-[rel:IN_CLUSTER]->(parent)
    CALL apoc.refactor.to(rel, mainStop) YIELD output
    RETURN count(*)
    """

    mo.md(fr"""
    ### Finding representative cluster roots

    So far, we have organized related stops into clusters that are represented by a root node with the tag `ClusterStop`. However, the choice of this cluster stop was merely arbitrary. Now, we want to determine a rightful representative by choosing the stop with the most traffic among all stops in the cluster.

    Therefore, we count the stop times scheduled for each cluster and make the busiest stop the new cluster stop.
    ```cypher
    {operation_assign_cluster_root}
    ```
    """
    )
    return (operation_assign_cluster_root,)


@app.cell
def _(present):
    button_reassign_cluster_stops = present.create_run_button(label="Reassign Cluster Stops")
    return (button_reassign_cluster_stops,)


@app.cell
def reassign_cluster_stops(
    button_reassign_cluster_stops,
    graph,
    operation_assign_cluster_root,
    present,
):
    def _reassign_cluster_stops():
        print("Re-assigning cluster stops...")
        _affected_rows = graph.execute_operation_returning_count(operation_assign_cluster_root)
        print(f"Affected {_affected_rows} nodes")

    present.run_code(button_reassign_cluster_stops.value, _reassign_cluster_stops)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_move_stop_relations_to_root = """
    MATCH (:Trip)-[at:STOPS_AT]->(s:Stop)-[:IN_CLUSTER]->(c:Stop:ClusterStop)
    WHERE s.id <> c.id
    CALL (at, c) {
      CALL apoc.refactor.to(at, c) YIELD output
      RETURN count(output) AS refactoredCount
    } IN TRANSACTIONS OF 10000 ROWS
    RETURN sum(refactoredCount) AS movedRelationships
    """

    mo.md(fr"""
    ### Move transport-related relationships to cluster stop

    Now that we have created clusters with the busiest stop in them as their root node (the `ClusterStop`), we move all `:STOPS_AT` relationships of the other nodes in the cluster to that `ClusterStop` instead.

    ```cypher
    {operation_move_stop_relations_to_root}
    ```
    **WARNING**: Be aware that this is a very expensive operation and might take a while to finish execution.
    """
    )
    return (operation_move_stop_relations_to_root,)


@app.cell
def _(present):
    button_move_stop_relations_to_root = present.create_run_button(label="Move Stop Relations")
    return (button_move_stop_relations_to_root,)


@app.cell
def _(
    button_move_stop_relations_to_root,
    graph,
    operation_move_stop_relations_to_root,
    present,
):
    def _move_stop_relations_to_root():
        print("Moving over all :STOPS_AT relationships to cluster roots...")
        _response = graph.execute_batched_query(operation_move_stop_relations_to_root)
        _moved_relationships: int = int(_response[0][0]) if _response else 0
        print(f"Moved a total of {_moved_relationships} :STOPS_AT relationships")

    present.run_code(button_move_stop_relations_to_root.value, _move_stop_relations_to_root)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""This concludes our efforts to find clusters of highly related stops and create structures that reflect this close relationship. The new data model greatly simplifies measuring the true importance of certain stops, which will aid us in predicting missing links in the public transport network.""")
    return


@app.cell(hide_code=True)
def _(mo):
    operation_delete_stops_outside_vienna = """
    MATCH (s:Stop)
    WHERE s.id STARTS WITH 'at:43:'
    DETACH DELETE s
    """

    mo.md(fr"""
    ## Matching stops to districts
    Our next goal is to match each stop to Viennese registration districts they serve in order to get a rough grasp on how many people can benefit them. 
    First, let's delete stops entirely that are located outside the Vienna city boundary. Most notably, this includes many stops of the _Badner Bahn_ that reach all the way to _Baden bei Wien_.

    This is a simple task by relying again on the semantics of the stop IDs. Stops whose ID starts with `at:49` are within Vienna whereas stops with `at:43` at the beginning of their ID are located outside Vienna. Thus, we find and delete the latter.
    ```cypher
    {operation_delete_stops_outside_vienna}
    ```
    """
    )
    return (operation_delete_stops_outside_vienna,)


@app.cell
def _(present):
    button_delete_stops_outside_vienna = present.create_run_button(label="Delete Stops outside Viena")
    return (button_delete_stops_outside_vienna,)


@app.cell
def _(
    button_delete_stops_outside_vienna,
    graph,
    operation_delete_stops_outside_vienna,
    present,
):
    def _delete_stops_outside_vienna():
        _summary = graph.execute_operation(operation_delete_stops_outside_vienna)
        print(f"Deleted {_summary.counters.nodes_deleted} nodes")

    present.run_code(button_delete_stops_outside_vienna.value, _delete_stops_outside_vienna)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Next, we use the geographic coordinates of the stops to match each stop to the Viennese registration districts that either contain the stop or are reasonably close to it.""")
    return


@app.cell
def _(present):
    button_match_stops_to_districts = present.create_run_button(label="Match Stops to Districts")
    return (button_match_stops_to_districts,)


@app.cell
def match_stops_with_districts(
    button_match_stops_to_districts,
    geo,
    graph,
    present,
):
    def _match_stops_to_districts():
        print("Querying stops and subdistricts...")
        _stops = graph.get_stops()
        print(f"Queried {len(_stops)} stops from the graph")
        _subdistricts = graph.get_subdistricts()
        print(f"Queried {len(_subdistricts)} subdistricts from the graph")

        print("Detecting stops within subdistricts...")
        _stops_within_districts = geo.match_stops_to_subdistricts(_stops, _subdistricts, buffer_metres = 20)
        _summary = graph.connect_stop_to_subdistricts(_stops_within_districts, 'LOCATED_IN')
        print(f"Created {_summary.counters.relationships_created} LOCATED_IN relationships")

        print("Detecting stops near subdistricts...")
        _stops_close_to_districts = geo.match_stops_to_subdistricts(_stops, _subdistricts, buffer_metres = 500)
        _summary = graph.connect_stop_to_subdistricts(_stops_close_to_districts, 'LOCATED_NEARBY')
        print(f"Created {_summary.counters.relationships_created} LOCATED_NEARBY relationships")

    present.run_code(button_match_stops_to_districts.value, _match_stops_to_districts)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_cluster_located_nearby = """
    MATCH (s:Stop)-[:IN_CLUSTER]->(c:ClusterStop),
          (s)-[:LOCATED_NEARBY]->(d:SubDistrict)
    WHERE NOT (c)-[:LOCATED_NEARBY]->(d)
    MERGE (c)-[:LOCATED_NEARBY]->(d);
    """

    operation_cluster_located_in = """
    MATCH (c:ClusterStop)<-[:IN_CLUSTER]-(s:Stop)
    WITH c, count(s) as clusterSize
    MATCH (c)<-[:IN_CLUSTER]-(s:Stop)-[:LOCATED_IN]->(d:SubDistrict)
    WHERE NOT (c)-[:LOCATED_IN]->(d)
    WITH c, d, count(s) as stopsInDistrict, clusterSize
    WHERE stopsInDistrict >= clusterSize / 2 OR stopsInDistrict >= 3
    MERGE (c)-[:LOCATED_IN]->(d)
    """

    mo.md(fr"""
    Additionally, we define:
    > For any cluster $C$, if **there exists** a stop $s \in C$ such that $s$ is **located nearby** a subdistrict $d$, then the cluster stop $c \in C$ is also considered nearby $d$.

    As well as:
    > For any cluster $C$, if **at least half** of the stops in $C$ are **located in** a subdistrict $d$, then the cluster stop $c \in C$ is also considered to be located in $d$.

    This can be solved using two simple cypher queries:
    ```cypher
    {operation_cluster_located_nearby}
    ```

    ```cypher
    {operation_cluster_located_in}
    ```
    """
    )
    return operation_cluster_located_in, operation_cluster_located_nearby


@app.cell
def _(present):
    button_determine_nearby_clusters = present.create_run_button(label="Determine Proximity for Clusters")
    return (button_determine_nearby_clusters,)


@app.cell
def functions_entails_vicinity(
    button_determine_nearby_clusters,
    graph,
    operation_cluster_located_in,
    operation_cluster_located_nearby,
    present,
):
    def _determine_nearby_clusters():
        # Located nearby relationships
        _summary = graph.execute_operation(operation_cluster_located_nearby)
        print(f"Created {_summary.counters.relationships_created} LOCATED_NEARBY relationships")

        # Located in relationships
        _summary = graph.execute_operation(operation_cluster_located_in)
        print(f"Created {_summary.counters.relationships_created} LOCATED_IN relationships")

    present.run_code(button_determine_nearby_clusters.value, _determine_nearby_clusters)
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
def _(present):
    button_calculate_cluster_position = present.create_run_button(label="Calculate Cluster Positions")
    return (button_calculate_cluster_position,)


@app.cell
def _(
    button_calculate_cluster_position,
    graph,
    operation_cluster_position,
    present,
):
    def _calculate_cluster_position():
        print("Calculating the average position of each cluster...")
        _summary = graph.execute_operation(operation_cluster_position)
        print(f"Set {_summary.counters.properties_set} properties")

    present.run_code(button_calculate_cluster_position.value, _calculate_cluster_position)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ----

    ### **Visualization:** Exploring Stop Clusters

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
    _available_subdistricts = subdistricts_per_dist.get(stops_map_district_num_combobox.value, [])
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
    # Define a state object to hold the currently open tab of the stops_map_tabs component
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
    # Display the tabs
    mo.hstack([stops_map_tabs, stops_map_search_button], justify="start", gap=2)
    return


@app.cell
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
        active_tab = get_stops_map_tab() # stops_map_tabs.value
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
    return (stops_map_get_data,)


@app.cell
def _(mo, present, stops_map_get_data, stops_map_search_button):
    _transport_map = present.TransportMap(lat=48.2102331, lon=16.3796424, zoom=12)

    _stack = [mo.md(f"_Press 'Search' to run query_")]
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


@app.cell(hide_code=True)
def _(graph, mo, present):
    if False:
        _stops = graph.get_stops(with_clusters=True, id_list=["at:49:1000:0:1"], name_list=['Marx'])
        #_stops = graph.get_stops(id_list=["at:49:1000:0:1", "at:49:1081:0:1"])
        #_stops = graph.get_stop_cluster('Valiergasse')
        #_stops = graph.get_stops_for_subdistrict(11, 2)

        # tiles='https://{s}.tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=2006ee957e924a28a24e5be254c48329',
        # attr='&copy; <a href="http://www.thunderforest.com/">Thunderforest</a>, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        _transport_map = present.TransportMap(lat=48.2102331, lon=16.3796424, zoom=11)
        _transport_map.add_stops(_stops)

        # Visible output
        _heading = mo.md("### **Explore stop locations and clusters**")
        _iframe = mo.iframe(_transport_map.as_html(), height=650)
        mo.vstack([_heading, _iframe])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ----

    ## Organizing City Districts

    ### Determine neighbouring subdistricts

    To better describe the geographic relationship between subdistricts, we determine which subdistricts border each other and store this in our knowledge graph.
    """
    )
    return


@app.cell
def _(present):
    button_determine_neighbouring_districts = present.create_run_button(label="Determine Neighbouring Districts")
    return (button_determine_neighbouring_districts,)


@app.cell
def _(button_determine_neighbouring_districts, geo, graph, present):
    def _determine_neighbouring_districts():
        _subdistricts = graph.get_subdistricts()

        print("Finding neighbours of each subdistrict...")
        # For each district, collect a list of neighbouring districts (with a tolerance of 20 metres)
        _neighbours = geo.find_neighbouring_subdistricts(_subdistricts, buffer_metres=20)

        _operation = """
        WITH $neighbours_dict as source_dict
        UNWIND keys(source_dict) AS district_id 
          WITH source_dict, district_id, 
             toInteger(split(district_id, '-')[0]) AS left_dist, 
             toInteger(split(district_id, '-')[1]) AS left_sub
          MATCH (left:SubDistrict {district_num: left_dist, sub_district_num: left_sub})

          UNWIND source_dict[district_id] AS neighbour_id
            WITH left, 
               toInteger(split(neighbour_id, '-')[0]) AS right_dist, 
               toInteger(split(neighbour_id, '-')[1]) AS right_sub
            MATCH (right:SubDistrict {district_num: right_dist, sub_district_num: right_sub})
            MERGE (left)-[:NEIGHBOURS]->(right)
        """

        print("Adding ':NEIGHBOURS' relationships to districts...")
        _summary = graph.execute_operation(_operation, neighbours_dict=_neighbours)
        print(f"Created {_summary.counters.relationships_created} relationships")

    present.run_code(button_determine_neighbouring_districts.value, _determine_neighbouring_districts)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_calculate_population_density = """
    MATCH (d:SubDistrict)
    SET d.density = 1_000_000 * d.population / d.area;
    """

    mo.md(fr"""
    ### Pre-calculate population density of all subdistricts

    Since a subdistrict's area is given in m², we multiply the population count by one million to get the population density in people/km².

    ```cypher
    {operation_calculate_population_density}
    ```
    """
    )
    return (operation_calculate_population_density,)


@app.cell
def _(present):
    button_calculate_population_density = present.create_run_button(label="Calculate Population Densities")
    return (button_calculate_population_density,)


@app.cell
def _(
    button_calculate_population_density,
    graph,
    operation_calculate_population_density,
    present,
):
    def _calculate_population_density():
        print("Calculating population density of each subdistrict...")
        _summary = graph.execute_operation(operation_calculate_population_density)
        print(f"(Re)set {_summary.counters.properties_set} properties")

    present.run_code(button_calculate_population_density.value, _calculate_population_density)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_classify_service_exceptions = """
    MATCH (ex:ServiceException)
    WHERE ex.exception_type = 1
    SET ex: AddedService;

    MATCH (ex:ServiceException)
    WHERE ex.exception_type = 2
    SET ex: RemovedService;
    """

    operation_classify_trips = """
    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 0  // enum value for trams or light rail
    SET t: TramTrip;

    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 1  // enum value for subways
    SET t: SubwayTrip;

    // Note: r.type = 2 would be for trains but our dataset does not include S-Bahn trips,
    // as they are operated by ÖBB instead of Wiener Linien.

    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 3  // enum value for buses
    SET t: BusTrip;
    """

    operation_classify_stops = """
    // Bus stops
    MATCH (s:Stop)<-[:STOPS_AT]-(:BusTrip)
    WHERE NOT (s:BusStop)
    SET s: BusStop;

    // Tram stops
    MATCH (s:Stop)<-[:STOPS_AT]-(:TramTrip)
    WHERE NOT (s:TramStop)
    SET s: TramStop;

    // Subway stops/stations
    MATCH (s:Stop)<-[:STOPS_AT]-(:SubwayTrip)
    WHERE NOT (s:SubwayStation)
    SET s: SubwayStation;
    """

    operation_collect_in_use_stops = """
    MATCH (s:Stop&(BusStop|TramStop|SubwayStation))
    WHERE NOT (s: InUse)
    SET s: InUse;
    """

    # ----------------------------- markdown ------------------------------

    mo.md(fr"""
    ### Adding Labels

    Additionally, we add some **new labels to existing entities** based on their properties/relationships for easier retrieval of a certain category of entities later. 

    **Classify exceptions** to the regularly scheduled service into additional and removed service:
    ```cypher
    {operation_classify_service_exceptions}
    ```

    **Label trips** according to their mode of transport:
    ```cypher
    {operation_classify_trips}
    ```

    **Classify stops** according to their usage:
    ```cypher
    {operation_classify_stops}
    ```

    Collect all stops that are _used by some trip_ (now after we have redirected trips to cluster roots)
    ```cypher
    {operation_collect_in_use_stops}
    ```
    """
    )
    return (
        operation_classify_service_exceptions,
        operation_classify_stops,
        operation_classify_trips,
        operation_collect_in_use_stops,
    )


@app.cell
def _(present):
    button_classify_service_exceptions = present.create_run_button(label="Classify Exceptions+Trips+Stops")
    return (button_classify_service_exceptions,)


@app.cell
def _(
    button_classify_service_exceptions,
    graph,
    operation_classify_service_exceptions,
    operation_classify_stops,
    operation_classify_trips,
    operation_collect_in_use_stops,
    present,
):
    def _classify_service_exceptions():
        print("Classifying service exceptions...")
        _added_exceptions_query, _removed_exceptions_query, _ = operation_classify_service_exceptions.split(";")
        _summary = graph.execute_operation(_added_exceptions_query)
        print(f"Added {_summary.counters.labels_added} ':AddedService' labels")
        _summary = graph.execute_operation(_removed_exceptions_query)
        print(f"Added {_summary.counters.labels_added} ':RemovedService' labels")

        print("\nClassifying trips according to mode of transport...")
        _bus_trips_query, _tram_trips_query, _subway_trips_query, _ = operation_classify_trips.split(";")
        _summary = graph.execute_operation(_bus_trips_query)
        print(f"Added {_summary.counters.labels_added} ':BusTrip' labels")
        _summary = graph.execute_operation(_tram_trips_query)
        print(f"Added {_summary.counters.labels_added} ':TramTrip' labels")
        _summary = graph.execute_operation(_subway_trips_query)
        print(f"Added {_summary.counters.labels_added} ':SubwayTrip' labels")

        print("\nClassifying stops according to their transit connections...")
        _bus_stop_query, _tram_stop_query, _subway_station_query, _ = operation_classify_stops.split(";")
        _summary = graph.execute_operation(_bus_stop_query)
        print(f"Added {_summary.counters.labels_added} ':BusStop' labels")
        _summary = graph.execute_operation(_tram_stop_query)
        print(f"Added {_summary.counters.labels_added} ':TramStop' labels")
        _summary = graph.execute_operation(_subway_station_query)
        print(f"Added {_summary.counters.labels_added} ':SubwayStation' labels")

        print("\nMarking stops that are actually in use...")
        _summary = graph.execute_operation(operation_collect_in_use_stops)
        print(f"Added {_summary.counters.labels_added} ':InUse' labels")

    present.run_code(button_classify_service_exceptions.value, _classify_service_exceptions)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_find_related_stops = """
    MATCH (s:Stop:InUse), (t:Stop:InUse)
    WHERE s.id < t.id
    WITH s, t,
         point({
           latitude: CASE WHEN s:ClusterStop THEN s.cluster_lat ELSE s.lat END,
           longitude: CASE WHEN s:ClusterStop THEN s.cluster_lon ELSE s.lon END
         }) AS s_location,
         point({
           latitude: CASE WHEN t:ClusterStop THEN t.cluster_lat ELSE t.lat END,
           longitude: CASE WHEN t:ClusterStop THEN t.cluster_lon ELSE t.lon END
         }) AS t_location
    WITH s, t, s_location, t_location,
       point.distance(s_location, t_location) AS distance_meters
    WHERE distance_meters < 800
    MERGE (s)-[:IS_CLOSE_TO {distance: distance_meters}]->(t)
    MERGE (t)-[:IS_CLOSE_TO {distance: distance_meters}]->(s)
    """

    mo.md(
        fr"""
    ## Finding relations between stops

    ### Geographic proximity of stops

    First, we collect all pairs of stops that are within 800 metres of each other and connect them by a symmetric `:IS_CLOSE_TO` relationship. For points that are the root of a cluster, we take the geographic midpoint of that cluster which we calculated earlier.

    ```cypher
    {operation_find_related_stops}
    ```
    """
    )
    return (operation_find_related_stops,)


@app.cell
def _(present):
    button_find_neighbouring_stops = present.create_run_button(label="Find Neighbouring Stops")
    return (button_find_neighbouring_stops,)


@app.cell
def _(
    button_find_neighbouring_stops,
    graph,
    operation_find_related_stops,
    present,
):
    def _find_neighbouring_stops():
        print("Finding pairs of geographically close stops...")
        _summary = graph.execute_operation(operation_find_related_stops)
        print(f"Added {int(_summary.counters.relationships_created / 2)} symmetric :IS_CLOSE_TO relationships")

    present.run_code(button_find_neighbouring_stops.value, _find_neighbouring_stops)
    return


@app.cell
def _(mo):
    operation_find_far_apart_related_stops = """
    MATCH (s:Stop)-[:BUS_CONNECTS_TO|TRAM_CONNECTS_TO]->(t:Stop)
    WHERE NOT (s)-[:IS_CLOSE_TO]->(t)
    MERGE (s)-[:IS_CLOSE_TO]->(t)
    """

    mo.md(r"""While a distance of 800 metres covers most stop pairs with a direct connection, there are some consecutive stops which are unusually far apart. We still want to consider those to be in reach of each other. Thus, we artificially add a `IS_CLOSE_TO` relationship between such stops.""")
    return (operation_find_far_apart_related_stops,)


@app.cell
def _(present):
    button_find_far_but_connected_stops = present.create_run_button(label="Find Directly Connected Stops")
    return (button_find_far_but_connected_stops,)


@app.cell
def _(
    button_find_far_but_connected_stops,
    graph,
    operation_find_far_apart_related_stops,
    present,
):
    def _find_far_but_connected_stops():
        print("Finding pairs of geographically far apart but connected stops...")
        _summary = graph.execute_operation(operation_find_far_apart_related_stops)
        print(f"Added {int(_summary.counters.relationships_created)} additional :IS_CLOSE_TO relationships")

    present.run_code(button_find_far_but_connected_stops.value, _find_far_but_connected_stops)
    return


@app.cell(hide_code=True)
def _(mo):
    operation_calculate_frequency_of_trips = """
    MATCH (s:Service)
    // Calculate (approximately) how many times a year the trip is operated regularly
    WITH s,
        s.monday + s.tuesday + s.wednesday + s.thursday + s.friday + s.saturday + s.sunday AS days_per_week,
        duration.inDays(s.start_date, s.end_date).days + 1 AS operational_days
    WITH s,
        toInteger(ceil((operational_days / 7.0) * days_per_week)) as regular_operations_per_year
    // If there are some exceptions to the schedule, subtract them
    OPTIONAL MATCH (s)<-[:FOR_SERVICE]-(ex:ServiceException:RemovedService)
    WITH s, regular_operations_per_year, count(DISTINCT ex.date) AS removed_days
    // Store the result in a property of each schedule node
    SET s.operations_per_year = regular_operations_per_year - removed_days;
    """


    mo.md(r"""
    ### Transit connections between stops

    Our next main goal is to detect the **frequency of a direct public transport connections** from one stop to another. In the end, we would like every stop `s` in our graph to contain relationships of the form `(s)-[:X_CONNECTS_TO]->(t)` for every stop `t` it has a _direct_ connection to at least once per year, where `X` is replaced by the mode of transport ($X \in \{\texttt{BUS}, \texttt{TRAM}, \texttt{SUBWAY}\}$).
    """ +
    fr"""

    For that, we first calculate an approximation of the number of times a single trip is operated per year: 
    ```cypher
    {operation_calculate_frequency_of_trips}
    ```

    The value obtained by this query is not the exact number of trips in the year 2024, since it only considers the number of weeks in a year instead of the exact number of Mondays, Tuesdays, etc. in the year 2024. However, the numbers should be within roughly $2\%$ of the true value and basically represent a year-on-year average for each trip.
    """
    )
    return (operation_calculate_frequency_of_trips,)


@app.cell
def _(present):
    button_calculate_frequency_of_trips = present.create_run_button(label="Calculate Frequency of Trips")
    return (button_calculate_frequency_of_trips,)


@app.cell
def calculate_trips_per_year(
    button_calculate_frequency_of_trips,
    graph,
    operation_calculate_frequency_of_trips,
    present,
):
    def _calculate_frequency_of_trips():
        print("Calculating operations per year for every trip...")
        _summary = graph.execute_operation(operation_calculate_frequency_of_trips)
        print(f"Calculated and (re)set {_summary.counters.properties_set} properties")

    present.run_code(button_calculate_frequency_of_trips.value, _calculate_frequency_of_trips)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Next, we find every pair of stops $(s1, s2)$ such that some trip $t$ directly connects $s1$ to $s2$ (in that order). We aggregate over all such trips $t$ and sum their operations per year to get the total number of direct connections $s1 \to s2$ in a year.  
    Note that such trips do not necessarily move between the exact stops $s1$ and $s2$ but between two stops within their (separate) clusters, as we have previously moved all `:STOPS_AT` relationships to a cluster's root node.

    To determine a fine-grained level of service, we differentiate between modes of transport (bus, tram, subway) when creating these connection relationships, and we also store the exact number of operations per year for that connection in the `yearly` property of that relationship.

    ```cypher
    // Consider each pair of stops that appears consecutively in some trip t
    MATCH (t:Trip:{type_of_trip})-[at1:STOPS_AT]->(s1:Stop),
          (t)-[at2:STOPS_AT]->(s2:Stop)
    WHERE s1.id <> s2.id AND at2.stop_sequence = at1.stop_sequence + 1
    // Grab the unique Service connected to each trip t and sum up the yearly operations of all trips through s1 -> s2
    MATCH (t)-[:OPERATING_ON]->(service:Service)
    WITH s1, s2,
      sum(service.operations_per_year) as total_operations_per_year
    // Create a connection relationship with the number of connections per year
    MERGE (s1)-[conn:$type_of_connection]->(s2)
    SET conn.yearly = total_operations_per_year
    ```
    Just be sure to replace the variable `$type_of_trip` and `$type_of_connection` with the correct labels for the respective mode of transport.

    A quick word about the `WHERE` clause:

    - `s1.id <> s2.id` -- This guarantees that we ignore trips within a cluster to prevent overcounting, since our bundling of stops may have lead to sequential stops being part of the same cluster (and thus the `:STOPS_AT` relationship was redirected to the same root node) .
    - `st2.stop_sequence = st1.stop_sequence + 1` -- This ensures that we only consider _direct_ connections between stops.
    """
    )
    return


@app.cell
def _(present):
    button_find_connections_between_stops = present.create_run_button(label="Find Connections between Stops")
    return (button_find_connections_between_stops,)


@app.cell
def _(button_find_connections_between_stops, graph, present):
    def _find_connections_between_stops():
        _operation = """
        // Consider each pair of stops that appears consecutively in some trip t
        MATCH (t:Trip:{type_of_trip})-[at1:STOPS_AT]->(s1:Stop),
              (t)-[at2:STOPS_AT]->(s2:Stop)
        WHERE s1.id <> s2.id AND at2.stop_sequence = at1.stop_sequence + 1
        // Grab the unique Service connected to each trip t and sum up the yearly operations of all trips through s1 -> s2
        MATCH (t)-[:OPERATING_ON]->(service:Service)
        WITH s1, s2,
          sum(service.operations_per_year) as total_operations_per_year
        // Create a connection relationship with the number of connections per year
        MERGE (s1)-[conn:{type_of_connection}]->(s2)
        SET conn.yearly = total_operations_per_year
        """

        _modes_of_transport = [("BusTrip", "BUS_CONNECTS_TO"), ("TramTrip", "TRAM_CONNECTS_TO"), ("SubwayTrip", "SUBWAY_CONNECTS_TO")]

        for trip, connection in _modes_of_transport:
            print(f"Finding '{trip}' connections...")
            # We need to do string interpolation here since Neo4j does not allow parameters in labels
            _query = _operation.format(type_of_trip=trip, type_of_connection=connection)
            _summary = graph.execute_operation(_query)
            print(f"Created {_summary.counters.relationships_created} new '{connection}' relationships and set {_summary.counters.properties_set} yearly operations properties")

    present.run_code(button_find_connections_between_stops.value, _find_connections_between_stops)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ----

    ## **Visualization:** Explore Transit Connections

    Thus, we have completed all steps towards our overall goal of predicting missing transit connections. Below, you can find an interactive map to **explore key information about transit connections** that we have derived from out initial dataset.  
    These views of the data are a core subset of what we will use to train knowledge graph embedding models on in the next phase of our workflow.
    """
    )
    return


@app.cell
def _(mo):
    connections_map_tabs = mo.ui.tabs({
        "Connection Types": mo.vstack([
            mo.md("""_Shows the web of bus, tram and subway connections throughout Vienna._"""),
        ]),
        "Connection Frequency": mo.vstack([
            mo.md("_Shows all direct transit connections and colours them by how often they are operated across the whole year._")
        ])
    })

    connections_map_tabs
    return (connections_map_tabs,)


@app.cell
def _(connections_map_tabs, graph, present):
    # Behind the scenes: Query the respective data based on the user-selection
    def connections_map_get_data():
        active_tab = connections_map_tabs.value
        connections = []
        nodes = graph.get_stops(with_clusters=True, only_in_use=True)
        legend_config = ("Legend", [("key", "val")])

        if active_tab == "Connection Types":
            connections_query = """
            MATCH (s:Stop)-[c:BUS_CONNECTS_TO|TRAM_CONNECTS_TO|SUBWAY_CONNECTS_TO]-(t:Stop)
            WHERE s.id < t.id AND c.yearly > 4 * 365
            RETURN DISTINCT s as from, t as to, type(c) as label
            """
            connections = graph.get_connections(connections_query)
        
            legend_entries = [(present.snake_to_title_case(conn.name), colour)
                      for conn, colour in list(present.TransportMap.connection_colours.items())[:-1]]
            legend_config = ("Mode of Transport", legend_entries)
        
        elif active_tab == "Connection Frequency":
            connections_query = """
            MATCH (s1:Stop)-[conn:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]-(s2:Stop)
            WHERE s1.id < s2.id AND conn.yearly > 4 * 365
            WITH conn, s1, s2,
              CASE 
                WHEN conn.yearly > 105_000 THEN 'NONSTOP_TO'
                WHEN conn.yearly > 75_000 THEN 'VERY_FREQUENTLY_TO'
                WHEN conn.yearly > 50_000 THEN 'FREQUENTLY_TO'
                WHEN conn.yearly > 30_000 THEN 'REGULARLY_TO'
                WHEN conn.yearly > 8_000 THEN 'OCCASIONALLY_TO'
                ELSE 'RARELY_TO'
              END as level_of_service
            RETURN DISTINCT s1 as from, level_of_service as label, s2 as to
            """
            connections = graph.get_connections(connections_query)
        
            legend_entries = [(present.snake_to_title_case(conn.name, remove_words=["to"]), colour)
              for conn, colour in list(present.TransportMap.frequency_colours.items())[:-1]]
            legend_config = ("Connection Frequency", legend_entries)

        return nodes, connections, legend_config
    return (connections_map_get_data,)


@app.cell
def _(connections_map_get_data, mo, present):
    def display_connections_map():
        transport_map = present.TransportMap(lat=48.2102331, lon=16.3796424, zoom=12,
                                              visible_layers=present.VisibleLayers.STOPS | present.VisibleLayers.CONNECTIONS)
        nodes, connections, legend_config = connections_map_get_data()
        transport_map.add_transit_nodes(nodes)
        transport_map.add_transit_connections(connections)

        transport_map.add_legend(legend_config[0], legend_config[1])
    
        stop_disclaimer = mo.Html(f"""
        <div width="100%">
            {mo.md("**Note:** The map considers connections between **stop clusters** as opposed to individual stops. Therefore, the stops shown as black dots on the map are (in most cases) not exact stopping locations but the average position of the whole stop cluster.   This is exactly why we created stop clusters in the first place.")}
        </div>
        """)
    
        return mo.vstack([mo.iframe(transport_map.as_html(), height=650), stop_disclaimer])

    # Visible output
    mo.lazy(lambda : display_connections_map(), show_loading_indicator=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ----

    # Knowledge Graph Embeddings

    Now, our knowledge graph is ready for this project's main goal, which is **predicting missing connections in Vienna's public transport network**. As the wording suggests, this becomes a classic **link prediction problem** in our knowledge graph. A key tool to tackle such problems are knowledge graph embeddings. In this chapter, we will use the popular KG embedding library **PyKEEN**.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    triples_queries = {
    "Existing transit connections": """
    // Existing transit connections
    MATCH (s1:Stop)-[conn:BUS_CONNECTS_TO|TRAM_CONNECTS_TO|SUBWAY_CONNECTS_TO]->(s2:Stop)
    WHERE conn.yearly > 4 * 365
    RETURN s1.id as head, type(conn) as rel, s2.id as tail""",

    "Routes serving stops": """
    // Routes serving stops
    MATCH (t:Trip)-[:OPERATING_ON]->(ser:Service)
    WITH t, sum(ser.operations_per_year) as operations
    MATCH (r:Route)<-[:PART_OF_ROUTE]-(t)-[:STOPS_AT]->(s:Stop)
    WITH r.short_name as route_name, s, sum(operations) as trip_count
    WHERE trip_count >= 365
    RETURN route_name as head, 'SERVES' as rel, s.id as tail""",

    "Mode of transport of each route": """
    // Mode of transport of each route
    MATCH (r:Route)
    RETURN DISTINCT r.short_name as head, 'IS_MODE_OF_TRANSPORT' as rel,
    CASE r.type
      WHEN 0 THEN 'TRAM'
      WHEN 1 THEN 'SUBWAY'
      WHEN 2 THEN 'TRAIN'
      WHEN 3 THEN 'BUS'
      ELSE 'SPECIAL'
    END AS tail""",

    "Stop locations in/nearby subdistricts": """
    // Stop locations in/nearby subdistricts
    MATCH (s:Stop:InUse)-[loc:LOCATED_NEARBY|LOCATED_IN]->(d:SubDistrict)
    WITH s, loc, d.district_num + '-' + d.sub_district_num as subdistrict
    RETURN s.id as head, type(loc) as rel, subdistrict as tail""",

    "Neighbouring subdistricts": """
    // Neighbouring subdistricts
    MATCH (d1:SubDistrict)-[:NEIGHBOURS]->(d2:SubDistrict)
    WITH d1, d2,
        d1.district_num + '-' + d1.sub_district_num as left_neighbour,
        d2.district_num + '-' + d2.sub_district_num as right_neighbour
    RETURN left_neighbour as head, "NEIGHBOURS" as rel, right_neighbour as tail""",

    "Geographic proximity of stops": """
    // Geographic proximity of stops
    MATCH (s:Stop:InUse)-[c:IS_CLOSE_TO]->(t:Stop:InUse)
    RETURN s.id as head, type(c) as rel, t.id as tail""",

    "Classify districts by density": """
    // Classify districts according to their density
    MATCH (d:SubDistrict)
    WITH d, d.district_num + "-" + d.sub_district_num as district,
      CASE 
        WHEN d.density > 20_000 THEN 'VERY_HIGH_DENSITY'
        WHEN d.density > 10_000 THEN 'HIGH_DENSITY'  
        WHEN d.density > 5000 THEN 'MEDIUM_DENSITY'
        WHEN d.density > 1500 THEN 'LOW_DENSITY'
        ELSE 'VERY_LOW_DENSITY'
      END as density_category
    RETURN district as head, 'HAS_DENSITY' as rel, density_category as tail""",

    "Classify connections by frequency": """
    // Frequency of direct connections
    MATCH (s1:Stop)-[conn:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]->(s2:Stop)
    WHERE conn.yearly > 4 * 365
    WITH conn, s1, s2,
      CASE 
        WHEN conn.yearly > 105_000 THEN 'NONSTOP_TO'
        WHEN conn.yearly > 75_000 THEN 'VERY_FREQUENTLY_TO'
        WHEN conn.yearly > 50_000 THEN 'FREQUENTLY_TO'
        WHEN conn.yearly > 30_000 THEN 'REGULARLY_TO'
        WHEN conn.yearly > 8_000 THEN 'OCCASIONALLY_TO'
        ELSE 'RARELY_TO'
      END as level_of_service
    RETURN s1.id as head, level_of_service as rel, s2.id as tail"""
    }

    mo.md(f"""
    ## Training Triples Generation

    Since link prediction works on the basis of **triples** of the form $(h,r,t)$ ($h:$ head, $r:$ relation, $t:$ tail), we need to derive and extract meaningful triples from our knowledge graph. This training data should include all reasonable information about the domain that is considered relevant for planning new transport connections between existing stops.

    ### Existing Transit Network

    ```cypher
    {triples_queries["Existing transit connections"]}
    ```

    We exclude connections that happen fewer than four times a day (<1460 times a year) on average, since these mostly represent temporary reroutings due to construction work or special trips for operational reasons (e.g. ending a trip at a depot at the end of a day). Remember, this does not mean that e.g. a single bus line connects the two stops four times a day, but _ALL_ direct bus connections exceed four operations a day _on average_.

    ```cypher
    {triples_queries["Routes serving stops"]}
    {triples_queries["Mode of transport of each route"]}
    ```

    Similarly as for the connection relations before, we discard some noise. In this case, we only consider a route to serve a particular stop if it stops there at least once per day on average.

    ### Geographic Location

    Since the embedding model has no idea about Vienna's geography, we should represent the idea of proximity in our training data. 

    ```cypher
    {triples_queries["Stop locations in/nearby subdistricts"]}
    {triples_queries["Neighbouring subdistricts"]}
    {triples_queries["Geographic proximity of stops"]}
    ```

    ### Population Density

    In addition to knowing which stops serve which district, it is also paramount to know their population density to estimate the demand for public transport. Ideally, the embedding model will draw the conclusion that high-density subdistricts need frequent transit connections. 

    ```cypher
    {triples_queries["Classify districts by density"]}
    ```

    ### Level of Service

    Lastly, we categorize connections between stops regarding their frequency of operation. This complements the information about the type of connection (mode of transport) we gave earlier and hopefully correlates with population density.

    ```cypher
    {triples_queries["Classify connections by frequency"]}
    ```
    """
    )
    return (triples_queries,)


@app.cell
def _(present):
    button_query_triples = present.create_run_button(label="Query Training Triples")
    return (button_query_triples,)


@app.cell
def _(button_query_triples, graph, learning, present, triples_queries):
    training = []
    validation = []
    testing = []

    def _query_triples():
        fact_triples = graph.query_triples(triples_queries)

        # Index the entities/relations in the triples and split them into training, validation and testing data 
        training, validation, testing = learning.generate_training_set(fact_triples)

    present.run_code(button_query_triples.value, _query_triples)
    return testing, training, validation


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Model Training

    Next comes the setup for model training. We will train two models using different KG embedding algorithms and comparatively analyze their results.
    """
    )
    return


@app.cell
def _():
    # Prepare model training configurations
    training_configs = {
        'TransE': {
            'model': 'TransE',
            'model_kwargs': {'embedding_dim': 128, 'scoring_fct_norm': 1}, # L1 norm said to work better with TransE
            'optimizer_kwargs': {'lr': 0.001},
            'training_kwargs': {'num_epochs': 200, 'batch_size': 256},
            'negative_sampler': 'bernoulli',
            'negative_sampler_kwargs': {'num_negs_per_pos': 3},
            'loss': 'MarginRankingLoss',
            'loss_kwargs': {'margin': 1.0},
            'stopper': 'early',
            'stopper_kwargs':dict(
                patience=30,  # Stop if loss value doesn't improve for 30 iterations
                frequency=15 # Check every 10 epochs
            )
        },

        'RotatE': {
            'model': 'RotatE', 
            'model_kwargs': {'embedding_dim': 256},
            'optimizer_kwargs': {'lr': 0.0004},
            'training_kwargs': {'num_epochs': 450, 'batch_size': 512},
            'loss': 'MarginRankingLoss',
            'loss_kwargs': {'margin': 6.0},
            'negative_sampler': 'bernoulli',
            'negative_sampler_kwargs': {'num_negs_per_pos': 3},
            'stopper': 'early',
            'stopper_kwargs':dict(
                patience=30,  # Stop if loss value doesn't improve for 30 iterations
                frequency=15 # Check every 10 epochs
            )
        },

        'ComplEx': {
            'model': 'ComplEx',
            'model_kwargs': {'embedding_dim': 200},
            'optimizer': 'Adagrad', # said to work better with ComplEx
            'optimizer_kwargs': {'lr': 0.015},
            'training_kwargs': {'num_epochs': 600, 'batch_size': 512},
            'regularizer': 'LpRegularizer',
            'regularizer_kwargs': {'weight': 1e-6},
            'negative_sampler': 'bernoulli',
            'negative_sampler_kwargs': {'num_negs_per_pos': 3},
            'loss': 'SoftplusLoss',
            'loss_kwargs': {'reduction': 'mean'},  # Ensure proper reduction
            'stopper': 'early',
            'stopper_kwargs':dict(
                patience=30,  # Stop if loss value doesn't improve for 30 iterations
                frequency=15 # Check every 10 epochs
            )
        }
    }
    return (training_configs,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Model 1: RotatE

    The following code starts the training of the first graph embedding model, namely RotatE. Upon completion, the trained model is saved to the file system at `./trained_models/RotatE/`.

    **WARNING: This is a long-running task!** Depending on your hardware, this might take between 5-60 minutes.
    """
    )
    return


@app.cell
def _(present):
    button_train_model_rotate = present.create_run_button(label="Train Model RotatE", extra_classes="danger")
    return (button_train_model_rotate,)


@app.cell
def _(
    button_train_model_rotate,
    learning,
    present,
    testing,
    training,
    training_configs,
    validation,
):
    def _train_model_rotate():
        _model = 'RotatE'
        _save_path = f"trained_models/{_model}"

        rotate_results = learning.train_model(training, validation, testing, training_configs[_model])
        learning.save_training_results(rotate_results, _save_path, validation_triples=validation, testing_triples=testing)

        # Display some immediate results to assess the quality of the trained model
        learning.summarize_training_metrics(rotate_results.metric_results)

    present.run_code(button_train_model_rotate.value, _train_model_rotate)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Model 2: ComplEx

    The next code block starts the training of the second graph embedding model: ComplEx. Upon completion, the trained model is saved to the file system at `./trained_models/ComplEx/`.

    Again, this will take a long time, so be sure to consider that before running.
    """
    )
    return


@app.cell
def _(present):
    button_train_model_complex = present.create_run_button(label="Train Model ComplEx", extra_classes="danger")
    return (button_train_model_complex,)


@app.cell
def _(
    button_train_model_complex,
    learning,
    present,
    testing,
    training,
    training_configs,
    validation,
):
    def _train_model_complex():
        _model = 'ComplEx'
        _save_path = f"trained_models/{_model}"

        complex_results = learning.train_model(training, validation, testing, training_configs[_model])
        learning.save_training_results(complex_results, _save_path, validation_triples=validation, testing_triples=testing)

        # Display some immediate results to assess the quality of the trained model
        learning.summarize_training_metrics(complex_results.metric_results)

    present.run_code(button_train_model_complex.value, _train_model_complex)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Link Prediction

    With our KG embedding models trained, we can now utilize them to generate predictions about missing links in the public transport network. This is done by prompting the trained model with incomplete triples, i.e., triples $(h, r, t)$ where exactly one of the three components is left blank. The model will then fill this gap with various entities and assess their likelyhood. We take the guessed triples with the highest probability to retrieve the most reasonable predictions, according to the model.
    """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Scoring potential bus and tram connections of nearby stops.""")
    return


@app.cell
def _(graph, learning, prediction, testing, validation):
    if False:
        _loaded_model, _loaded_triples = learning.load_model("trained_models/RotatE")
        _stops_with_neighbours = graph.get_nearby_stops()

        _pred = prediction.PredictionMachine(_loaded_model, _loaded_triples, validation, testing)
        #_pred.score_potential_connections(_stops_with_neighbours)

        _pred.predict_connection_frequency(_stops_with_neighbours)
    return


@app.cell
def _(mo):
    mo.md(r"""Scoring potential subway connections between""")
    return


@app.cell
def _(learning, prediction, testing, validation):
    if False:
        _loaded_model, _loaded_triples = learning.load_model("trained_models/RotatE")

        _pred = prediction.PredictionMachine(_loaded_model, _loaded_triples, validation, testing)
        _prediction_dataframe = _pred.predict_component(head="at:49:1530:0:4", rel="TRAM_CONNECTS_TO").sort_values(by=['score'], ascending=True)
        _prediction_dataframe["tail_label"].tolist()
    return


if __name__ == "__main__":
    app.run()
