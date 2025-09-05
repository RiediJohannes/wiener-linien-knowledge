import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium", app_title="", css_file="styles/notebook.css")


@app.cell(hide_code=True)
def imports():
    import marimo as mo

    import src.components.graph as graph
    import src.components.geo_spatial as geo
    import src.components.presentation as present
    return geo, graph, mo


@app.cell(hide_code=True)
def project_heading(mo):
    mo.callout(mo.md("""
    # 🚊 Wiener Linien Knowledge Graph 🚋
    """), kind='danger')
    return


@app.cell(hide_code=True)
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

    ## Merging geographically related stops
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
    print(f"""\nOperation successful:
    Created {_summary.counters.relationships_created} relationships
    Added {_summary.counters.labels_added} labels""")
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
def detect_station_exits(graph):
    print("Merging related clusters...")
    _updated_clusters: int = graph.merge_related_clusters()
    print(f"Updated {_updated_clusters} stop clusters")

    # Since this is such a complex operation, we verify that everything worked as expected
    print("\nVerifying integrity...")
    _query = """
    MATCH (s:Stop)-[:IN_CLUSTER]->(a), (s:Stop)-[:IN_CLUSTER]->(b)
    WHERE a.id <> b.id
    RETURN s.id, a.id, b.id
    """
    _result = graph.execute_query(_query)
    if not _result:
        print("✅ No node is in two clusters")

    """
    MATCH (s:Stop)-[:IN_CLUSTER]->(p:Stop)
    WHERE NOT apoc.label.exists(p, "ClusterStop")
    RETURN s.id, p.id
    """
    _result = graph.execute_query(_query)
    if not _result:
        print("✅ Every cluster parent has the label 'ClusterStop'")   

    """
    MATCH (s:ClusterStop)-[:IN_CLUSTER*1..5]-(p:ClusterStop)
    WHERE s.id <> p.id
    RETURN s.id, p.id
    """
    _result = graph.execute_query(_query)
    if not _result:
        print("✅ No cluster has more than one ClusterStop")

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Finding representative cluster parents

    So far, we have organized related stops into clusters that are represented by a parent node with the tag `ClusterStop`. However, the choice of this cluster stop was merely arbitrary. Now, we want to determine a rightful representative by choosing the stop with the most traffic among all stops in the cluster.

    Therefore, we count the stop times scheduled for each cluster and make the busiest stop the new cluster stop.
    """
    )
    return


@app.cell
def reassign_cluster_stops(graph):
    _operation = """
    // For each cluster, rank members by usage
    MATCH (stop:Stop)-[:IN_CLUSTER]->(parent:ClusterStop)
    OPTIONAL MATCH (stop)<-[:AT_STOP]-(st:StopTime)
    WITH parent, stop, count(st) AS usageCount
    ORDER BY parent, usageCount DESC, stop.name DESC, stop.id ASC
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

    print("Re-assigning cluster stops...")
    _affected_rows = graph.execute_operation_returning_count(_operation)
    print(f"Affected {_affected_rows} nodes")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Move transport-related relationships to cluster stop

    Now that we have created clusters with the busiest stop in them as their root node (the `ClusterStop`), we move all `:AT_STOP` relationships of the other nodes in the cluster to that `ClusterStop` instead.
    """
    )
    return


@app.cell
def _(graph):
    _expensive_operation = """
    MATCH (time:StopTime)-[at:AT_STOP]->(s:Stop)-[:IN_CLUSTER]->(c:ClusterStop)
    WHERE s.id <> c.id
    CALL (at, c) {
      CALL apoc.refactor.to(at, c) YIELD output
      RETURN count(output) AS refactoredCount
    } IN TRANSACTIONS OF 10000 ROWS
    RETURN sum(refactoredCount) AS movedRelationships
    """

    _moved_relationships: int = graph.execute_operation_returning_count(_expensive_operation)
    print(f"Moved a total of {_moved_relationships} :AT_STOP relationships")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""This concludes our efforts to find clusters of highly related stops and create structures that reflect this close relationship. The new data model greatly simplifies measuring the true importance of certain stops, which will aid us in predicting missing links in the public transport network.""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Matching stops to districts
    Our next goal is to match each stop to Viennese registration districts they serve in order to get a rough grasp on how many people can benefit them. 
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Next, we use the geographic coordinates of the stops to match each stop to the Viennese registration districts that either contain the stop or are reasonably close to it.""")
    return


@app.cell
def match_stops_with_districts(geo, graph):
    _stops = graph.get_stops()
    print(f"Queried {len(_stops)} stops from the graph")
    _subdistricts = graph.get_subdistricts()
    print(f"Queried {len(_subdistricts)} subdistricts from the graph")

    _stops_within_districts = geo.match_stops_to_subdistricts(_stops, _subdistricts, buffer_metres = 20)
    _summary = graph.connect_stop_to_subdistricts(_stops_within_districts, 'LOCATED_IN')
    print(f"Created {_summary.counters.relationships_created} LOCATED_IN relationships")

    _stops_close_to_districts = geo.match_stops_to_subdistricts(_stops, _subdistricts, buffer_metres = 500)
    _summary = graph.connect_stop_to_subdistricts(_stops_close_to_districts, 'LOCATED_NEARBY')
    print(f"Created {_summary.counters.relationships_created} LOCATED_NEARBY relationships")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Additionally, we define:
    > For any cluster $C$, if **there exists** a stop $s \in C$ such that $s$ is **located nearby** a subdistrict $d$, then the cluster stop $c \in C$ is also considered nearby $d$.

    As well as:
    > For any cluster $C$, if **at least half** of the stops in $C$ are **located in** a subdistrict $d$, then the cluster stop $c \in C$ is also considered to be located in $d$.

    This can be solved using two simple cypher queries:
    ```cypher
    MATCH (s:Stop)-[:IN_CLUSTER]->(c:ClusterStop),
          (s)-[:LOCATED_NEARBY]->(d:SubDistrict)
    WHERE NOT (c)-[:LOCATED_NEARBY]->(d)
    MERGE (c)-[:LOCATED_NEARBY]->(d);
    ```

    ```cypher
    MATCH (c:ClusterStop)<-[:IN_CLUSTER]-(s:Stop)
    WITH c, count(s) as clusterSize
    MATCH (c)<-[:IN_CLUSTER]-(s:Stop)-[:LOCATED_IN]->(d:SubDistrict)
    WHERE NOT (c)-[:LOCATED_IN]->(d)
    WITH c, d, count(s) as stopsInDistrict, clusterSize
    WHERE stopsInDistrict >= clusterSize / 2 OR stopsInDistrict >= 3
    MERGE (c)-[:LOCATED_IN]->(d)
    ```
    """
    )
    return


@app.cell
def functions_entails_vicinity(graph):
    _operation = """
    MATCH (s:Stop)-[:IN_CLUSTER]->(c:ClusterStop),
          (s)-[:LOCATED_NEARBY]->(d:SubDistrict)
    WHERE NOT (c)-[:LOCATED_NEARBY]->(d)
    MERGE (c)-[:LOCATED_NEARBY]->(d);
    """

    _summary = graph.execute_operation(_operation)
    print(f"Created {_summary.counters.relationships_created} LOCATED_NEARBY relationships")


    _operation = """
    MATCH (c:ClusterStop)<-[:IN_CLUSTER]-(s:Stop)
    WITH c, count(s) as clusterSize
    MATCH (c)<-[:IN_CLUSTER]-(s:Stop)-[:LOCATED_IN]->(d:SubDistrict)
    WHERE NOT (c)-[:LOCATED_IN]->(d)
    WITH c, d, count(s) as stopsInDistrict, clusterSize
    WHERE stopsInDistrict >= clusterSize / 2 OR stopsInDistrict >= 3
    MERGE (c)-[:LOCATED_IN]->(d)
    """

    _summary = graph.execute_operation(_operation)
    print(f"Created {_summary.counters.relationships_created} LOCATED_IN relationships")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Lastly, we calculate the average position of all stops in a cluster and store that as the position of the overall cluster in the cluster stop for display purposes.""")
    return


@app.cell
def _(graph):
    _operation = """
    MATCH (s:Stop)-[:IN_CLUSTER]->(c:ClusterStop)
    WITH c, avg(s.lat) AS cluster_lat, avg(s.lon) AS cluster_lon
    SET c.cluster_lat = cluster_lat,
        c.cluster_lon = cluster_lon
    """

    _summary = graph.execute_operation(_operation)
    print(f"Set {_summary.counters.properties_set} properties")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Pre-calculate population density of all subdistricts:

    Since a subdistrict's area is given in m², we multiply the population count by one million to get the population density in people/km².

    ```cypher
    MATCH (d:SubDistrict)
    SET d.density = 1_000_000 * d.population / d.area;
    ```

    ### Adding Labels

    Classify exceptions to the regularly scheduled service into additional and removed service
    ```cypher
    MATCH (ex:ServiceException)
    WHERE ex.exception_type = 1
    SET ex: AddedService;

    MATCH (ex:ServiceException)
    WHERE ex.exception_type = 2
    SET ex: RemovedService;
    ```

    Label trips according to their mode of transport:
    ```cypher
    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 0  // enum value for trams or light rail
    SET t: TramTrip

    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 1  // enum value for subways
    SET t: SubwayTrip

    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 2  // enum value for trains (heavy rail)
    SET t: TrainTrip

    MATCH (t:Trip)-[:PART_OF_ROUTE]->(r:Route)
    WHERE r.type = 3  // enum value for buses
    SET t: BusTrip
    ```

    ### Classify stops

    **Bus stops:**
    ```cypher
    MATCH (s:Stop)<-[:AT_STOP]-(:StopTime)-[:DURING_TRIP]->(:BusTrip)
    WHERE NOT (s:BusStop)
    SET s: BusStop
    ```
    **Tram stops:**
    ```cypher
    MATCH (s:Stop)<-[:AT_STOP]-(:StopTime)-[:DURING_TRIP]->(:TramTrip)
    WHERE NOT (s:TramStop)
    SET s: TramStop
    ```
    **Subway stops/stations:**
    ```cypher
    MATCH (s:Stop)<-[:AT_STOP]-(:StopTime)-[:DURING_TRIP]->(:SubwayTrip)
    WHERE NOT (s:SubwayStation)
    SET s: SubwayStation
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Determine connections between stops

    Our next main goal is to detect the **frequency of a direct public transport connections** from one stop to another. In the end, we would like every stop `s` in our graph to contain relationships of the form `(s)-[:X_CONNECTS_TO]->(t)` for every stop `t` it has a _direct_ connection to at least once per year, where `X` is replaced by the mode of transport ($X \in \{\texttt{BUS}, \texttt{TRAM}, \texttt{SUBWAY}\}$).

    For that, we first calculate an approximation of the number of times a single trip is operated per year: 
    ```cypher
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
    ```

    The value obtained by this query is not the exact number of trips in the year 2024, since it only considers the number of weeks in a year instead of the exact number of Mondays, Tuesdays, etc. in the year 2024. However, the numbers should be within roughly $2\%$ of the true value and basically represent a year-on-year average for each trip.
    """
    )
    return


@app.cell
def calculate_trips_per_year(graph):
    _operation = """
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

    print("Calculating operations per year for every trip...")
    _summary = graph.execute_operation(_operation)
    print(f"Calculated and (re)set {_summary.counters.properties_set} properties")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Next, we find every pair of stops $(s1, s2)$ such that some trip $t$ directly connects $s1$ to $s2$ (in that order). We aggregate over all such trips $t$ and sum their operations per year to get the total number of direct connections $s1 \to s2$ in a year.  
    Note that such trips do not necessarily move between the exact stops $s1$ and $s2$ but between two stops within their (separate) clusters, as we have previously moved all `:AT_STOP` relationships to a cluster's root node.

    To determine a fine-grained level of service, we differentiate between modes of transport (bus, tram, subway) when creating these connection relationships, and we also store the exact number of operations per year for that connection in the `yearly` property of that relationship.
  
    ```cypher
    // Consider each pair of stops that appears consecutively in some trip t
    MATCH (t:Trip:$type_of_trip)<-[:DURING_TRIP]-(st1:StopTime)-[:AT_STOP]->(s1:Stop),
          (t)<-[:DURING_TRIP]-(st2:StopTime)-[:AT_STOP]->(s2:Stop)
    WHERE s1.id <> s2.id AND st2.stop_sequence = st1.stop_sequence + 1
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

    - `s1.id <> s2.id` -- This guarantees that we ignore trips within a cluster to prevent overcounting, since our bundling of stops may have lead to sequential stops being part of the same cluster (and thus the `:AT_STOP` relationship was redirected to the same root node) .
    - `st2.stop_sequence = st1.stop_sequence + 1` -- This ensures that we only consider _direct_ connections between stops.
    """
    )
    return


@app.cell
def _(graph):
    _operation = """
    // Consider each pair of stops that appears consecutively in some trip t
    MATCH (t:Trip:{type_of_trip})<-[:DURING_TRIP]-(st1:StopTime)-[:AT_STOP]->(s1:Stop),
          (t)<-[:DURING_TRIP]-(st2:StopTime)-[:AT_STOP]->(s2:Stop)
    WHERE s1.id <> s2.id AND st2.stop_sequence = st1.stop_sequence + 1
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
    return


@app.cell
def _(graph, mo):
    import folium

    _stops = graph.get_stops()
    #_stops = graph.get_stop_cluster(stop_name='Possingergasse')
    #_stops = graph.get_stops_for_subdistrict(16, 10)

    # Create a folium map centered on the mean of the coordinates
    map = folium.Map(
        tiles=None,
        #tiles='https://{s}.tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=2006ee957e924a28a24e5be254c48329',
        #attr='&copy; <a href="http://www.thunderforest.com/">Thunderforest</a>, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        location=[48.2202331, 16.3796424],
        zoom_start=11
    )

    # Add map as base layer
    folium.TileLayer("https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",
                    attr='&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a> &copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                    name="Stadiamaps", overlay=False).add_to(map)

    # Add layers to show/hide markers
    stop_marks = folium.FeatureGroup(name="Stop markers", control=True, show=True).add_to(map)
    cluster_marks = folium.FeatureGroup(name="Cluster markers", control=True, show=False).add_to(map)
    folium.LayerControl().add_to(map)

    # Create panes to put differnt markers on different z-indexes
    folium.map.CustomPane("stops", z_index=600).add_to(map)
    folium.map.CustomPane("clusters", z_index=450).add_to(map)

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
            tooltip=stop.id,
            pane="stops"
        ).add_to(stop_marks)

        if stop.is_cluster:
            folium.CircleMarker(
                location=[stop.cluster_lat, stop.cluster_lon],
                radius=15,
                color="violet",
                fill=True,
                fill_opacity=0.2,
                opacity=0.1,
                pane="clusters"
            ).add_to(cluster_marks)

    # Save the map to an HTML file
    #map.save("stops_map.html")

    mo.iframe(map._repr_html_(), height=650)
    return


if __name__ == "__main__":
    app.run()
