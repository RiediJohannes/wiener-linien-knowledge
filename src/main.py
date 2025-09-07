import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium", app_title="", css_file="styles/notebook.css")


@app.cell(hide_code=True)
def imports():
    import marimo as mo

    import src.components.graph as graph
    import src.components.geo_spatial as geo
    import src.components.presentation as present
    return geo, graph, mo, present


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
    ### Finding representative cluster roots

    So far, we have organized related stops into clusters that are represented by a root node with the tag `ClusterStop`. However, the choice of this cluster stop was merely arbitrary. Now, we want to determine a rightful representative by choosing the stop with the most traffic among all stops in the cluster.

    Therefore, we count the stop times scheduled for each cluster and make the busiest stop the new cluster stop.
    """
    )
    return


@app.cell
def reassign_cluster_stops(graph):
    _operation = """
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

    print("Re-assigning cluster stops...")
    _affected_rows = graph.execute_operation_returning_count(_operation)
    print(f"Affected {_affected_rows} nodes")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Move transport-related relationships to cluster stop

    Now that we have created clusters with the busiest stop in them as their root node (the `ClusterStop`), we move all `:STOPS_AT` relationships of the other nodes in the cluster to that `ClusterStop` instead.

    **WARNING**: Be aware that this is a very expensive operation and might take a while to finish execution.
    """
    )
    return


@app.cell
def _(graph):
    _expensive_operation = """
    MATCH (:Trip)-[at:STOPS_AT]->(s:Stop)-[:IN_CLUSTER]->(c:Stop:ClusterStop)
    WHERE s.id <> c.id
    CALL (at, c) {
      CALL apoc.refactor.to(at, c) YIELD output
      RETURN count(output) AS refactoredCount
    } IN TRANSACTIONS OF 10000 ROWS
    RETURN sum(refactoredCount) AS movedRelationships
    """

    print("Moving over all :STOPS_AT relationships to cluster roots...")
    _response = graph.execute_batched_query(_expensive_operation)
    _moved_relationships: int = int(_response[0][0]) if _response else 0
    print(f"Moved a total of {_moved_relationships} :STOPS_AT relationships")
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

    print("Calculating the average position")
    _summary = graph.execute_operation(_operation)
    print(f"Set {_summary.counters.properties_set} properties")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Organizing City Districts

    ### Determine neighbouring subdistricts

    To better describe the geographic relationship between subdistricts, we determine which subdistricts border each other and store this in our knowledge graph.
    """
    )
    return


@app.cell
def _(geo, graph):
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

    print("Creating ':NEIGHBOURS' relationships...")
    _summary = graph.execute_operation(_operation, neighbours_dict=_neighbours)
    print(f"Created {_summary.counters.relationships_created} relationships")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Pre-calculate population density of all subdistricts

    Since a subdistrict's area is given in m², we multiply the population count by one million to get the population density in people/km².

    ```cypher
    MATCH (d:SubDistrict)
    SET d.density = 1_000_000 * d.population / d.area;
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
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

    Adding these labels is mostly done for easier retrieval of certain stops later.

    **Bus stops:**
    ```cypher
    MATCH (s:Stop)<-[:STOPS_AT]-(:BusTrip)
    WHERE NOT (s:BusStop)
    SET s: BusStop
    ```
    **Tram stops:**
    ```cypher
    MATCH (s:Stop)<-[:STOPS_AT]-(:TramTrip)
    WHERE NOT (s:TramStop)
    SET s: TramStop
    ```
    **Subway stops/stations:**
    ```cypher
    MATCH (s:Stop)<-[:STOPS_AT]-(:SubwayTrip)
    WHERE NOT (s:SubwayStation)
    SET s: SubwayStation
    ```
    **Collect all stops that are used by some trip (after redirecting trips to cluster roots)**
    ```cypher
    MATCH (s:Stop&(BusStop|TramStop|SubwayStation))
    WHERE NOT (s: InUse)
    SET s: InUse
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
def _(graph):
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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Knowledge Graph Embeddings

    Now, we are ready for this project's main goal, which is predicting missing connections in Vienna's public transport network. As the wording suggests, this becomes a classic **link prediction problem** in our knowledge graph. A key tool to tackle such problems are knowledge graph embeddings. In this chapter, we will use the popular KG embedding library **PyKEEN**.

    ## Training Triples Generation

    Since link prediction works on the basis of **triples** of the form $(h,r,t)$ ($h:$ head, $r:$ relation, $t:$ tail), we need to derive and extract meaningful triples from our knowledge graph. This training data should include all reasonable information about the domain that is considered relevant for planning new transport connections between existing stops.

    ### Existing Transit Network

    ```cypher
    // Existing transit connections with mode of transport
    MATCH (s1:Stop)-[conn:BUS_CONNECTS_TO|TRAM_CONNECTS_TO|SUBWAY_CONNECTS_TO]->(s2:Stop)
    WHERE conn.yearly > 4 * 365
    RETURN s1.id as head, type(conn) as rel, s2.id as tail
    ```

    We exclude connections that happen fewer than four times a day (<1460 times a year) on average, since these mostly represent temporary reroutings due to construction work or special trips for operational reasons (e.g. ending a trip at a depot at the end of a day). Remember, this does not mean that e.g. a single bus line connects the two stops four times a day, but _ALL_ direct bus connections exceed four operations a day _on average_.

    ```cypher
    // Routes serving stops
    MATCH (t:Trip)-[:OPERATING_ON]->(ser:Service)
    WITH t, sum(ser.operations_per_year) as operations
    MATCH (r:Route)<-[:PART_OF_ROUTE]-(t)-[:STOPS_AT]->(s:Stop)
    WITH r.short_name as route_name, s, sum(operations) as trip_count
    WHERE trip_count >= 365
    RETURN route_name as head, 'SERVES' as rel, s.id as tail

    // Mode of transport of each route
    MATCH (r:Route)
    RETURN DISTINCT r.short_name as head, 'IS_MODE_OF_TRANSPORT' as rel,
    CASE r.type
      WHEN 0 THEN 'TRAM'
      WHEN 1 THEN 'SUBWAY'
      WHEN 2 THEN 'TRAIN'
      WHEN 3 THEN 'BUS'
      ELSE 'SPECIAL'
    END AS tail
    ```

    Similarly as for the connection relations before, we discard some noise. In this case, we only consider a route to serve a particular stop if it stops there at least once per day on average.

    ### Geographic Location

    ```cypher
    // Stop locations in/nearby subdistricts
    MATCH (s:Stop:InUse)-[loc:LOCATED_NEARBY|LOCATED_IN]->(d:SubDistrict)
    WITH s, loc, d.district_num + '-' + d.sub_district_num as subdistrict
    RETURN s.id as head, type(loc) as rel, subdistrict as tail

    // Geographic proximity (without direct connection)
    ...
    ```

    ### Population Density

    ```
    MATCH (d:SubDistrict)
    WITH d, 
      CASE 
        WHEN d.population_density > 8000 THEN 'VERY_HIGH_DENSITY'
        WHEN d.population_density > 5000 THEN 'HIGH_DENSITY'  
        WHEN d.population_density > 2000 THEN 'MEDIUM_DENSITY'
        ELSE 'LOW_DENSITY'
      END as density_category
    RETURN d.id, 'HAS_DENSITY', density_category

    // Stops in high-demand areas
    MATCH (s:Stop)-[:LOCATED_NEARBY]->(d:SubDistrict)
    WHERE d.population_density > 5000
    RETURN s.id, 'IN_HIGH_DEMAND_AREA', 'HIGH_DEMAND'
    ```

    ### Level of Service

    ```cypher
    // High-frequency connections
    MATCH (s1:Stop)-[conn:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]->(s2:Stop)
    WHERE conn.yearly > 10000  // Adjust threshold
    RETURN s1.id, 'HIGH_FREQUENCY_TO', s2.id

    // Transfer hubs (stops with many connections)
    MATCH (s:Stop)
    WITH s, size((s)-[:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]-()) as connection_count
    WHERE connection_count > 5
    RETURN s.id, 'IS_HUB', 'TRANSFER_HUB'
    ```

    stop_in_high_density_area + SHOULD_CONNECT_TO ≈ stop_in_high_density_area
    stop_A + GEOGRAPHICALLY_CLOSE ≈ stop_B → Maybe they should be connected
    district + HIGH_DENSITY ≈ DENSE_AREA → Stops in dense areas need good connections
    stop + IS_WELL_CONNECTED ≈ HUB → Hubs should connect to underserved areas
    """
    )
    return


@app.cell
def _(graph):
    def generate_training_triples():
        queries = [
            # 1. Core transit network
            """
            MATCH (s1:Stop)-[conn:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]->(s2:Stop)
            RETURN s1.id as head, type(conn) as relation, s2.id as tail
            """,
        
            # 2. Geographic context
            """
            MATCH (s:Stop)-[:LOCATED_NEARBY]->(d:SubDistrict)
            RETURN s.id as head, 'LOCATED_IN' as relation, d.id as tail
            """,
        
            # 3. Demand indicators
            """
            MATCH (d:SubDistrict)
            WITH d, CASE 
                WHEN d.population_density > 8000 THEN 'VERY_HIGH_DENSITY'
                WHEN d.population_density > 5000 THEN 'HIGH_DENSITY'
                WHEN d.population_density > 2000 THEN 'MEDIUM_DENSITY'
                ELSE 'LOW_DENSITY' END as category
            RETURN d.id as head, 'HAS_DENSITY' as relation, category as tail
            """,
        
            # 4. Service patterns
            """
            MATCH (s:Stop)
            WITH s, size((s)-[:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]-()) as conn_count
            WHERE conn_count > 3
            RETURN s.id as head, 'IS_WELL_CONNECTED' as relation, 'HUB' as tail
            """,
        
            # 5. Geographic proximity without connection (important!)
            """
            MATCH (s1:Stop)-[:LOCATED_NEARBY]->(d1:SubDistrict)
            MATCH (s2:Stop)-[:LOCATED_NEARBY]->(d2:SubDistrict) 
            WHERE d1 <> d2 AND distance(s1, s2) < 800
            AND NOT EXISTS((s1)-[:SUBWAY_CONNECTS_TO|BUS_CONNECTS_TO|TRAM_CONNECTS_TO]-(s2))
            RETURN s1.id as head, 'NEARBY_UNCONNECTED' as relation, s2.id as tail
            """,
        ]
    
        all_triples = []
        for query in queries:
            response = graph.execute_query(query)
            triples = [(rec['head'], rec['relation'], rec['tail']) for rec in response]
            all_triples.extend(triples)
            print(f"Added {len(triples)} triples from query")
    
        return all_triples
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Model Training

    Next comes the setup for model training. We will train two models using different KG embedding algorithms and comparatively analyze their results.

    ### Model 1: RotatE


    """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(graph, mo, present):
    _stops = graph.get_stops(id_list=["at:49:1115:0:2", "at:49:124:0:2"])
    #_stops = graph.get_stop_cluster(stop_name='Possingergasse')
    #_stops = graph.get_stops_for_subdistrict(10, 1)

    # tiles='https://{s}.tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=2006ee957e924a28a24e5be254c48329',
    # attr='&copy; <a href="http://www.thunderforest.com/">Thunderforest</a>, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    transport_map = present.TransportMap(lat=48.2202331, lon=16.3796424, zoom=11)
    transport_map.add_stops(_stops)

    mo.iframe(transport_map.as_html(), height=650)
    return


if __name__ == "__main__":
    app.run()
