from neo4j import GraphDatabase, ResultSummary, Record

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "")

# Create a driver instance
driver = GraphDatabase.driver(URI, auth=AUTH)


class Stop:
    def __init__(self, stop_id: str, latitude: float, longitude: float, name: str):
        self.id: str = stop_id
        self.lat: float = latitude
        self.lon: float = longitude
        self.name: str = name

class ClusterStop(Stop):
   def __init__(self, stop_id: str, latitude: float, longitude: float, name: str, cluster_lat: float, cluster_lon: float, cluster_points: list[list[float]]):
       super().__init__(stop_id, latitude, longitude, name)
       self.cluster_lat: float = cluster_lat
       self.cluster_lon: float = cluster_lon
       if cluster_points:
           self.cluster_points: list[tuple[float, float]] = [(point[0], point[1]) for point in cluster_points]
       else:
           self.cluster_points = []

class SubDistrict:
    def __init__(self, district_num: int, subdistrict_num: int, population: int, area: float, shape: str):
        self.id: str = f"{district_num}-{subdistrict_num}"
        self.population: int = population
        self.area: float = area
        self.shape: str = shape


def import_city_data():
    execute_operation("CREATE INDEX IF NOT EXISTS FOR (s:SubDistrict) ON (s.district_num);")
    execute_operation("CREATE INDEX IF NOT EXISTS FOR (s:SubDistrict) ON (s.sub_district_num);")

    operation = """
    LOAD CSV WITH HEADERS FROM 'file:///city/vienna_population.csv' AS row
    FIELDTERMINATOR ';' // specify the custom delimiter
    MERGE (s:SubDistrict {
        district_num: toInteger(substring(row.DISTRICT_CODE, 1, 2)),
        sub_district_num: toInteger(substring(row.SUB_DISTRICT_CODE, 3, 2))
      })
      SET s.population = toInteger(row.WHG_POP_TOTAL)
    """
    summary = execute_operation(operation)
    if summary is not None:
        print(f"Successfully imported population data. Added {summary.counters.nodes_created} nodes.")
    else:
        return

    operation = """
    LOAD CSV WITH HEADERS FROM 'file:///city/registration_districts_names.csv' AS row
    FIELDTERMINATOR ';' // specify the custom delimiter
    MERGE (s:SubDistrict {
        district_num: toInteger(substring(row.DISTRICT_CODE, 1, 2)),
        sub_district_num: toInteger(substring(row.SUB_DISTRICT_CODE_VIE, 3, 2))
      })
      SET s.name = row.NAME_VIE
    """
    summary = execute_operation(operation)
    if summary is not None:
        print(f"Successfully imported registration district names. (Re)set {summary.counters.properties_set} properties.")
    else:
        return

    operation = """
    LOAD CSV WITH HEADERS FROM 'file:///city/registration_districts_shapes.csv' AS row
    MERGE (s:SubDistrict {
        district_num: toInteger(row.BEZNR),
        sub_district_num: toInteger(row.ZBEZNR)
      })
      SET s.area = toFloat(row.FLAECHE),
          s.shape = row.SHAPE
    """
    summary = execute_operation(operation)
    if summary is not None:
        print(f"Successfully imported registration district coordinates. (Re)set {summary.counters.properties_set} properties.")
    else:
        return

def get_subdistricts() -> list[SubDistrict]:
    query = """
    MATCH (s:SubDistrict)
    RETURN s.district_num as district,
           s.sub_district_num as subdistrict,
           s.population as population,
           s.area as area,
           s.shape as shape;
    """
    results = execute_query(query)

    return [SubDistrict(
        record["district"],
        record["subdistrict"],
        record["population"],
        record["area"],
        record["shape"]
    ) for record in results]

def get_stops(*, id_list: list[str] = None) -> list[Stop] | None:
    where_clause = f"WHERE s.id IN [\"{'", "'.join(id_list)}\"]" if id_list else ""

    base_query = f"""
    MATCH (s:Stop)
    {where_clause}
    """

    query = _finalize_stop_query(base_query, "s")
    response = execute_query(query)
    return _parse_stops_from_response(response)

def get_stop_cluster(*, stop_id = None, stop_name = None) -> list[Stop] | None:
    if stop_id is None and stop_name is None:
        return get_stops()

    where_clause: str = f"start.id = '{stop_id}'" if stop_id is not None else f"start.name = '{stop_name}'"

    base_query = f"""
    MATCH (start:Stop)
    WHERE {where_clause}
    OPTIONAL MATCH (start)-[:IN_CLUSTER]->(c:Stop)<-[:IN_CLUSTER]-(d:Stop)
    
    WITH start, c, collect(d) AS others
    UNWIND [start, c] + others AS node
    WITH node
    WHERE node IS NOT NULL
    """

    query = _finalize_stop_query(base_query, "node")
    response = execute_query(query)
    return _parse_stops_from_response(response)

def get_stops_for_subdistrict(district_code: int, subdistrict_code: int, only_stops_within = False) -> list[Stop] | None:
    base_query = f"""
    MATCH (d:SubDistrict)
    WHERE d.district_num = $dist_num AND d.sub_district_num = $subdist_num
    MATCH (s:Stop)-[:{"LOCATED_IN" if only_stops_within else "LOCATED_NEARBY"}]->(d)
    """

    query = _finalize_stop_query(base_query, "s")
    response = execute_query(query, dist_num=district_code, subdist_num=subdistrict_code)
    return _parse_stops_from_response(response)


def cluster_stops(stop_clusters: list[list[str]]) -> ResultSummary | None:
    if stop_clusters and len(stop_clusters[0]) > 0:
        operation = """
        UNWIND $cluster_list AS cluster
          UNWIND cluster AS stopId
            MATCH (stop:Stop {id: stopId})
            WITH cluster, collect(stop) AS clusterMembers
            WITH cluster, clusterMembers, clusterMembers[0] AS mainStop
            // Mark the main stop as the representative for the cluster
            SET mainStop:ClusterStop
            // Connect all stops in the cluster to the main stop
            FOREACH (member IN clusterMembers | MERGE (member)-[:IN_CLUSTER]->(mainStop))
        """
        return execute_operation(operation, cluster_list=stop_clusters)

    return None

def merge_related_clusters() -> int:
    operation = """
    // Find stops that are related according to their ID but not in the same cluster
    MATCH (left:Stop)
    WITH left, split(left.id, ':') as parts
    WITH left, parts[0] + ':' + parts[1] + ':' + parts[2] + ':' + parts[3] + ':' as prefix
    MATCH (right:Stop)
    WHERE right.id STARTS WITH prefix
      AND right.id <> left.id
      AND NOT (left)-[:IN_CLUSTER]->()<-[:IN_CLUSTER]-(right)
    
    // Check if they already have a cluster parent
    OPTIONAL MATCH (left)-[:IN_CLUSTER]->(leftParent:Stop)
    OPTIONAL MATCH (right)-[:IN_CLUSTER]->(rightParent:Stop)
    
    // Handle the four different cases to put the two nodes into the same cluster
    WITH left, right, leftParent, rightParent
    CALL apoc.do.case([
        // Case 1: left has parent, right is orphan
        leftParent IS NOT NULL AND rightParent IS NULL,
        'MERGE (right)-[:IN_CLUSTER]->(leftParent)
         RETURN 1 as result',
        
        // Case 2: right has parent, left is orphan
        leftParent IS NULL AND rightParent IS NOT NULL,
        'MERGE (left)-[:IN_CLUSTER]->(rightParent)
         RETURN 2 as result',
        
        // Case 3: both are orphans - make right the parent
        leftParent IS NULL AND rightParent IS NULL,
        'SET right:ClusterStop
         MERGE (left)-[:IN_CLUSTER]->(right)
         RETURN 3 as result',
        
        // Case 4: both have parents - merge clusters
        leftParent IS NOT NULL AND rightParent IS NOT NULL,
        'REMOVE leftParent:ClusterStop
         WITH leftParent, rightParent
         MATCH (leftParent)<-[rel:IN_CLUSTER]-()
         CALL apoc.refactor.to(rel, rightParent) YIELD output
         RETURN 4 as result'
      ],
      '', // else clause (will never be reached)
      {left: left, right: right, leftParent: leftParent, rightParent: rightParent}
    ) YIELD value
    
    RETURN count(*) as mergedClusters
    """

    return execute_operation_returning_count(operation)

def connect_stop_to_subdistricts(stops_with_districts: list[tuple[str, list[str]]], relation_name: str) -> ResultSummary | None:
    # Prepare the data as a list of dictionaries for neo4j's UNWIND operation
    stop_district_pairs = [
        {"stop_id": stop_id, "dist_code": int(dist_code), "subdist_code": int(subdist_code)}
        for stop_id, subdistricts in stops_with_districts
        for combined_id in subdistricts
        for dist_code, subdist_code in [combined_id.split('-')]  # Split combined subdistrict identifier
    ]

    if not stop_district_pairs or not relation_name:
        return None

    query = f"""
        UNWIND $stop_district_pairs AS matchup
        MATCH (s:Stop {{id: matchup.stop_id}})
        MATCH (d:SubDistrict {{district_num: matchup.dist_code, sub_district_num: matchup.subdist_code}})
        MERGE (s)-[:{relation_name}]->(d)
        """
    return execute_operation(query, stop_district_pairs=stop_district_pairs)


def query_triples(names_queries: dict[str, str]) -> list[tuple[str, str, str]]:
    triples = []
    for name, query in names_queries.items():
        print(f"Running query '{name}'...")
        records = execute_query(query)
        triples.extend([(triple["head"], triple["rel"], triple["tail"]) for triple in records])
        print(f"✅ Received {len(records)} triples")

    return triples

def execute_operation(cypher_operation, **params) -> ResultSummary | None:
    """
    Runs a cypher query that executes some create/update/delete operations on the connected neo4j instance.

    This function does not return any objects retrieved through a "retrieve" operation, it simply returns
    a summary about the actions performed.
    """

    try:
        with driver.session() as session:
            result = session.run(cypher_operation, **params)
            return result.consume()
    except Exception as e:
        print(f"Database operation failed with error: {e}")
        return None

def execute_query(cypher_query, **params) -> list[Record]:
    """
    Runs a cypher query that is meant to return some data on the connected neo4j instance.
    """

    try:
        result = driver.execute_query(cypher_query, **params)
        return result.records
    except Exception as e:
        print(f"Database query failed with error: {e}")
        return []

def execute_batched_operation(batched_operation, **params) -> ResultSummary | None:
    """
    Runs a cypher operation that is meant to update the database without a managed transaction.
    This is required for operations that are split into multiple batches using the IN TRANSACTIONS
    OF X ROWS cypher feature.
    """

    try:
        with driver.session() as session:
            result = session.run(batched_operation, **params)
            return result.consume()
    except Exception as e:
        print(f"Database operation failed with error: {e}")
        return None

def execute_batched_query(batched_query, **params) -> list[Record] | None:
    try:
        with driver.session() as session:
            result = session.run(batched_query, **params)
            return [record for record in result]
    except Exception as e:
        print(f"Database query failed with error: {e}")
        return None

def execute_operation_returning_count(cypher_query_returning_count, **params) -> int:
    """
    Runs a cypher query that executes some create/update/delete operations on the connected neo4j instance
    and returns an integer, most commonly the number of affected rows.
    """
    result = execute_query(cypher_query_returning_count, **params)
    return int(result[0][0]) if result else 0


def _parse_stops_from_response(response: list[Record]) -> list[Stop]:
    stops: list[Stop] = []
    for record in response:
        if record["is_cluster"]:
            stops.append(ClusterStop(record["id"], record["lat"], record["lon"], record["name"],
                                     record["cluster_lat"], record["cluster_lon"], record["cluster_points"]))
        else:
            stops.append(Stop(record["id"], record["lat"], record["lon"], record["name"]))

    return stops

def _finalize_stop_query(base_query: str, stop_variable: str) -> str:
    return f"""
    {base_query}
    WITH {stop_variable}
    OPTIONAL MATCH ({stop_variable})<-[:IN_CLUSTER]-(child:Stop)
    WITH {stop_variable}, collect([child.lat, child.lon]) as cluster_points
    RETURN DISTINCT
        {stop_variable}.id as id,
        {stop_variable}.lat as lat,
        {stop_variable}.lon as lon,
        {stop_variable}.name as name,
        apoc.label.exists({stop_variable}, "ClusterStop") as is_cluster,
        {stop_variable}.cluster_lat as cluster_lat,
        {stop_variable}.cluster_lon as cluster_lon,
        cluster_points;
    """