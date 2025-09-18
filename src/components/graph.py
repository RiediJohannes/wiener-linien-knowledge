import neo4j.graph
from neo4j import GraphDatabase, ResultSummary, Record

from src.components.types import SubDistrict, Stop, Connection, ClusterStop, ModeOfTransport, Frequency

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "")

# Create a driver instance
driver = GraphDatabase.driver(URI, auth=AUTH)


# noinspection PyBroadException
def is_available():
    try:
        driver.verify_connectivity()
        return True
    except:
        return False

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

def get_stops(*, with_clusters = False, only_in_use: bool = False, id_list: list[str] = None, name_list: list[str] = None) -> list[Stop] | None:
    conditions = []
    if id_list:
        conditions.append(f"s.id IN [\"{'", "'.join(id_list)}\"]")
    if name_list:
        conditions.append(f"""ANY(
          elem IN [\"{'", "'.join(name_list)}\"]
          WHERE s.name CONTAINS elem
        )""")

    where_clause = f"WHERE {' OR '.join(conditions)}" if conditions else ""
    in_use_label = f":InUse" if only_in_use else ""

    base_query = f"""
    MATCH (s:Stop{in_use_label})
    {where_clause}
    """

    query = _finalize_stop_query(base_query, "s", with_clusters)
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


def get_nearby_stops(*, id_list: list[str] = None) -> list[tuple[str, list[str]]]:
    where_clause = f"WHERE s.id IN [\"{'", "'.join(id_list)}\"]" if id_list else ""

    query = f"""
    MATCH (s:Stop:InUse)-[:IS_CLOSE_TO]->(t:Stop:InUse)
    {where_clause}
    WITH s, collect(t.id) as potential_targets
    RETURN s.id as start, potential_targets
    """

    records = execute_query(query)
    return [(record["start"], record["potential_targets"]) for record in records]

def get_connections(connection_query: str):
    query_result = execute_query(connection_query)

    connections = []
    for record in query_result:
        from_stop = _parse_stop(record["from"], [])
        to_stop = _parse_stop(record["to"], [])
        mode_of_transport = _parse_mode_of_transport(record["label"])
        frequency = _parse_frequency(record["label"])

        connections.append(Connection(from_stop, to_stop, mode_of_transport, frequency))

    return connections


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

def _finalize_stop_query(base_query: str, stop_variable: str, with_clusters = False) -> str:
    if with_clusters:
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
    else:
        return f"""
        {base_query}
        WITH {stop_variable}
        RETURN DISTINCT
            {stop_variable}.id as id,
            {stop_variable}.lat as lat,
            {stop_variable}.lon as lon,
            {stop_variable}.name as name,
            False as is_cluster;
        """

def _parse_stop(node: neo4j.graph.Node, cluster_points) -> Stop:
    if 'ClusterStop' in node.labels:
        return ClusterStop(node["id"], float(node["lat"]), float(node["lon"]), node["name"],
                           float(node["cluster_lat"]), float(node["cluster_lon"]), cluster_points)
    else:
        return Stop(node["id"], float(node["lat"]), float(node["lon"]), node["name"])

def _parse_mode_of_transport(connection_label: str) -> ModeOfTransport:
    match connection_label:
        case "BUS_CONNECTS_TO": return ModeOfTransport.BUS
        case "TRAM_CONNECTS_TO": return ModeOfTransport.TRAM
        case "SUBWAY_CONNECTS_TO": return ModeOfTransport.SUBWAY
        case _: return ModeOfTransport.ANY

def _parse_frequency(frequency_label: str) -> Frequency:
    if frequency_label in [member.name for member in Frequency]:
        return Frequency[frequency_label]
    else:
        return Frequency.UNKNOWN
