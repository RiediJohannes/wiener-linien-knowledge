from neo4j import GraphDatabase, ResultSummary, Record

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "")

# Create a driver instance
driver = GraphDatabase.driver(URI, auth=AUTH)


class Stop:
    def __init__(self, stop_id: str, latitude: float, longitude: float):
        self.id: str = stop_id
        self.lat: float = latitude
        self.lon: float = longitude

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

def get_stops() -> list[Stop] | None:
    query = """
    MATCH (s:Stop)
    RETURN s.id as id,
           s.lat as lat,
           s.lon as lon;
    """
    results = execute_query(query)
    return [Stop(record["id"], record["lat"], record["lon"]) for record in results]

def cluster_stops(stop_clusters: list[list[str]]) -> ResultSummary | None:
    all_stop_pairs = []
    for cluster in stop_clusters:
        pairs = [(stop1, stop2) for i, stop1 in enumerate(cluster)
                 for stop2 in cluster[i+1:]]
        all_stop_pairs.extend(pairs)

    if all_stop_pairs:
        operation = """
        UNWIND $all_pairs AS pair
        MERGE (s1:Stop {id: pair[0]})
        MERGE (s2:Stop {id: pair[1]})
        MERGE (s1)-[:FUNCTIONS_AS]->(s2)
        MERGE (s2)-[:FUNCTIONS_AS]->(s1)
        """
        return execute_operation(operation, all_pairs=all_stop_pairs)

    return None


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
