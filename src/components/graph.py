from jedi.inference.gradual.typing import AnyClass
from neo4j import GraphDatabase

# Replace with your Neo4j credentials and URI
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "")

# Create a driver instance
driver = GraphDatabase.driver(URI, auth=AUTH)


def import_population_data():
    run_query("CREATE INDEX IF NOT EXISTS FOR (s:SubDistrict) ON (s.district_num);")
    run_query("CREATE INDEX IF NOT EXISTS FOR (s:SubDistrict) ON (s.sub_district_num);")

    query = """
    LOAD CSV WITH HEADERS FROM 'file:///city/vienna_population.csv' AS row
    FIELDTERMINATOR ';' // specify the custom delimiter
    MERGE (s:SubDistrict {
        district_num: toInteger(substring(row.DISTRICT_CODE, 1, 2)),
        sub_district_num: toInteger(substring(row.SUB_DISTRICT_CODE, 3, 2))
      })
      SET s.population = toInteger(row.WHG_POP_TOTAL)
    """
    run_query(query)
    print("Successfully imported population data")

    query = """
    LOAD CSV WITH HEADERS FROM 'file:///city/registration_districts_names.csv' AS row
    FIELDTERMINATOR ';' // specify the custom delimiter
    MERGE (s:SubDistrict {
        district_num: toInteger(substring(row.DISTRICT_CODE, 1, 2)),
        sub_district_num: toInteger(substring(row.SUB_DISTRICT_CODE_VIE, 3, 2))
      })
      SET s.name = row.NAME_VIE
    """
    run_query(query)
    print("Successfully imported registration district names")

    query = """
    LOAD CSV WITH HEADERS FROM 'file:///city/registration_districts_shapes.csv' AS row
    MERGE (s:SubDistrict {
        district_num: toInteger(row.BEZNR),
        sub_district_num: toInteger(row.ZBEZNR)
      })
      SET s.area = toFloat(row.FLAECHE),
          s.shape = row.SHAPE
    """
    run_query(query)
    print("Successfully imported registration district coordinates")


# Define a function to run a query
def run_query(query, **params) -> list[dict[str, AnyClass]]:
    with driver.session() as session:
        result = session.run(query, **params)
        return [dict(record) for record in result]
