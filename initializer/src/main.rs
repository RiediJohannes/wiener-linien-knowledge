use std::{fs, path};
use neo4rs::{query, ConfigBuilder, Graph};
use path::PathBuf;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    const URI: &str = "127.0.0.1:7687";
    let neo4j_config = ConfigBuilder::default()
        .uri(URI)
        .user("neo4j")
        .password("")
        .db("gtfs")
        .fetch_size(500)
        .max_connections(10)
        .build()
        .expect("Failed to construct connection settings for Neo4j");

    println!("Connecting to Neo4j instance at {} ...", URI);
    let graph = Graph::connect(neo4j_config).await.unwrap();

    let result = graph.execute(query("MATCH (n) RETURN 1 LIMIT 1")).await;

    println!("Importing GTFS data into Neo4j ...");
    let queries = [
        ("Agencies",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///agency.txt' AS row
        CREATE (a:Agency {id: row.agency_id})
          SET a.name = row.agency_name,
              a.url = row.agency_url
        "#),

        ("Stops",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///stops.txt' AS row
        CREATE (s:Stop {id: row.stop_id})
          SET s.name = row.stop_name,
              s.lat = toFloat(row.stop_lat),
              s.lon = toFloat(row.stop_lon)
              // s.zone_id = row.zone_id
        "#),

        ("Routes",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///routes.txt' AS row
        MATCH (a:Agency {id: row.agency_id})
        CREATE (r:Route {id: row.route_id})
          SET r.short_name = row.route_short_name,
              r.long_name = row.route_long_name,
              r.type = toInteger(row.route_type),
              r.color = row.route_color
        CREATE (r)-[:OPERATED_BY]->(a)
        "#),

        ("Services",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///calendar.txt' AS row
        CREATE (s:Service {id: row.service_id})
          SET s.monday = (row.monday = '1'),
              s.tuesday = (row.tuesday = '1'),
              s.wednesday = (row.wednesday = '1'),
              s.thursday = (row.thursday = '1'),
              s.friday = (row.friday = '1'),
              s.saturday = (row.saturday = '1'),
              s.sunday = (row.sunday = '1'),
              s.start_date = date({
                  year: toInteger(substring(row.start_date,0,4)),
                  month: toInteger(substring(row.start_date,4,2)),
                  day: toInteger(substring(row.start_date,6,2))
              }),
              s.end_date = date({
                  year: toInteger(substring(row.end_date,0,4)),
                  month: toInteger(substring(row.end_date,4,2)),
                  day: toInteger(substring(row.end_date,6,2))
              })
        "#),

        ("Service exceptions",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///calendar_dates.txt' AS row
        MATCH (s:Service {id: row.service_id})
        CREATE (ex:ServiceException {
            service_id: row.service_id,
            date: date({
                year: toInteger(substring(row.date,0,4)),
                month: toInteger(substring(row.date,4,2)),
                day: toInteger(substring(row.date,6,2))
            })
        })
          SET ex.exception_type = toInteger(row.exception_type)
        CREATE (ex)-[:FOR_SERVICE]->(s)
        "#),

        ("Trips",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///trips.txt' AS row
        MATCH (r:Route {id: row.route_id})
        MATCH (s:Service {id: row.service_id})
        CREATE (t:Trip {id: row.trip_id})
          SET t.headsign = row.trip_headsign,
              t.direction = toInteger(row.direction_id),
              t.block = row.block_id
        CREATE (t)-[:PART_OF_ROUTE]->(r)
        "#),

        ("Stop times",
        r#"
        LOAD CSV WITH HEADERS FROM 'file:///stop_times.txt' AS row
        MATCH (t:Trip {id: row.trip_id})
        MATCH (s:Stop {id: row.stop_id})
        CREATE (st:StopTime {trip_id: row.trip_id, stop_id: row.stop_id, stop_sequence: toInteger(row.stop_sequence)})
          SET st.arrival_time = localtime(row.arrival_time),
              st.departure_time = localtime(row.departure_time),
              st.stop_sequence = row.stop_sequence,
              st.pickup_type = row.pickup_type,
              st.drop_off_type = row.drop_off_type,
              st.distance = row.shape_dist_traveled
        CREATE (st)-[:FOR_TRIP]->(t)
        CREATE (st)-[:AT_STOP]->(s)
        "#)
    ];

    for (name, query_str) in queries {
        print!("-- Importing data: {}", name);
        let start_time = Instant::now();

        graph.run(query(query_str)).await.unwrap();
        println!("-> finished in {} seconds", start_time.elapsed().as_secs());
    }

    println!("Successfully initialized Neo4j database!");
}

fn load_gtfs_in_memory(gtfs_zip_path: &str) -> gtfs_structures::Gtfs {
    let absolute_gtfs_path = fs::canonicalize(PathBuf::from(gtfs_zip_path))
        .unwrap_or_else(|_| panic!("Failed to resolve an absolute path from '{}'", gtfs_zip_path))
        .to_str().expect("GTFS path found but failed to stringify it").to_string();

    println!("Reading GTFS database...");
    let gtfs = gtfs_structures::GtfsReader::default()
        // .read_stop_times(false) // Won’t read the stop times to save time and memory
        .read_shapes(false) // Omit shapes to save time and memory
        .unkown_enum_as_default(false) // Don’t convert unknown enumerations into their default value
        .trim_fields(false)
        .read(absolute_gtfs_path.as_str())
        .unwrap_or_else(|e| panic!("Could not find or open GTFS database at {}.\nError: {}", absolute_gtfs_path, e)
        );
    println!("GTFS database read in {:?} seconds", gtfs.read_duration.as_secs());

    gtfs
}
