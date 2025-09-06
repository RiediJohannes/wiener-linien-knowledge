use dialoguer::Confirm;
use neo4rs::{ConfigBuilder, Graph, query};
use path::PathBuf;
use std::io::Write;
use std::{fs, io, path};
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
    let graph = Graph::connect(neo4j_config)
        .await
        .expect("Failed to connect to Neo4j instance");

    // Ask the database to return '1' if there is ANY node and check if we received something
    let mut result = graph.execute(query(r#"
        MATCH (n)
        WHERE n:Agency OR n:Route OR n:Trip OR n:Service OR n:ServiceException OR n:Stop
        RETURN 1 LIMIT 1
    "#)).await.unwrap();
    if let Ok(Some(_)) = result.next().await {
        println!(
            "Warning: Your Neo4j instance already contains some GTFS data. Importing the GTFS data might lead to inconsistent data."
        );
        let confirmation = Confirm::new()
            .with_prompt("Do you want to continue? (y/n)")
            .interact();
        if confirmation.is_err() || !confirmation.unwrap() {
            std::process::exit(0);
        }
    }

    println!("Importing GTFS data into Neo4j ...");
    println!("-- Creating indexes for GTFS node types");
    // Note: Uniqueness constraints implicitly create indexes
    let uniqueness_constraints = [
        ("(r:Route)",   "r.id"),
        ("(s:Service)", "s.id"),
        ("(t:Trip)",    "t.id"),
        ("(s:Stop)",    "s.id"),
    ];
    for (node, prop) in uniqueness_constraints {
        graph.run(neo4rs::query(&format!(
            "CREATE CONSTRAINT IF NOT EXISTS FOR {} REQUIRE {} IS UNIQUE", node, prop)))
            .await
            .unwrap_or_else(|e| panic!("Failed to create uniqueness constraint.\nError: {}", e));
    }

    let index_queries = [
        ("(ex:ServiceException)", "(ex.service_id)"),
        ("(s:Stop)", "(s.lon)"),
        ("(s:Stop)", "(s.lat)"),
        ("()-[at:STOPS_AT]-())", "(at.stop_sequence)"),
    ];
    for (node, prop) in index_queries {
        graph.run(neo4rs::query(&format!(
            "CREATE INDEX IF NOT EXISTS FOR {} ON {}", node, prop)))
            .await
            .unwrap_or_else(|e| panic!("Failed to create indexes.\nError: {}", e));
    }

    let csv_queries = [
        (
            "Agencies",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/agency.txt' AS row
        MERGE (a:Agency {id: row.agency_id})
          SET a.name = row.agency_name,
              a.url = row.agency_url
        "#,
        ),
        (
            "Stops",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/stops.txt' AS row
        MERGE (s:Stop {id: row.stop_id})
          SET s.name = row.stop_name,
              s.lat = toFloat(row.stop_lat),
              s.lon = toFloat(row.stop_lon)
        "#,
        ),
        (
            "Routes",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/routes.txt' AS row
        MATCH (a:Agency {id: row.agency_id})
        MERGE (r:Route {id: row.route_id})
          SET r.short_name = row.route_short_name,
              r.long_name = row.route_long_name,
              r.type = toInteger(row.route_type),
              r.color = row.route_color
        MERGE (r)-[:OPERATED_BY]->(a)
        "#,
        ),
        (
            "Services",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/calendar.txt' AS row
        MERGE (s:Service {id: row.service_id})
          SET s.monday = toInteger(row.monday),
              s.tuesday = toInteger(row.tuesday),
              s.wednesday = toInteger(row.wednesday),
              s.thursday = toInteger(row.thursday),
              s.friday = toInteger(row.friday),
              s.saturday = toInteger(row.saturday),
              s.sunday = toInteger(row.sunday),
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
        "#,
        ),
        (
            "Service exceptions",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/calendar_dates.txt' AS row
        MATCH (s:Service {id: row.service_id})
        MERGE (ex:ServiceException {
            service_id: row.service_id,
            date: date({
                year: toInteger(substring(row.date,0,4)),
                month: toInteger(substring(row.date,4,2)),
                day: toInteger(substring(row.date,6,2))
            })
        })
          SET ex.exception_type = toInteger(row.exception_type)
        MERGE (ex)-[:FOR_SERVICE]->(s)
        "#,
        ),
        (
            "Trips",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/trips.txt' AS row
        MATCH (r:Route {id: row.route_id})
        MATCH (s:Service {id: row.service_id})
        MERGE (t:Trip {id: row.trip_id})
          SET t.headsign = row.trip_headsign,
              t.direction = toInteger(row.direction_id),
              t.block = row.block_id
        MERGE (t)-[:PART_OF_ROUTE]->(r)
        MERGE (t)-[:OPERATING_ON]->(s)
        "#,
        ),
        (
            "Importing stop times",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/stop_times.txt' AS row
        CALL (row) {
            MATCH (t:Trip {id: row.trip_id})
            MATCH (s:Stop {id: row.stop_id})
            // Use CREATE instead of MERGE since MERGE for relationships is very inefficient
            CREATE (t)-[at:STOPS_AT {stop_sequence: toInteger(row.stop_sequence)}]->(s)
            SET at.arrival_time = localtime(row.arrival_time),
                at.departure_time = localtime(row.departure_time),
                at.pickup_type = toInteger(row.pickup_type),
                at.drop_off_type = toInteger(row.drop_off_type),
                at.distance = row.shape_dist_traveled
        } IN TRANSACTIONS OF 10000 ROWS
        "#,
        ),
    ];

    for (name, query_str) in csv_queries {
        print!("-- Importing: {}", name);
        io::stdout().flush().unwrap();

        let start_time = Instant::now();
        graph.run(query(query_str)).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred during the import of '{}'.\nError: {}",
                name, e
            )
        });
        println!(" -> finished in {} seconds", start_time.elapsed().as_secs());
    }

    println!("Successfully initialized Neo4j database!");
}

#[allow(dead_code)]
fn load_gtfs_in_memory(gtfs_zip_path: &str) -> gtfs_structures::Gtfs {
    let absolute_gtfs_path = fs::canonicalize(PathBuf::from(gtfs_zip_path))
        .unwrap_or_else(|_| {
            panic!(
                "Failed to resolve an absolute path from '{}'",
                gtfs_zip_path
            )
        })
        .to_str()
        .expect("GTFS path found but failed to stringify it")
        .to_string();

    println!("Reading GTFS database...");
    let gtfs = gtfs_structures::GtfsReader::default()
        // .read_stop_times(false) // Won’t read the stop times to save time and memory
        .read_shapes(false) // Omit shapes to save time and memory
        .unkown_enum_as_default(false) // Don’t convert unknown enumerations into their default value
        .trim_fields(false)
        .read(absolute_gtfs_path.as_str())
        .unwrap_or_else(|e| {
            panic!(
                "Could not find or open GTFS database at {}.\nError: {}",
                absolute_gtfs_path, e
            )
        });
    println!(
        "GTFS database read in {:?} seconds",
        gtfs.read_duration.as_secs()
    );

    gtfs
}
