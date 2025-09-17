use indicatif::{HumanDuration, MultiProgress, ProgressBar};
use neo4rs::{query, Graph};
use path::PathBuf;
use std::time::Duration;
use std::{fs, path};
use thiserror::Error;
use tokio::time::Instant;

/// Public error struct of this module, providing an easily parsable reason why the requested
/// action failed.
#[derive(Debug, Error)]
pub enum ImportError {
    #[error("Failed to create uniqueness constraint for {node}.{prop}:\n{source}")]
    ConstraintCreation {
        node: String,
        prop: String,
        #[source]
        source: neo4rs::Error,
    },

    #[error("Failed to create index for {node}.{prop}:\n{source}")]
    IndexCreation {
        node: String,
        prop: String,
        #[source]
        source: neo4rs::Error,
    },

    #[error("An error occurred during the import of '{name}':\n{source}")]
    DataImport {
        name: String,
        #[source]
        source: neo4rs::Error,
    },

    /// General error that auto-maps from `neo4rs` errors
    #[error("Database connection error: {0}")]
    Connection(#[from] neo4rs::Error),
}


pub async fn is_database_empty(graph: &Graph) -> Result<bool, ImportError> {
    // Ask the database to return '1' if there is ANY GTFS node and check if we received something
    let mut result = graph.execute(query(r#"
        MATCH (n)
        WHERE n:Agency OR n:Route OR n:Trip OR n:Service OR n:ServiceException OR n:Stop
        LIMIT 1
        RETURN 1
    "#)).await?;

    match result.next().await {
        Ok(Some(_)) => Ok(false), // DB already contains some GTFS data
        Ok(None) => Ok(true), // DB contains no GTFS data
        Err(e) => Err(ImportError::Connection(e))
    }
}

/// Writes the data from the GTFS files placed in the `/gtfs` directory into the given
/// neo4j graph instance.
pub async fn write_gtfs_into(graph: &Graph) -> Result<(), ImportError> {
    let multi_progress = MultiProgress::new();
    let outer_spinner = fork_spinner(&multi_progress, 300,
                                     "Importing GTFS data into Neo4j ...".to_string());

    // Note: Uniqueness constraints implicitly create indexes
    let local_spinner = fork_spinner(&multi_progress, 150,
                                     "Creating uniqueness constraints for GTFS node types".to_string());
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
            .map_err(|e| ImportError::ConstraintCreation {
                node: node.to_string(),
                prop: prop.to_string(),
                source: e,
            })?;
    }
    local_spinner.finish_with_message("✅  Created uniqueness constraints for GTFS node types");

    let local_spinner = fork_spinner(&multi_progress, 150,
                                     "Creating indexes for GTFS node types".to_string());
    let index_queries = [
        ("(ex:ServiceException)", "(ex.service_id)"),
        ("(s:Stop)", "(s.lon)"),
        ("(s:Stop)", "(s.lat)"),
        ("()-[at:STOPS_AT]-()", "(at.stop_sequence)"),
    ];
    for (node, prop) in index_queries {
        graph.run(neo4rs::query(&format!(
            "CREATE INDEX IF NOT EXISTS FOR {} ON {}", node, prop)))
            .await
            .map_err(|e| ImportError::IndexCreation {
                node: node.to_string(),
                prop: prop.to_string(),
                source: e,
            })?;
    }
    local_spinner.finish_with_message("✅  Created indexes for GTFS node types");


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
        CALL (row) {
            MATCH (r:Route {id: row.route_id})
            MATCH (s:Service {id: row.service_id})
            MERGE (t:Trip {id: row.trip_id})
            SET t.headsign = row.trip_headsign,
                t.direction = toInteger(row.direction_id),
                t.block = row.block_id
            MERGE (t)-[:PART_OF_ROUTE]->(r)
            MERGE (t)-[:OPERATING_ON]->(s)
        } IN TRANSACTIONS OF 10000 ROWS
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
        let query_task = graph.run(query(query_str));
        let status_message = format!("Importing: {}", name);
        run_task_with_spinner(query_task, status_message, &multi_progress).await
            .map_err(|e| ImportError::DataImport {
            name: name.to_string(),
            source: e,
        })?;
    }

    outer_spinner.finish_with_message("\r✅  Imported GTFS data into Neo4j");
    Ok(())
}

// fn run_task_with_spinner(query: &str, status_msg: &str) {
async fn run_task_with_spinner<T, E>(
    task: impl Future<Output = Result<T, E>>,
    status_msg: String,
    spinner_pool: &MultiProgress
) -> Result<T, E>
{
    let spinner = spinner_pool.add(ProgressBar::new_spinner());
    //spinner.set_style(ProgressStyle::with_template("  {spinner} {msg}").unwrap_or(ProgressStyle::default_spinner()));
    spinner.enable_steady_tick(Duration::from_millis(150));

    spinner.set_message(status_msg.clone());
    let start_time = Instant::now();

    let result = task.await;
    if result.is_ok() {
        spinner.finish_with_message(format!("✅  {} -> finished in {}", status_msg, HumanDuration(start_time.elapsed())));
    }

    result
}

fn fork_spinner(spinner_pool: &MultiProgress, tick_millis: u64, start_message: String) -> ProgressBar {
    let spinner = spinner_pool.add(ProgressBar::new_spinner());
    spinner.enable_steady_tick(Duration::from_millis(tick_millis));
    spinner.set_message(start_message);
    spinner
}


#[allow(dead_code)]
pub fn load_gtfs_in_memory(gtfs_zip_path: &str) -> gtfs_structures::Gtfs {
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