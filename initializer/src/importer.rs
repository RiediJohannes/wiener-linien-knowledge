use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use neo4rs::Graph;
use path::PathBuf;
use std::time::Duration;
use std::{env, fs, path};
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


pub async fn db_contains_gtfs_data(graph: &Graph) -> Result<bool, ImportError> {
    // Ask the database to return '1' if there is ANY GTFS node and check if we received something
    let mut result = graph.execute(neo4rs::query(r#"
        MATCH (n)
        WHERE n:Agency OR n:Route OR n:Trip OR n:Service OR n:ServiceException OR n:Stop
        LIMIT 1
        RETURN 1
    "#)).await?;

    match result.next().await {
        Ok(Some(_)) => Ok(true), // DB already contains some GTFS data
        Ok(None) => Ok(false), // DB contains no GTFS data
        Err(e) => Err(ImportError::Connection(e))
    }
}

pub async fn db_contains_city_data(graph: &Graph) -> Result<bool, ImportError> {
    // Ask the database to return '1' if there is ANY Subdistrict node and check if we received something
    let mut result = graph.execute(neo4rs::query(r#"
        MATCH (n)
        WHERE n:SubDistrict
        LIMIT 1
        RETURN 1
    "#)).await?;

    match result.next().await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(e) => Err(ImportError::Connection(e))
    }
}


/// Writes the data from the GTFS files placed in the `/gtfs` directory into the given
/// neo4j graph instance.
pub async fn write_gtfs_data_into(graph: &Graph) -> Result<(), ImportError> {
    let multi_progress = MultiProgress::with_draw_target(get_draw_target());
    let outer_spinner = multi_progress.fork_parent_spinner("Importing GTFS data into Neo4j ...".to_string());

    // Note: Uniqueness constraints implicitly create indexes
    let local_spinner = multi_progress.fork_child_spinner("Creating uniqueness constraints for GTFS node types".to_string());
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

    let local_spinner = multi_progress.fork_child_spinner("Creating indexes for GTFS node types".to_string());
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


    let gtfs_csv_queries = [
        (
            "agencies",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/agency.txt' AS row
        MERGE (a:Agency {id: row.agency_id})
          SET a.name = row.agency_name,
              a.url = row.agency_url
        "#,
        ),
        (
            "stops",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///gtfs/stops.txt' AS row
        MERGE (s:Stop {id: row.stop_id})
          SET s.name = row.stop_name,
              s.lat = toFloat(row.stop_lat),
              s.lon = toFloat(row.stop_lon)
        "#,
        ),
        (
            "routes",
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
            "services",
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
            "service exceptions",
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
            "trips",
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
            "stop times",
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

    for (name, query_str) in gtfs_csv_queries {
        let query_task = graph.run(neo4rs::query(query_str));
        let status_message = format!("Importing {}", name);
        run_task_with_spinner(query_task, status_message, &multi_progress).await
            .map_err(|e| ImportError::DataImport {
            name: name.to_string(),
            source: e,
        })?;
    }

    outer_spinner.finish_with_message(" -- GTFS data imports ✔️");
    Ok(())
}

pub async fn write_population_data_into(graph: &Graph) -> Result<(), ImportError> {
    let multi_progress = MultiProgress::with_draw_target(get_draw_target());
    let outer_spinner = multi_progress.fork_parent_spinner("Importing Vienna city data into Neo4j ...".to_string());

    let local_spinner = multi_progress.fork_child_spinner("Creating indexes for subdistrict codes".to_string());
    let index_queries = [
        ("(s:SubDistrict)", "(s.district_num)"),
        ("(s:SubDistrict)", "(s.sub_district_num)"),
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
    local_spinner.finish_with_message("✅  Created indexes for subdistrict codes");

    let city_csv_queries = [
        (
            "Importing population data",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///city/vienna_population.csv' AS row
        FIELDTERMINATOR ';' // specify the custom delimiter
        MERGE (s:SubDistrict {
            district_num: toInteger(substring(row.DISTRICT_CODE, 1, 2)),
            sub_district_num: toInteger(substring(row.SUB_DISTRICT_CODE, 3, 2))
        })
        SET s.population = toInteger(row.WHG_POP_TOTAL)
        "#
        ),
        (
            "Importing registration district names",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///city/registration_districts_names.csv' AS row
        FIELDTERMINATOR ';' // specify the custom delimiter
        MERGE (s:SubDistrict {
            district_num: toInteger(substring(row.DISTRICT_CODE, 1, 2)),
            sub_district_num: toInteger(substring(row.SUB_DISTRICT_CODE_VIE, 3, 2))
          })
          SET s.name = row.NAME_VIE
        "#
        ),
        (
            "Importing registration district coordinates",
            r#"
        LOAD CSV WITH HEADERS FROM 'file:///city/registration_districts_shapes.csv' AS row
        MERGE (s:SubDistrict {
            district_num: toInteger(row.BEZNR),
            sub_district_num: toInteger(row.ZBEZNR)
          })
          SET s.area = toFloat(row.FLAECHE),
              s.shape = row.SHAPE
        "#
        ),
    ];

    for (task_description, query_str) in city_csv_queries {
        let query_task = graph.run(neo4rs::query(query_str));
        run_task_with_spinner(query_task, task_description.to_string(), &multi_progress).await
            .map_err(|e| ImportError::DataImport {
                name: task_description.to_string(),
                source: e,
            })?;
    }

    outer_spinner.finish_with_message(" -- Vienna city data imports ✔️");
    print!("\r\n");

    Ok(())
}


fn get_draw_target() -> ProgressDrawTarget {
    // Reduce the refresh rate of the progress output if the STDOUT is not dynamic.
    // This ENV flag is often used when the output does not allow for redrawing the same line
    // using \r, and instead appends every print as a new line.
    match env::var("OUTPUT").ok().as_deref() {
        Some("static") => ProgressDrawTarget::stdout_with_hz(1),
        _ => ProgressDrawTarget::stdout()
    }
}

trait SpinnerPool {
    fn fork_spinner(&self, tick_millis: u64, start_message: String) -> ProgressBar;
    fn fork_parent_spinner(&self, start_message: String) -> ProgressBar;
    fn fork_child_spinner(&self, start_message: String) -> ProgressBar;
}

impl SpinnerPool for MultiProgress {
    fn fork_spinner(&self, tick_millis: u64, start_message: String) -> ProgressBar {
        let spinner = self.add(ProgressBar::new_spinner());
        spinner.enable_steady_tick(Duration::from_millis(tick_millis));
        spinner.set_message(start_message);
        spinner
    }
    fn fork_parent_spinner(&self, start_message: String) -> ProgressBar {
        let spinner = self.fork_spinner(200, start_message);
        spinner.set_style(
            ProgressStyle::with_template("{spinner:.blue} {msg:.bold}")
                .unwrap_or(ProgressStyle::default_spinner())
                .tick_strings(&[". ", "..", " .", "  "])
        );

        spinner
    }
    fn fork_child_spinner(&self, start_message: String) -> ProgressBar {
        let spinner = self.fork_spinner(100, start_message);
        spinner.set_style(
            ProgressStyle::with_template("  {spinner:.green} {msg}")
                .unwrap_or(ProgressStyle::default_spinner())
                .tick_chars("⣾⣽⣻⢿⡿⣟⣯⣷ ")
        );

        spinner
    }
}

async fn run_task_with_spinner<T, E>(
    task: impl Future<Output = Result<T, E>>,
    status_msg: String,
    spinner_pool: &dyn SpinnerPool
) -> Result<T, E>
{
    let spinner = spinner_pool.fork_child_spinner(status_msg.clone());
    //spinner.set_style(ProgressStyle::with_template("  {spinner} {msg}").unwrap_or(ProgressStyle::default_spinner()));

    spinner.set_message(status_msg.clone());
    let start_time = Instant::now();

    let result = task.await;
    if result.is_ok() {
        spinner.finish_with_message(format!("✅  {} -> finished in {}", status_msg, intuitive_duration(start_time.elapsed())));
    }

    result
}

fn intuitive_duration(duration: Duration) -> String {
    let total_millis = duration.as_millis();
    let total_secs = duration.as_secs_f64();

    if total_millis < 1000 {
        format!("{} milliseconds", total_millis)
    } else if total_secs < 60.0 {
        format!("{:.1} seconds", total_secs)
    } else {
        let total_mins = total_secs / 60.0;
        format!("{:.1} minutes", total_mins)
    }
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