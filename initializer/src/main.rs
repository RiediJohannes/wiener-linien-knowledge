mod importer;

use std::env;
use dialoguer::Confirm;
use neo4rs::{ConfigBuilder, Graph};

#[tokio::main]
async fn main() {
    // Parse command-line flags
    let args: Vec<String> = env::args().collect();
    let abort_flag = args.contains(&"--abort-if-present".to_string());
    let force_flag = args.contains(&"--force".to_string());

    let connection_uri = env::var("NEO4J_URI").unwrap_or("127.0.0.1:7687".to_string());
    let neo4j_config = ConfigBuilder::default()
        .uri(&connection_uri)
        .user("neo4j")
        .password("")
        .db("gtfs")
        .fetch_size(500)
        .max_connections(10)
        .build()
        .expect("Failed to construct connection settings for Neo4j");

    println!("Connecting to Neo4j instance at {} ...", connection_uri);
    let graph = Graph::connect(neo4j_config)
        .await
        .expect("Failed to connect to Neo4j instance");


    // Check if the database already contains some GTFS data and conditionally abort the import
    let db_contains_gtfs: bool = importer::db_contains_gtfs_data(&graph).await
        .unwrap_or_else(|e| on_error(e));

    let should_import_gtfs = match (db_contains_gtfs, abort_flag, force_flag) {
        (false, _, _) => true,
        (true, true, _) => {
            println!("  GTFS data already present ✔️");
            false
        }
        (true, _, true) => {
            println!("GTFS data already present, but proceeding nonetheless (--force).");
            true
        }
        (true, _, _) => {
            println!(
                "Warning: Your Neo4j instance already contains some GTFS data. Importing might lead to inconsistent data."
            );
            Confirm::new()
                .with_prompt("Do you want to continue? (y/n)")
                .interact()
                .unwrap_or(false)
        }
    };

    if should_import_gtfs {
        _ = importer::write_gtfs_data_into(&graph).await.map_err(|e| on_error(e));
    }

    let db_contains_city_data = importer::db_contains_city_data(&graph).await
        .unwrap_or_else(|e| on_error(e));

    if !db_contains_city_data || force_flag {
        _ = importer::write_population_data_into(&graph).await.map_err(|e| on_error(e));
    } else {
        println!("  City data already present ✔️");
    }

    println!("\n=== Successfully initialized Neo4j database ===");
}

fn on_error(err: impl std::fmt::Display) -> ! {
    eprintln!("{}", err);
    std::process::exit(1);
}