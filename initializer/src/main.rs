mod importer;

use std::env;
use dialoguer::Confirm;
use neo4rs::{ConfigBuilder, Graph};

#[tokio::main]
async fn main() {
    // Parse command-line flags
    let args: Vec<String> = env::args().collect();
    let skip_if_present = args.contains(&"--skip-if-present".to_string());
    let force = args.contains(&"--force".to_string());

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

    // Check if the database already contains some GTFS data and conditionally abort the import
    let db_is_empty: bool = importer::is_database_empty(&graph).await.unwrap_or_else(|e| on_error(e));
    if !db_is_empty {
        if skip_if_present {
            println!("GTFS data already present -> skipping import to prevent inconsistent data.");
            std::process::exit(0);
        } else if force {
            println!("GTFS data already present, but proceeding with import (--force).");
        } else {
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
    }

    match importer::write_gtfs_into(&graph).await {
        Ok(_) => println!("Successfully initialized Neo4j database!"),
        Err(e) => on_error(e)
    };
}

fn on_error(err: impl std::fmt::Display) -> ! {
    eprintln!("{}", err);
    std::process::exit(1);
}