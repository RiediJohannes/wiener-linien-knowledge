mod importer;

use dialoguer::Confirm;
use neo4rs::{ConfigBuilder, Graph, query};

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

    match importer::write_gtfs_into(graph).await {
        Ok(_) => {
            println!("Successfully initialized Neo4j database!");
        },
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    };
}