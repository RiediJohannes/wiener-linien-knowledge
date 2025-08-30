use std::{fs, path};
use neo4rs::{query, ConfigBuilder, Graph};
use path::PathBuf;

#[tokio::main]
async fn main() {
    const RELATIVE_GTFS_PATH: &str = "..\\data\\wiener_linien_gtfs.zip";
    let gtfs_db_path = fs::canonicalize(&PathBuf::from(RELATIVE_GTFS_PATH))
        .expect(format!("Failed to resolve an absolute path from '{}'", RELATIVE_GTFS_PATH).as_ref())
        .to_str().expect("GTFS path found but failed to stringify it").to_string();

    println!("Reading GTFS database...");
    // let gtfs = gtfs_structures::GtfsReader::default()
    //     // .read_stop_times(false) // Won’t read the stop times to save time and memory
    //     .read_shapes(false) // Omit shapes to save time and memory
    //     .unkown_enum_as_default(false) // Don’t convert unknown enumerations into their default value
    //     .trim_fields(false)
    //     .read(gtfs_db_path.as_str())
    //     .unwrap_or_else(|e| panic!("Could not find or open GTFS database at {}.\nError: {}", &gtfs_db_path, e)
    // );
    // println!("GTFS database read in {:?} seconds", gtfs.read_duration.as_secs());

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

    // gtfs.agencies.into_iter().for_each(async |agency: Agency| {
    //     graph.run(
    //         query("CREATE (a:Agency { name: $name, url: $url })")
    //             .param("name", agency.name)
    //             .param("url", agency.url)
    //     ).await.unwrap();
    // });

    let result = graph.execute(query("MATCH (n) RETURN 1 LIMIT 1")).await;


    // for agency in gtfs.agencies {
    //     graph.run(
    //         query("CREATE (a:Agency { name: $name, url: $url })")
    //             .param("name", agency.name)
    //             .param("url", agency.url)
    //     ).await.unwrap();
    // }

    // Transaction
    let mut txn = graph.start_txn().await.unwrap();
    txn.run_queries([
        "LOAD CSV WITH HEADERS FROM 'file:///agency.txt' AS row
         MERGE (a:Agency {agency_id: row.agency_id, agency_name: row.agency_name})"
    ])
    .await
    .unwrap();
    txn.commit().await.unwrap();

    println!("Successfully initialized Neo4j database!");
}
