# Wiener Linien Knowledge Graph

_A machine learning project to uncover missing transit connections in Vienna’s public transport network._

This project builds a [knowledge graph](https://en.wikipedia.org/wiki/Knowledge_graph) (KG) of the Dec 2024 - Dec 2025 public
transport schedule of Wiener Linien GmbH & Co KG. It combines an open [GTFS schedule dataset](https://www.data.gv.at/katalog/datasets/ab4a73b6-1c2d-42e1-b4d9-049e04889cf0)
with [fine-grained population data](https://www.data.gv.at/katalog/datasets/09e70f89-cadf-4a3b-a29a-2b3f3c6cbd71) (2021) and [geographic shapes of registration districts](https://www.data.gv.at/katalog/datasets/e4079286-310c-435a-af2d-64604ba9ade5) (_“Zählbezirke”_)
to **reason about transport availability in relation to population density**.

Applying the machine learning technique of [**knowledge graph embeddings**](https://en.wikipedia.org/wiki/Knowledge_graph_embedding),
this KG is then used to predict **where new or improved connections could strengthen the network** by suggesting either entirely new connections
a higher frequency service between existing stops (see [link prediction](https://en.wikipedia.org/wiki/Link_prediction)).

## Technologies

All data is stored in a [Neo4j graph database](https://neo4j.com), providing a unified representation of the aggregated
data sources and derived knowledge. For exploration and experimentation, the project includes a [Marimo notebook](https://marimo.io)
(a modern alternative to Jupyter) that offers an interactive and user-friendly interface to work with the knowledge graph.

For the knowledge graph embeddings, two different embedding models were compared, namely **RotatE** and **ComplEx**.
