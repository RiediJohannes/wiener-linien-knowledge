## General Plan

- Create KG
  - Load GTFS data into Neo4j
  - Load population data into Neo4J
  - Load district data into Neo4J
- Use cypher to derive logical connections between data points
- Feed everything into a KG embedding
- Find missing links

## Experiences

### GTFS Import

Reading the database using rust took roughly 3 minutes before optimisation.
- After deactivating shape files → roughly 2 minutes
- After deactivating trimming → 1 minute
- Additional problem with times that extend beyond 23:59:59


DISTRICT_CODE -- Gemeindebezirkskennzahl  
SUB_DISTRICT_CODE -- Zählbezirkskennzahl  
WHG_POP_TOTAL -- population

## Time

- 30.8.
  + 11:00-12:00
  + 13:00-14:00
  + 16:30-20:00

- 31.8.
  + 14:00-16:30
  + 18:00-19:00
  + 20:00-22:30


## Submission

Dockerizing my marimo app:  
https://docs.marimo.io/guides/deploying/deploying_docker/#create-a-dockerfile