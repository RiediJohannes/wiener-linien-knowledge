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

## Time

- 30.8.
  + 11:00-12:00
  + 13:00-14:00
  + 16:30-20:00

- 31.8.
  + 14:00-16:30
  + 18:00-19:00
  + 20:00-22:30

- 1.9.
  + 15:30-22:00
  + 23:30-2:00

- 2.9.
  + 15:30-22:00

- 3.9.
  - 14:00-23:00
  - 00:30-2:00

- 4.9
  - 16:00-17:00
  - 18:00-1:30
  - 4:00-8:30

- 5.9
  - 15:00-22:00


## Submission

### Data sources

Subdistrict names:  
https://www.data.gv.at/katalog/datasets/ae8d7db5-98e9-4f86-bbec-9babfa2a4f03

Subdistrict shapes:  
https://www.data.gv.at/katalog/datasets/e4079286-310c-435a-af2d-64604ba9ade5

Dockerizing my marimo app:  
https://docs.marimo.io/guides/deploying/deploying_docker/#create-a-dockerfile