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

## Embeddings

Look at comparative analysis paper:
https://dl.acm.org/doi/pdf/10.1145/3424672  
They were particularly fond of:
- HAKE and its predecessor RotatE
- ComplEx

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
- 4.9.
  - 16:00-17:00
  - 18:00-1:30
  - 4:00-8:30
- 5.9.
  - 15:00-22:00
- 6.9.
  - 17:00-22:30
  - 0:00-3:00
- 7.9.
  - 16:00-22:30
  - 1:30-3:00
- 9.9.
  - 7:00-11:00
  - 15:00-18:00
- 13.9.
  - 19:30-20:00
- 14.9.
  - 12:15-19:30
- 15.9.
  - 13:00-16:00
  - 17:30-20:30
  - 22:30-2:30
- 16.9.
  - 16:30-22:00
- 17.9.
  - 14:00-19:00
  - 22:00-00:30
- 18.9.
  - 15:00-18:30
  - 19:30-21:30
- 20.9.
  - 13:00-
  
## Submission

### Data sources

Population data:

https://www.data.gv.at/katalog/datasets/09e70f89-cadf-4a3b-a29a-2b3f3c6cbd71

Subdistrict names:  
https://www.data.gv.at/katalog/datasets/ae8d7db5-98e9-4f86-bbec-9babfa2a4f03

Subdistrict shapes:  
https://www.data.gv.at/katalog/datasets/e4079286-310c-435a-af2d-64604ba9ade5

Dockerizing my marimo app:  
https://docs.marimo.io/guides/deploying/deploying_docker/#create-a-dockerfile


##### Trying to make a multipage app

```html
<div style="display: flex; justify-content: space-between; width: 100%;">
    <a href="./notebooks/test.py" style="text-decoration: none;">← Previous page</a>
    <a href="./../src/main.py" style="text-decoration: none;">Next page →</a>
</div>
```