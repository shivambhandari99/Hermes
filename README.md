# Hermes
A framework to perform PostgreSQL with PostGIS and Apache Sedona (formerly GeoSpark) to perform spatial queries.

# Performance Markers

|               |         | #locations = 400  | #locations = 10   |
| ------------- | ------- | ----------------- | ----------------- |
| PostgreSQL    | Line    | 96                | 10.705            |
|               | Polygon | 174               | 11.245            |
|               | Point   | 14.963            | 2.766             |
| Apache Sedona | Line    | 33.28013610839844 | 33.56338620185852 |
|               | Polygon | 48.85433387756348 | 47.79330921173096 |
|               | Point   | 7.315363168716431 | 4.588996171951294 |

For PostgreSQL, as we increase the number of sensor locations from 10 to 400, the time taken increases quite significantly, usually by a magnitude of ~10-15 times. For Apache Sedona, as we increase the sensor locations from 10 to 400, the time remains virtually the same. The reason for this is that buffer is being broadcast across different executors which divide up the osm data. Since the osm data remains the same regardless of number of sensors, the execution time is virtually the same. The same isn’t true for PostgreSQL because the data is likely being processed linearly. 
