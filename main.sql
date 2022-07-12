-- create postgis extension
CREATE EXTENSION postgis;

-- create a table for locations
-- you might want to create two tables for the two location sets, same for the following code
DROP TABLE IF EXISTS sample_locations;
CREATE TABLE sample_locations(
  sensor_id INTEGER PRIMARY KEY,
  lon DOUBLE PRECISION NOT NULL,
  lat DOUBLE PRECISION NOT NULL);


DROP TABLE IF EXISTS sample_locations2;
CREATE TABLE sample_locations2(
  sensor_id INTEGER PRIMARY KEY,
  lon DOUBLE PRECISION NOT NULL,
  lat DOUBLE PRECISION NOT NULL);

-- create a table for buffers
DROP TABLE IF EXISTS sample_location_buffers;
CREATE TABLE sample_location_buffers(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER,
	lon DOUBLE PRECISION NOT NULL,
	lat DOUBLE PRECISION NOT NULL,
	buffer_size INTEGER NOT NULL,
	buffer geometry(Polygon,4326) NOT NULL);
CREATE INDEX "sample_location_buffers_buffer_idx" ON sample_location_buffers USING gist(buffer);

DROP TABLE IF EXISTS sample_location_buffers2;
CREATE TABLE sample_location_buffers2(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER,
	lon DOUBLE PRECISION NOT NULL,
	lat DOUBLE PRECISION NOT NULL,
	buffer_size INTEGER NOT NULL,
	buffer geometry(Polygon,4326) NOT NULL);
CREATE INDEX "sample_location_buffers_buffer_idx2" ON sample_location_buffers2 USING gist(buffer);


-- insert buffers into the buffer table
INSERT INTO sample_location_buffers (sensor_id,lon,lat,buffer_size,buffer)
SELECT sensor_id,lon,lat,100,ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(ST_SetSRID(ST_POINT(lon,lat), 4326),3857),100),4326)
from sample_locations;

INSERT INTO sample_location_buffers2 (sensor_id,lon,lat,buffer_size,buffer)
SELECT sensor_id,lon,lat,100,ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(ST_SetSRID(ST_POINT(lon,lat), 4326),3857),100),4326)
from sample_locations2;


INSERT INTO sample_location_buffers (sensor_id,lon,lat,buffer_size,buffer)
SELECT sensor_id,lon,lat,500,ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(ST_SetSRID(ST_POINT(lon,lat), 4326),3857),500),4326)
from sample_locations;

INSERT INTO sample_location_buffers2 (sensor_id,lon,lat,buffer_size,buffer)
SELECT sensor_id,lon,lat,500,ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(ST_SetSRID(ST_POINT(lon,lat), 4326),3857),500),4326)
from sample_locations2;


INSERT INTO sample_location_buffers (sensor_id,lon,lat,buffer_size,buffer)
SELECT sensor_id,lon,lat,1000,ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(ST_SetSRID(ST_POINT(lon,lat), 4326),3857),1000),4326)
from sample_locations;

INSERT INTO sample_location_buffers2 (sensor_id,lon,lat,buffer_size,buffer)
SELECT sensor_id,lon,lat,1000,ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(ST_SetSRID(ST_POINT(lon,lat), 4326),3857),1000),4326)
from sample_locations2;

-- # code block # --

CREATE INDEX "line_features_wkb_geometry_geom_idx" ON line_features USING gist(wkb_geometry);
CREATE INDEX "point_features_wkb_geometry_geom_idx" ON point_features USING gist(wkb_geometry);
CREATE INDEX "polygon_features_wkb_geometry_geom_idx" ON polygon_features USING gist(wkb_geometry);

-- create a table for geographic features
DROP TABLE IF EXISTS geographic_features;
CREATE TABLE geographic_features(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER NOT NULL,
	geom_type TEXT NOT NULL,
	geo_feature TEXT NOT NULL,
	feature_type TEXT NOT NULL,
	buffer_size INTEGER NOT NULL,
	value  DOUBLE PRECISION);

-- insert geographic features into the geographic_features table
INSERT INTO geographic_features (sensor_id,geom_type,geo_feature,feature_type,buffer_size,value)
SELECT sensor_id, 'line' ,geo_feature,feature_type,buffer_size,SUM(ST_LENGTH(ST_Intersection(ST_TRANSFORM(wkb_geometry,3857), ST_TRANSFORM(buffer,3857)))) from sample_location_buffers2,line_features 
where ST_Intersects(wkb_geometry,buffer) 
GROUP BY(sensor_id,buffer_size,geo_feature,feature_type);

INSERT INTO geographic_features (sensor_id,geom_type,geo_feature,feature_type,buffer_size,value)
SELECT sensor_id, 'line' ,geo_feature,feature_type,buffer_size,SUM(ST_LENGTH(ST_Intersection(ST_TRANSFORM(wkb_geometry,3857), ST_TRANSFORM(buffer,3857)))) from sample_location_buffers,line_features 
where ST_Intersects(wkb_geometry,buffer) 
GROUP BY(sensor_id,buffer_size,geo_feature,feature_type);

INSERT INTO geographic_features (sensor_id,geom_type,geo_feature,feature_type,buffer_size,value)
SELECT sensor_id, 'polygon' ,geo_feature,feature_type,buffer_size,SUM(ST_AREA(ST_Intersection(ST_TRANSFORM(wkb_geometry,3857), ST_TRANSFORM(buffer,3857)))) from sample_location_buffers2,polygon_features 
where ST_Intersects(wkb_geometry,buffer) 
GROUP BY(sensor_id,buffer_size,geo_feature,feature_type);

INSERT INTO geographic_features (sensor_id,geom_type,geo_feature,feature_type,buffer_size,value)
SELECT sensor_id, 'polygon' ,geo_feature,feature_type,buffer_size,SUM(ST_AREA(ST_Intersection(ST_TRANSFORM(wkb_geometry,3857), ST_TRANSFORM(buffer,3857)))) from sample_location_buffers,polygon_features 
where ST_Intersects(wkb_geometry,buffer) 
GROUP BY(sensor_id,buffer_size,geo_feature,feature_type);

INSERT INTO geographic_features (sensor_id,geom_type,geo_feature,feature_type,buffer_size,value)
SELECT sensor_id, 'point' ,geo_feature,feature_type,buffer_size,COUNT(ST_Contains(ST_TRANSFORM(buffer,3857), ST_TRANSFORM(wkb_geometry,3857))) from sample_location_buffers,point_features 
where ST_Contains(buffer,wkb_geometry) 
GROUP BY(sensor_id,buffer_size,geo_feature,feature_type);

INSERT INTO geographic_features (sensor_id,geom_type,geo_feature,feature_type,buffer_size,value)
SELECT sensor_id, 'point' ,geo_feature,feature_type,buffer_size,COUNT(ST_Contains(ST_TRANSFORM(buffer,3857), ST_TRANSFORM(wkb_geometry,3857))) from sample_location_buffers2,point_features 
where ST_Contains(buffer,wkb_geometry) 
GROUP BY(sensor_id,buffer_size,geo_feature,feature_type);

-- # code block # --