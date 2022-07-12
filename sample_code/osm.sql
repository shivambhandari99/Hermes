
        DROP TABLE IF EXISTS line_features;
        CREATE TABLE line_features(
            gid BIGSERIAL PRIMARY KEY,
            osm_id CHARACTER VARYING,
            geo_feature CHARACTER VARYING NOT NULL,
            feature_type CHARACTER VARYING NOT NULL,
            wkb_geometry geometry(MultiLineString,4326) NOT NULL);
    
            INSERT INTO line_features 
            SELECT nextval('line_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'highway' AS geo_feature, 
                   t1.highway AS feature_type, 
                   t1.wkb_geometry
            FROM lines t1, 
                 (SELECT t.highway, count(t.highway) as count 
                  FROM lines t WHERE t.highway IS NOT NULL GROUP BY t.highway) t2
            WHERE t1.highway is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.highway = t2.highway and t2.count > 30
                    and t1.highway != 'yes';
        
            INSERT INTO line_features 
            SELECT nextval('line_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'waterway' AS geo_feature, 
                   t1.waterway AS feature_type, 
                   t1.wkb_geometry
            FROM lines t1, 
                 (SELECT t.waterway, count(t.waterway) as count 
                  FROM lines t WHERE t.waterway IS NOT NULL GROUP BY t.waterway) t2
            WHERE t1.waterway is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.waterway = t2.waterway and t2.count > 30
                    and t1.waterway != 'yes';
        
            INSERT INTO line_features 
            SELECT nextval('line_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'aerialway' AS geo_feature, 
                   t1.aerialway AS feature_type, 
                   t1.wkb_geometry
            FROM lines t1, 
                 (SELECT t.aerialway, count(t.aerialway) as count 
                  FROM lines t WHERE t.aerialway IS NOT NULL GROUP BY t.aerialway) t2
            WHERE t1.aerialway is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.aerialway = t2.aerialway and t2.count > 30
                    and t1.aerialway != 'yes';
        
            INSERT INTO line_features 
            SELECT nextval('line_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'man_made' AS geo_feature, 
                   t1.man_made AS feature_type, 
                   t1.wkb_geometry
            FROM lines t1, 
                 (SELECT t.man_made, count(t.man_made) as count 
                  FROM lines t WHERE t.man_made IS NOT NULL GROUP BY t.man_made) t2
            WHERE t1.man_made is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.man_made = t2.man_made and t2.count > 30
                    and t1.man_made != 'yes';

        DROP TABLE IF EXISTS point_features;
        CREATE TABLE point_features(
            gid BIGSERIAL PRIMARY KEY,
            osm_id CHARACTER VARYING,
            geo_feature CHARACTER VARYING NOT NULL,
            feature_type CHARACTER VARYING NOT NULL,
            wkb_geometry geometry(MultiPoint,4326) NOT NULL);
    
            INSERT INTO point_features 
            SELECT nextval('point_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'highway' AS geo_feature, 
                   t1.highway AS feature_type, 
                   t1.wkb_geometry
            FROM points t1, 
                 (SELECT t.highway, count(t.highway) as count 
                  FROM points t WHERE t.highway IS NOT NULL GROUP BY t.highway) t2
            WHERE t1.highway is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.highway = t2.highway and t2.count > 30
                    and t1.highway != 'yes';
        DROP TABLE IF EXISTS polygon_features;
        CREATE TABLE polygon_features(
            gid BIGSERIAL PRIMARY KEY,
            osm_id CHARACTER VARYING,
            geo_feature CHARACTER VARYING NOT NULL,
            feature_type CHARACTER VARYING NOT NULL,
            wkb_geometry geometry(MultiPolygon,4326) NOT NULL);
    
            INSERT INTO polygon_features 
            SELECT nextval('polygon_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'aeroway' AS geo_feature, 
                   t1.aeroway AS feature_type, 
                   t1.wkb_geometry
            FROM multipolygons t1, 
                 (SELECT t.aeroway, count(t.aeroway) as count 
                  FROM multipolygons t WHERE t.aeroway IS NOT NULL GROUP BY t.aeroway) t2
            WHERE t1.aeroway is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.aeroway = t2.aeroway and t2.count > 30
                    and t1.aeroway != 'yes';
        
            INSERT INTO polygon_features 
            SELECT nextval('polygon_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'amenity' AS geo_feature, 
                   t1.amenity AS feature_type, 
                   t1.wkb_geometry
            FROM multipolygons t1, 
                 (SELECT t.amenity, count(t.amenity) as count 
                  FROM multipolygons t WHERE t.amenity IS NOT NULL GROUP BY t.amenity) t2
            WHERE t1.amenity is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.amenity = t2.amenity and t2.count > 30
                    and t1.amenity != 'yes';
        
            INSERT INTO polygon_features 
            SELECT nextval('polygon_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'building' AS geo_feature, 
                   t1.building AS feature_type, 
                   t1.wkb_geometry
            FROM multipolygons t1, 
                 (SELECT t.building, count(t.building) as count 
                  FROM multipolygons t WHERE t.building IS NOT NULL GROUP BY t.building) t2
            WHERE t1.building is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.building = t2.building and t2.count > 30
                    and t1.building != 'yes';
        
            INSERT INTO polygon_features 
            SELECT nextval('polygon_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'landuse' AS geo_feature, 
                   t1.landuse AS feature_type, 
                   t1.wkb_geometry
            FROM multipolygons t1, 
                 (SELECT t.landuse, count(t.landuse) as count 
                  FROM multipolygons t WHERE t.landuse IS NOT NULL GROUP BY t.landuse) t2
            WHERE t1.landuse is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.landuse = t2.landuse and t2.count > 30
                    and t1.landuse != 'yes';
        
            INSERT INTO polygon_features 
            SELECT nextval('polygon_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'natural' AS geo_feature, 
                   t1.natural AS feature_type, 
                   t1.wkb_geometry
            FROM multipolygons t1, 
                 (SELECT t.natural, count(t.natural) as count 
                  FROM multipolygons t WHERE t.natural IS NOT NULL GROUP BY t.natural) t2
            WHERE t1.natural is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.natural = t2.natural and t2.count > 30
                    and t1.natural != 'yes';
        
            INSERT INTO polygon_features 
            SELECT nextval('polygon_features_gid_seq'::regclass), 
                   t1.osm_id, 
                   'tourism' AS geo_feature, 
                   t1.tourism AS feature_type, 
                   t1.wkb_geometry
            FROM multipolygons t1, 
                 (SELECT t.tourism, count(t.tourism) as count 
                  FROM multipolygons t WHERE t.tourism IS NOT NULL GROUP BY t.tourism) t2
            WHERE t1.tourism is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.tourism = t2.tourism and t2.count > 30
                    and t1.tourism != 'yes';
