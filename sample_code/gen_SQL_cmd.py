""" This script is to generate the SQL queries to re-organize OSM tables,
    by selecting specific geo-features for each geometry type (points, lines, polygons)
"""

f = open('sample_code/osm.sql', 'w')

LINE_FEATURES = ["highway", "waterway", "aerialway", "man_made"]
LINE_TABLE_NAME = "lines"
NEW_LINE_TABLE_NAME = "line_features"

POINT_FEATURES = ["highway"]
POINT_TABLE_NAME = "points"
NEW_POINT_TABLE_NAME = "point_features"

POLYGON_FEATURES = ["aeroway", "amenity", "building", "landuse", "natural", "tourism"]
POLYGON_TABLE_NAME = "multipolygons"
NEW_POLYGON_TABLE_NAME = "polygon_features"

for (features, table_name, new_table_name, geom) in \
        [(LINE_FEATURES, LINE_TABLE_NAME, NEW_LINE_TABLE_NAME, "MultiLineString"),
         (POINT_FEATURES, POINT_TABLE_NAME, NEW_POINT_TABLE_NAME, "MultiPoint"),
         (POLYGON_FEATURES, POLYGON_TABLE_NAME, NEW_POLYGON_TABLE_NAME, "MultiPolygon")]:

    create_table = f"""
        DROP TABLE IF EXISTS {new_table_name};
        CREATE TABLE {new_table_name}(
            gid BIGSERIAL PRIMARY KEY,
            osm_id CHARACTER VARYING,
            geo_feature CHARACTER VARYING NOT NULL,
            feature_type CHARACTER VARYING NOT NULL,
            wkb_geometry geometry({geom},4326) NOT NULL);
    """

    insert_table = ""
    for feature in features:

        sql = f"""
            INSERT INTO {new_table_name} 
            SELECT nextval('{new_table_name}_gid_seq'::regclass), 
                   t1.osm_id, 
                   '{feature}' AS geo_feature, 
                   t1.{feature} AS feature_type, 
                   t1.wkb_geometry
            FROM {table_name} t1, 
                 (SELECT t.{feature}, count(t.{feature}) as count 
                  FROM {table_name} t WHERE t.{feature} IS NOT NULL GROUP BY t.{feature}) t2
            WHERE t1.{feature} is not NULL and ST_IsValid(t1.wkb_geometry) 
                    and t1.{feature} = t2.{feature} and t2.count > 30
                    and t1.{feature} != 'yes';
        """
        insert_table += sql

    f.writelines(create_table)
    for i in insert_table:
        f.writelines(i)

    # index_table = f"""CREATE INDEX "{new_table_name}_wkb_geometry_geom_idx"
    #                   ON {new_table_name} USING gist(wkb_geometry);"""
    # f.writelines(index_table)
