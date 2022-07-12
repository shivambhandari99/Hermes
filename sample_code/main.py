import argparse
import time
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
import geopandas as gpd
from pyspark.sql.functions import broadcast

#import findspark
#findspark.add_packages('mysql:mysql-connector-java:8.0.11')


from sedona.register.geo_registrator import SedonaRegistrator
from pyspark.sql.types import StructType, IntegerType, DoubleType
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

from common_db import engine


schema_point = StructType() \
    .add("sensor_id", IntegerType(), False) \
    .add("lon", DoubleType(), False) \
    .add("lat", DoubleType(), False)


def gen_buffers(input_file, buffer_sizes):

    point_df = spark.read.option("header", True).schema(schema_point).csv(input_file)
    point_df.createOrReplaceTempView("points")
    a_df = spark.sql("SELECT sensor_id,lon,lat,1000 AS buffer_size,ST_TRANSFORM(ST_BUFFER(ST_Transform(ST_SetSRID(ST_POINT(lat,lon),4326),'epsg:4326','epsg:3857'),1000),'epsg:3857','epsg:4326') as buffer from points")
    #a_df.show()
    b_df = spark.sql("SELECT sensor_id,lon,lat,500 AS buffer_size,ST_TRANSFORM(ST_BUFFER(ST_Transform(ST_SetSRID(ST_POINT(lat,lon),4326),'epsg:4326','epsg:3857'),500),'epsg:3857','epsg:4326') as buffer from points")
    #b_df.show()
    c_df = spark.sql("SELECT sensor_id,lon,lat,100 AS buffer_size,ST_TRANSFORM(ST_BUFFER(ST_Transform(ST_SetSRID(ST_POINT(lat,lon),4326),'epsg:4326','epsg:3857'),100),'epsg:3857','epsg:4326') as buffer from points")
    #c_df.show()
    d_df = a_df.union(b_df)
    buffer_df = d_df.union(c_df)
    """ complete the function to generate buffers """
    #buffer_df = spark.sql("SELECT  ST_TRANSFORM(ST_Buffer(ST_Transform(ST_SetSRID(ST_POINT(lon, lat), 4326),3857),100),4326) from points")
    #buffer_df = spark.sql("SELECT * from points")
    buffer_df.createOrReplaceTempView("buffers")
    buffer_df.show()
    return buffer_df
    """
    counties_geom = spark.sql("SELECT buffer as geometry from buffers")

    df = counties_geom.toPandas()
    gdf = gpd.GeoDataFrame(df, geometry="geometry")

    gdf.plot(
        figsize=(10, 8),
        column="value",
        legend=True,
        cmap='YlOrBr',
        scheme='quantiles',
        edgecolor='lightgray')
"""

def gen_geographic_features(osm_table, out_path):

    osm_df = pd.read_sql(f"select geo_feature, feature_type, wkb_geometry from {osm_table}", engine)
    osm_df = spark.createDataFrame(osm_df).persist()
    osm_df.createOrReplaceTempView("osm")
    #osm_df.show()
    for col in osm_df.dtypes:
        print(col[0]+" , "+col[1])

    # print(osm_df.rdd.getNumPartitions())

    # compute geographic features for different geom 
    if osm_table == 'polygon_features':
        geographic_feature_df = spark.sql(f"""
            SELECT/*+ BROADCAST(buffers) */
            buffers.sensor_id,osm.geo_feature,osm.feature_type,buffers.buffer_size,ST_Area(ST_Transform(ST_FlipCoordinates(ST_Intersection(st_geomFromWKB(osm.wkb_geometry),ST_FlipCoordinates(buffers.buffer))),'epsg:4326','epsg:3857')) as value from buffers,osm where ST_Intersects(st_geomFromWKB(osm.wkb_geometry),ST_FlipCoordinates(buffers.buffer))""")
        #geographic_feature_df.show()
        geographic_feature_df = geographic_feature_df.groupBy("buffers.sensor_id", "osm.geo_feature", "osm.feature_type","buffers.buffer_size").sum('value')
        #geographic_feature_df.show()
        #geographic_feature_df = [Code Block]

    elif osm_table == 'line_features':

        geographic_feature_df = spark.sql(f"""
            SELECT/*+ BROADCAST(buffers) */
            buffers.sensor_id,osm.geo_feature,osm.feature_type,buffers.buffer_size,ST_Length(ST_Transform(ST_FlipCoordinates(ST_Intersection(st_geomFromWKB(osm.wkb_geometry),ST_FlipCoordinates(buffers.buffer))),'epsg:4326','epsg:3857')) as value from buffers,osm where ST_Intersects(st_geomFromWKB(osm.wkb_geometry),ST_FlipCoordinates(buffers.buffer))""")
        
        geographic_feature_df.show()
        geographic_feature_df = geographic_feature_df.groupBy("buffers.sensor_id", "osm.geo_feature", "osm.feature_type","buffers.buffer_size").sum('value')
        #geographic_feature_df.show()
        #exit()


    elif osm_table == "point_features":
        geographic_feature_df = spark.sql("SELECT sensor_id,geo_feature,feature_type,buffer_size,ST_Contains(ST_TRANSFORM(buffer,'epsg:4326','epsg:3857'), ST_TRANSFORM(ST_FlipCoordinates(st_geomFromWKB(wkb_geometry)),'epsg:4326','epsg:3857')) as value from buffers,osm where ST_Contains(buffer,ST_FlipCoordinates(st_geomFromWKB(wkb_geometry)))")
        geographic_feature_df = geographic_feature_df.groupBy("sensor_id", "geo_feature", "feature_type","buffer_size").count()
        #geographic_feature_df.show()
        #geographic_feature_df = [Code Block]

    else:
        raise NotImplementedError

    start_time = time.time()
    geographic_feature_df.coalesce(1).write.csv(f'{out_path}/{osm_table}_{int(time.time()/1000)}',
                                                header=True, sep=',')
    print(time.time() - start_time)
"""
    # the following code is for Windows if coalesce is not working
    #start_time = time.time()
    #geographic_feature = geographic_feature_df.collect()
    #print(geographic_feature)
    #print(time.time() - start_time)
    if(osm_table == "point_features"):
        with open(f'{out_path}/{osm_table}_{int(time.time())}.csv', 'w') as f:
            f.write("sensor_id,geo_features,feature_type,buffer_size,value\n")
            for row in geographic_feature:
                f.write(f'{row["sensor_id"]},{row["geo_feature"]},{row["feature_type"]},{row["buffer_size"]},{row["count"]}\n')
    else:
        with open(f'{out_path}/{osm_table}_{int(time.time())}.csv', 'w') as f:
            f.write("sensor_id,geo_features,feature_type,buffer_size,value\n")
            for row in geographic_feature:
                f.write(f'{row["sensor_id"]},{row["geo_feature"]},{row["feature_type"]},{row["buffer_size"]},{row["sum"]}\n')

"""

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, default='data/ca_purple_air_locations_subset.csv',
                        help='The path to the location file.')
    parser.add_argument('--osm_table', type=str, default='polygon_features',
                        help='The OSM table to query.')
    parser.add_argument('--out_path', type=str, default='./results',
                        help='The output folder path.')
    args = parser.parse_args()

    conf = SparkConf(). \
        setMaster("local[*]"). \
        set("spark.executor.memory", '4g'). \
        set("spark.driver.memory", '16g')

    spark = SparkSession. \
        builder. \
        appName("hw1"). \
        config(conf=conf). \
        config("spark.serializer", KryoSerializer.getName). \
        config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
        getOrCreate()

    SedonaRegistrator.registerAll(spark)

    buffer_df = gen_buffers(args.input_file, buffer_sizes=[100, 500, 1000])
    gen_geographic_features(osm_table=args.osm_table, out_path=args.out_path)

    spark.stop()

