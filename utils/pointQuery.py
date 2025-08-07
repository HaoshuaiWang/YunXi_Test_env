from app.utils.db_utils import create_pg_connect

def pointQuery(conn,longitude,latitude):
    cur = conn.cursor()
    # 查询栅格数据中每个波段的值
    # 4490-2000,4548-CGCS2000 / 3-degree Gauss-Kruger CM 117E
    # 转换经纬度坐标为平面坐标
    query_transform = """
    WITH transformed_point AS (
        SELECT ST_Transform(ST_SetSRID(ST_Point(%s, %s), 4490), 4548) AS point
    )
    SELECT 
        ST_AsText(point) AS wkt_4548
    FROM 
        transformed_point
    """

    cur.execute(query_transform, (longitude, latitude))
    result = cur.fetchone()
    print(result)
    exit()
    # ('POINT(406180.4665146114 4326168.345585344)',)
    query_raster_values = """
    WITH transformed_point AS (
        SELECT ST_Transform(ST_SetSRID(ST_Point(%s, %s), 4490), 4548) AS point
    ),
    bands AS (
        SELECT generate_series(1, (SELECT ST_Numbands(rast) FROM dem_data_test)) AS band
    )
    SELECT
        bands.band,
        ST_Value(dem_data_test.rast, bands.band, transformed_point.point) AS value
    FROM dem_data_test,transformed_point, bands WHERE ST_Intersects(dem_data_test.rast, transformed_point.point) ORDER BY bands.band
    """
    cur.execute(query_raster_values, (longitude, latitude))
    results = cur.fetchall()
    if results:
        for band, value in results:
            print(f"波段 {band} 的栅格值: {value}")
    else:
        print("没有找到对应的栅格值")

if __name__=='__main__':
    # 115.916,39.064  4548
    # 投影转换
    # 连接pg库
    conn = create_pg_connect()
    pointQuery(conn,115.916,39.064)

