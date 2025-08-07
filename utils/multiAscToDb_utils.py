# -*- coding: utf-8 -*-
from app.utils.db_utils import create_pg_engine, create_pg_connect
import  time
from fastapi import FastAPI

def read_asc_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    header = {}
    for i in range(6):
        key, value = lines[i].strip().split()
        header[key] = float(value)
    data = []
    for line in lines[6:]:
        row = [float(x) for x in line.strip().split()]
        data.append(row)
    print(header)
    # print(data)
    return header, data

def create_raster_from_array(header, data, conn):
    cur = conn.cursor()
    ncols = int(header['ncols'])
    nrows = int(header['nrows'])
    xllcorner = header['xllcorner']
    yllcorner = header['yllcorner']
    cellsize = header['cellsize']
    # 创建空栅格
    create_raster_sql = """
    SELECT ST_MakeEmptyRaster(
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) AS rast
    """
    cur.execute(create_raster_sql, (
        ncols, nrows,
        xllcorner, yllcorner,cellsize
        , -cellsize,
        0, 0, 4326  # SRID
    ))
    empty_raster = cur.fetchone()[0]
    print('创建空栅格成功！')
    # 设置栅格值
    set_values_sql = """
    SELECT ST_SetValues(
        %s, 1, 1, 1, %s::double precision[]
    ) AS rast
    """
    cur.execute(set_values_sql, (empty_raster, data))
    raster_with_values = cur.fetchone()[0]
    # 插入到数据库
    insert_sql = """
    INSERT INTO dem_data_test (id, rast)
    VALUES (%s, %s)
    """
    cur.execute(insert_sql, (1, raster_with_values))
    conn.commit()
    cur.close()
    conn.close()


def create_multiband_raster_from_arrays(headers, datas, conn):
    cur = conn.cursor()
    # 假设所有 ASC 文件的元数据相同
    header = headers[0]
    ncols = int(header['ncols'])
    nrows = int(header['nrows'])
    xllcorner = header['xllcorner']
    yllcorner = header['yllcorner']
    cellsize = header['cellsize']

    # 创建空栅格
    create_raster_sql = """
       SELECT ST_MakeEmptyRaster(
           %s, %s, %s, %s, %s, %s, %s, %s, %s
       ) AS rast
       """
    # 这是原来的设置
    # cur.execute(create_raster_sql, (ncols, nrows, xllcorner, yllcorner, cellsize, -cellsize, 0, 0, 4548))
    cur.execute(create_raster_sql, ( ncols, nrows,xllcorner, yllcorner + nrows * cellsize, cellsize, -cellsize,0, 0, 4548))
    empty_raster = cur.fetchone()[0]
    print('创建空栅格成功！')
    # 添加多个空带
    add_band_sql = """
    SELECT ST_AddBand(
        %s, %s, '32BF', 0, 0
    ) AS rast
    """
    for _ in range(len(datas)):
        cur.execute(add_band_sql, (empty_raster, 1))
        empty_raster = cur.fetchone()[0]
    # 设置每个波段的值
    set_values_sql = """
    SELECT ST_SetValues(
        %s, %s, 1, 1, %s::double precision[]
    ) AS rast
    """
    for band, data in enumerate(datas, start=1):
        cur.execute(set_values_sql, (empty_raster, band, data))
        empty_raster = cur.fetchone()[0]

    # 插入到数据库
    insert_sql = """
    INSERT INTO dem_data_test (id, rast)
    VALUES (%s, %s)
    """
    cur.execute(insert_sql, (2, empty_raster))
    conn.commit()
    cur.close()
    conn.close()

# 创建多波段栅格并插入到数据库
#     create_multiband_raster_from_arrays(headers, datas, connection_params)

if __name__ == '__main__':
    print('开始插入数据')
    start_time = time.time()
    # 读取asc文件
    # file_paths=[r'G:\雄安内涝\temp\rongdong1.asc']
    file_paths = [r'G:\雄安内涝\temp\rongdong1.asc', r'G:\雄安内涝\temp\rongdong2.asc',r'G:\雄安内涝\temp\rongdong3.asc']
    
    headers = []
    datas = []
    for file_path in file_paths:
        header, data = read_asc_file(file_path)
        headers.append(header)
        datas.append(data)
    # 连接pg库
    conn=create_pg_connect()
    # 创建空栅格并插入数据
    create_multiband_raster_from_arrays(headers, datas ,conn)
    print('结束插入数据')
    end_time = time.time()
    # 计算时间差
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.6f} seconds")
    # 关闭数据库连接
    conn.close()



