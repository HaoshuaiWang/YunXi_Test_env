# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:24
# description: swmm模型排水口实体类
import json

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmNodeOutfall(Base):
    __tablename__ = 'swmm_node_outfall'  # 表名
    node_id = Column(String(50), primary_key=True, nullable=True)  # 节点编码
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    description = Column(String(255))  # 描述
    tag = Column(String(50))  # 标签
    elevation = Column(Numeric(8, 3))  # 海拔高程
    type = Column(String(50))  # 出流类型
    geom = Column(String)  # 空间字段Point


def insert_swmm_node_outfall(data_list):
    if len(data_list) != 0:
        model_id = data_list[0].model_id
        delete_by_model_id(model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO swmm_node_outfall(node_id,model_id,elevation,type,geom)"
                   " VALUES ('%s','%s',%f,'%s',%s);"
                   % (item.node_id, item.model_id, item.elevation, item.type, item.geom))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(SwmmNodeOutfall).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True
def query_nearest_vertexCode(model_id, node_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    sql = ("SELECT b.vertex_code,st_distance(a.geom, b.geom) FROM swmm_node_outfall a, tel_mesh_vertex b "
           "WHERE  a.node_id='%s' AND a.model_id ='%s' ORDER BY st_distance") % (node_id, model_id)
    data = session.execute(text(sql))
    vertex_code = data.first().vertex_code
    session.commit()
    session.close()
    return vertex_code
def query_swmm_node_outfall_info(modelId):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmNodeOutfall).filter_by(model_id = modelId).all()

    session.close()
    return data
def read_junction_geojson():
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    sql = ("""
    SELECT jsonb_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(snj.geom)::jsonb,
            'properties', to_jsonb(tsc) - 'geom' -- 排除几何字段避免重复
            ) AS geojson_feature
            FROM tel_source_coupling tsc
            JOIN swmm_node_junction snj ON tsc.node_code = snj.node_id
            WHERE tsc.model_id = 'A4_420322_YX'
            AND snj.model_id = 'A4_420322_YX'
    """ )
    data = session.execute(text(sql))
    data = data.all()

    # 提取geojson_feature字段的值，并转换为字符串
    geojson_features = [str(row.geojson_feature) for row in data]

    # 用逗号将所有geojson字符串连接起来
    result_str = ','.join(geojson_features)
    session.commit()
    session.close()
    return geojson_features

def read_vertex_geojson():
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    sql = ("""
    SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(snj.geom)::jsonb,
        'properties', to_jsonb(tsc) - 'geom' -- 排除几何字段避免重复
        ) AS geojson_feature
        FROM tel_source_coupling tsc
        JOIN tel_mesh_vertex snj ON tsc.vertex_code = snj.vertex_code
        WHERE tsc.model_id = 'A4_420322_YX'
        AND snj.model_id = 'A4_420322_YX';
    """ )
    data = session.execute(text(sql))
    data = data.all()

    # 提取geojson_feature字段的值，并转换为字符串
    geojson_features = [str(row.geojson_feature) for row in data]

    # 用逗号将所有geojson字符串连接起来
    result_str = ','.join(geojson_features)
    session.commit()
    session.close()
    return geojson_features
if __name__ == '__main__':
    # res = query_nearest_node("A11_420322_WLH",49993)
    # res = query_nearest_vertexCode("A4_420322_YX","YS3635")
    # print(res)
    res = str(read_vertex_geojson())
    # res =res.replace('(', '').replace(')', '').replace('\'','\"').replace(',,',',')
    res = res.replace('\"',"").replace('\'','\"')
    with open('tel_mesh_vertex.txt', 'w') as f:
        f.write(str(res))
    print(res)