# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:44
# description: telemac模型三角网格顶点基础信息实体类
import datetime
import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine, create_pg_connect

Base = declarative_base()


class TelMeshVertex(Base):
    __tablename__ = 'tel_mesh_vertex'  # 表名
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    vertex_code = Column(Integer)  # 顶点编码
    elevation = Column(Numeric(8, 3))  # 高程
    n = Column(Numeric(4, 2))  # 糙率
    geom = Column(String)  # 空间字段Point


def insert_tel_mesh_vertex(data_list):
    batch_size = 100
    if len(data_list) != 0:
        model_id = data_list[0][1]
        delete_by_model_id(model_id)
        sql = 'INSERT INTO tel_mesh_vertex(vertex_code, model_id, elevation, n, geom) VALUES (%s,%s,%s,%s,st_transform(st_geometryfromtext(%s,%s),4490))'
        db = create_pg_connect()
        cursor = db.cursor()
        i = 0
        while i < len(data_list):
            sub_list = data_list[i:i+batch_size]
            cursor.executemany(sql, sub_list)
            db.commit()
            i = i + batch_size
        cursor.close()
        db.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(TelMeshVertex).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True
def query_by_vertexCodeAndModelId(vertexCode,modelId):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(TelMeshVertex).filter_by(vertex_code = vertexCode).filter_by(model_id=modelId).first()

    session.close()
    return data
if __name__ == '__main__':
    res = query_by_vertexCodeAndModelId('120720','A4_420322_YX')
    print(res)
