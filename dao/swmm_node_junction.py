# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 11:46
# description: swmm模型节点实体类

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmNodeJunction(Base):
    __tablename__ = 'swmm_node_junction'  # 表名
    node_id = Column(String(50), primary_key=True, nullable=True)  # 节点编码
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    description = Column(String(255))  # 描述
    tag = Column(String(50))  # 标签
    elevation = Column(Numeric(8, 3))  # 海拔高程
    max_depth = Column(Numeric(8, 3))  # 最大深度
    init_depth = Column(Numeric(8, 3))  # 初始深度
    sur_depth = Column(Numeric(8, 3))  # 允许超过深度
    ponded_area = Column(Numeric(8, 3))  # 积水面积
    geom = Column(String)  # 空间字段Point


def insert_swmm_node_junction(data_list):
    if len(data_list) != 0:
        model_id = data_list[0].model_id
        delete_by_model_id(model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO swmm_node_junction(node_id, model_id,elevation,max_depth,init_depth,sur_depth,ponded_area, geom)"
                   " VALUES ('%s','%s',%f,%f,%f,%f,%f,%s);"
                   % (item.node_id, item.model_id, item.elevation, item.max_depth, item.init_depth, item.sur_depth, item.ponded_area, item.geom))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(SwmmNodeJunction).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True


# 查询二维顶点最近的一维节点
def query_nearest_node(model_id, vertex_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    sql = ("SELECT a.node_id,st_distance(a.geom, b.geom) FROM swmm_node_junction a, tel_mesh_vertex b "
           "WHERE b.vertex_code =%s AND b.model_id ='%s' ORDER BY st_distance") % (vertex_code, model_id)
    data = session.execute(text(sql))
    node_id = data.first().node_id
    session.commit()
    session.close()
    return node_id
def query_nearest_vertexCode(model_id, node_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    sql = ("SELECT b.vertex_code,st_distance(a.geom, b.geom) FROM swmm_node_junction a, tel_mesh_vertex b "
           "WHERE  a.node_id='%s' AND a.model_id ='%s' ORDER BY st_distance") % (node_id, model_id)
    data = session.execute(text(sql))
    vertex_code = data.first().vertex_code
    session.commit()
    session.close()
    return vertex_code
def query_swmm_node_junction_info(modelId):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmNodeJunction).filter_by(model_id = modelId).all()

    session.close()
    return data
if __name__ == '__main__':
    # res = query_nearest_node("A11_420322_WLH",49993)
    # res = query_nearest_vertexCode("A4_420322_YX","HS7586")
    res = query_swmm_node_junction_info('A4_420322_YX')
    print(res)