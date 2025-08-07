# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:26
# description: swmm模型管道实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmLinkConduit(Base):
    __tablename__ = 'swmm_link_conduit'  # 表名
    link_id = Column(String(50), primary_key=True, nullable=True)  # 管道编码
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    from_node = Column(String(50))  # 流入节点
    to_node = Column(String(50))  # 流出节点
    description = Column(String(255))  # 描述
    tag = Column(String(50))  # 标签
    length = Column(Numeric(8, 3))  # 管道长度（m）
    roughness = Column(Numeric(6, 3))  # 糙率
    in_offset = Column(Numeric(6, 3))  # 入流偏移（m）
    out_offset = Column(Numeric(6, 3))  # 出流偏移（m）
    init_flow = Column(Numeric(6, 3))  # 初始流量
    max_flow = Column(Numeric(6, 3))  # 最大流量
    geom = Column(String)  # 空间字段PolyLine


def insert_swmm_link_conduit(data_list):
    if len(data_list) != 0:
        model_id = data_list[0].model_id
        delete_by_model_id(model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO swmm_link_conduit(link_id,model_id,from_node,to_node,length,roughness,in_offset,out_offset,init_flow,max_flow,geom)"
                   " VALUES ('%s','%s','%s','%s',%f,%f,%f,%f,%f,%f,%s);"
                   % (item.link_id, item.model_id, item.from_node, item.to_node, item.length, item.roughness,
                      item.in_offset, item.out_offset, item.init_flow, item.max_flow, item.geom))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(SwmmLinkConduit).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True

