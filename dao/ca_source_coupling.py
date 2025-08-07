# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024-9-10 13:38:04
# description: 元胞计算器一二维耦合实体类

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class CaSourceCoupling(Base):
    __tablename__ = 'ca_source_coupling'  # 表名
    model_id = Column(String(50), primary_key=True)  # 模型编码
    node_id = Column(String(50), primary_key=True)   # 节点编码
    x = Column(Numeric(16, 6))                       # 投影坐标x
    y = Column(Numeric(16, 6))                       # 投影坐标y


# 查询二维模型工作空间
def query_ca_coupling(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(CaSourceCoupling).filter_by(model_id=model_id).all()
    data_list = [{'model_id': item.model_id, 'node_id': item.node_id, 'x': item.x, 'y': item.y} for item in data]
    session.commit()
    session.close()
    return data_list


