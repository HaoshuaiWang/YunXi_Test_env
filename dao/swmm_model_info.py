# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 11:20
# description: swmm模型基础信息实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmModelInfo(Base):
    __tablename__ = 'swmm_model_info'  # 表名
    model_id = Column(String(50), primary_key=True)  # 模型编码
    srid = Column(String(20), nullable=True)  # 坐标系id
    flow_unit = Column(String(20))  # 流量单位系统
    routing_model = Column(String(20))  # 管网模型
    infiltration_model = Column(String(20))  # 渗流模型
    allow_ponding = Column(String(20))  # 允许积水
    mini_conduit_slope = Column(Numeric(6, 4))  # 最小管道坡度
    inp_path = Column(String(255))  # 模型文件路径


def insert_swmm_model_info(data: SwmmModelInfo):
    if data is not None:
        delete_swmm_model_info(data.model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add(data)
        session.commit()
        session.close()


def delete_swmm_model_info(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(SwmmModelInfo).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()


def query_inp_path(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmModelInfo).filter_by(model_id=model_id).first()
    inp_path = data.inp_path
    print(inp_path)
    session.commit()
    session.close()
    return inp_path


def query_swmm_model_info():
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmModelInfo).all()
    # print(data[0].swmm_id)
    datas = session.query(SwmmModelInfo).filter_by(model_id='111').first()
    # print(datas.swmm_id)
    session.close()
    return data