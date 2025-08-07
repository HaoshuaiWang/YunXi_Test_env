# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:40
# description: telemac模型基础信息实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class TelModelInfo(Base):
    __tablename__ = 'tel_model_info'  # 表名
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    model_path = Column(String(255))  # 模型文件路径
    steering_file = Column(String(50))  # 模型文件名称
    geometry_file = Column(String(50))  # 几何文件名称
    source_file = Column(String(50))  # 点源文件名称
    boundary_file = Column(String(50))  # 边界文件名称
    result_file = Column(String(50))  # 结果文件名称
    initial_file = Column(String(50)) # 初始边界条件名称
    liq_boundary_file = Column(String(50))
    coupling_type = Column(String(50))
    relation_file = Column(String(50))

def insert_tel_model_info(data: TelModelInfo):
    if data is not None:
        delete_tel_model_info(data.model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add(data)
        session.commit()
        session.close()


def delete_tel_model_info(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(TelModelInfo).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()


def query_tel_model_info(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(TelModelInfo).filter_by(model_id=model_id).first()
    session.close()
    return datas
