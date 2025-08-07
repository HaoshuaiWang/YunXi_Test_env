# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:02
# description: telemac模型点源耦合关系表


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class TelSourceCoupling(Base):
    __tablename__ = 'tel_source_coupling'  # 表名
    fid = Column(Integer, primary_key=True)  # 自增编码
    model_id = Column(String(50), nullable=True)  # 模型编码
    node_code = Column(String(50))  # 一维节点编码
    vertex_code = Column(Integer)  # 二维顶点编码
    coup_type = Column(String(20))

def insert_tel_source_coupling(source_list):
    if source_list is not None:
        delete_tel_source_coupling(source_list[0].model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add_all(source_list)
        session.commit()
        session.close()


def delete_tel_source_coupling(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(TelSourceCoupling).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()


def query_tel_source_coupling(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(TelSourceCoupling).filter_by(model_id=model_id)
    session.commit()
    session.close()
    return datas.all()
