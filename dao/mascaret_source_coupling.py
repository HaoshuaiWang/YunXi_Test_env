# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:26
# description: mascaret一维模型耦合关系实体类

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretSourceCoupling(Base):
    __tablename__ = 'mascaret_source_coupling'  # 表名

    model_id = Column(String(50), primary_key=True, nullable=False)  # 模型编码
    rvcd = Column(String(20))  # 河流编码
    seccd = Column(String(36), primary_key=True, nullable=False)  # 断面编码
    type_code = Column(String(10), nullable=False)  # 节点类型
    in_code = Column(Numeric(12, 6), nullable=False)  # 入流节点编码


def insert_mascaret_source_coupling(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_source_coupling(model_id, rvcd, seccd, type_code, in_code) "
                   "VALUES ('%s', '%s', '%s', '%s', %f);"
                   % (item.model_id, item.rvcd, item.seccd, item.type_code, item.in_code))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretSourceCoupling).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True


def query_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretSourceCoupling).filter_by(model_id=model_id).all()
    session.close()
    return result
if __name__ == '__main__':
    # list=[]
    # mas = MascaretSourceCoupling()
    # mas.model_id = 'mascaret_source_coupling'
    # mas.rvcd="test"
    # mas.seccd="test"
    # mas.type_code="test"
    # mas.in_code=0.000
    # list.append(mas)
    # insert_mascaret_source_coupling(list)
    # res = query_by_model_id(mas.model_id)
    # for attr,value in res[0].__dict__.items():
    #     print(attr,value)
    # delete_by_model_id("mascaret_source_coupling")
    pass