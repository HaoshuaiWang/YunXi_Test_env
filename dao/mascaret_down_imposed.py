# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:26
# description: mascaret水位流量关系曲线实体类

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretDownImposed(Base):
    __tablename__ = 'mascaret_down_imposed'  # 表名

    model_id = Column(String(50), nullable=False)  # 模型编码
    rvcd = Column(String(20))  # 河流编码
    seccd = Column(String(10), primary_key=True, nullable=False, unique=True)  # 断面编码
    ptno = Column(Integer)  # 点序号
    z = Column(Numeric(7, 3))  # 水位(m)
    q = Column(Numeric(9, 3))  # 流量(m³/s)
    comments = Column(String(200))  # 备注


def insert_mascaret_down_imposed(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_down_imposed(model_id, rvcd, seccd, ptno, z, q, comments) "
                   "VALUES ('%s', '%s', '%s', %d, %f, %f, '%s');"
                   % (item.model_id, item.rvcd, item.seccd, item.ptno, item.z, item.q, item.comments))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_seccd(seccd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretDownImposed).filter_by(seccd=seccd)
    datas.delete()
    session.commit()
    session.close()
    return True


def query_by_seccd(seccd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretDownImposed).filter_by(seccd=seccd).all()
    session.close()
    return result
if __name__ == '__main__':
    # mas = MascaretDownImposed()
    # mas.model_id="test"
    # mas.rvcd="test"
    # mas.seccd="test"
    # mas.ptno=1
    # mas.z = 0.0
    # mas.q = 1.0
    # mas.comments = "test"
    # list=[]
    # list.append(mas)
    # insert_mascaret_down_imposed(list)
    # res = query_by_seccd(mas.seccd)
    # for attr, value in res[0].__dict__.items():
    #     print(attr, value)
    # delete_by_seccd("test")
    pass