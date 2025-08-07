

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretCrossInfo(Base):
    __tablename__ = 'mascaret_cross_info'  # 表名
    id = Column(Integer, primary_key=True)  # 新增代理主键
    rvcd = Column(String(20), nullable=False)  # 河流编码
    rvnm = Column(String(30), nullable=False)  # 河流名称
    model_id = Column(String(30), nullable=False)  # 模型编码
    seccd = Column(String(36), nullable=False)  # 断面编码
    secnm = Column(String(50), nullable=False)  # 断面名称
    di = Column(Numeric(8, 2), nullable=False)  # 起点距(m)
    zb = Column(Numeric(8, 3), nullable=False)  # 高程(m)
    x = Column(Numeric(12, 3))  # 投影坐标x(m)
    y = Column(Numeric(12, 3))  # 投影坐标y(m)


def insert_mascaret_cross_info(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_cross_info(rvcd, rvnm, model_id, seccd, secnm, di, zb, x, y) "
                   "VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f);"
                   % (item.rvcd, item.rvnm, item.model_id, item.seccd, item.secnm,
                      item.di, item.zb, item.x, item.y))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_seccd(seccd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretCrossInfo).filter_by(seccd=seccd)
    datas.delete()
    session.commit()
    session.close()
    return True

def delete_by_rvcd(rvcd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretCrossInfo).filter_by(rvcd=rvcd)
    datas.delete()
    session.commit()
    session.close()
    return True
def query_by_seccd(seccd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretCrossInfo).filter_by(seccd=seccd).all()
    session.close()
    return result
def query_by_rvcd(rvcd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretCrossInfo).filter_by(rvcd=rvcd).all()
    session.close()
    return result

def print_mascaret_cross(cross):
    """打印MascaretCrossInfo类的属性值"""
    attrs = [
        'rvcd', 'rvnm', 'model_id', 'seccd', 'secnm',
        'di', 'zb', 'x', 'y'
    ]
    for attr in attrs:
        print(f"{attr}: {getattr(cross, attr)}")
if __name__ == '__main__':
    # res = query_by_seccd("HH01")
    # for attr, value in res[0].__dict__.items():
    #     print(f"{attr}: {value}")
    #
    # mas = MascaretCrossInfo()
    # mas.rvcd = "test"
    # mas.rvnm = "test"
    # mas.model_id = "test"
    # mas.seccd = "test"
    # mas.secnm = ("t")
    # mas.di = 0.0
    # mas.zb = 0.0
    # mas.x = 0.0
    # mas.y = 0.0
    # list = []
    # list.append(mas)
    # insert_mascaret_cross_info(list)
    # new = query_by_seccd("test")
    # for attr, value in new[0].__dict__.items():
    #     print(f"{attr}: {value}")

    # delete_by_seccd("test")
    pass