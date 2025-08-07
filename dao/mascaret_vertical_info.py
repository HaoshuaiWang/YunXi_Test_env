

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretVerticalInfo(Base):
    __tablename__ = 'mascaret_vertical_info'  # 表名
    rvcd = Column(String(20), primary_key=True, nullable=False, unique=True)  # 河流编码
    rvnm = Column(String(30), nullable=False)  # 河流名称
    model_id = Column(String(30))  # 模型编码
    seccd = Column(String(36), nullable=False)  # 断面编码
    secnm = Column(String(50), nullable=False)  # 断面名称
    secid = Column(Numeric(10, 2), nullable=False)  # 断面桩号(m)
    zb = Column(Numeric(8, 2), nullable=False)  # 河底高程(m)
    zleft = Column(Numeric(8, 2), nullable=False)  # 左岸高程(m)
    zright = Column(Numeric(8, 2), nullable=False)  # 右岸高程(m)
    x = Column(Numeric(12, 3))  # 深泓点投影坐标x(m)
    y = Column(Numeric(12, 3))  # 深泓点投影坐标y(m)


def insert_mascaret_vertical_info(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_vertical_info(rvcd, rvnm, model_id, seccd, secnm, secid, zb, zleft, zright, x, y) "
                   "VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f, %f, %f);"
                   % (item.rvcd, item.rvnm, item.model_id, item.seccd, item.secnm,
                      item.secid, item.zb, item.zleft, item.zright, item.x, item.y))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_rvcd(rvcd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretVerticalInfo).filter_by(rvcd=rvcd)
    datas.delete()
    session.commit()
    session.close()
    return True


def query_by_rvcd(rvcd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretVerticalInfo).filter_by(rvcd=rvcd).first()
    session.close()
    return result
if __name__ == '__main__':
    pass
    # re = query_by_rvcd("AFG24006")
    # for attr, value in re.__dict__.items():
    #     print(attr, value)
    # test = MascaretVerticalInfo()
    # test.rvcd = "test"
    # test.rvnm = "test"
    # test.model_id = "test"
    # test.seccd = "test"
    # test.secnm = "t"
    # test.secid = 0.0
    # test.zb = 0.0
    # test.zleft = 0.0
    # test.zright = 0.0
    # test.x = 0.0
    # test.y = 0.0
    # mas = []
    # mas.append(test)
    # insert_mascaret_vertical_info(mas)
    # query = query_by_rvcd("test")
    # for attr, value in query.__dict__.items():
    #     print(attr, value)
    # delete_by_rvcd("test")
