
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretCrossLine(Base):
    __tablename__ = 'mascaret_cross_line'  # 表名
    id = Column(Integer, primary_key=True, nullable=False)  # 空间编码
    model_id = Column(String(50), nullable=False)  # 模型编码
    rvcd = Column(String(20), nullable=False)  # 河流编码
    rvnm = Column(String(20))  # 河流名称
    seccd = Column(String(36), nullable=False)  # 断面编码
    secnum = Column(String(30))  # 断面桩号
    geom = Column(String)  # 空间字段Polyline


def insert_mascaret_cross_line(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_cross_line(id, model_id, rvcd, rvnm, seccd, secnum, geom) "
                   "VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');"
                   % (item.id, item.model_id, item.rvcd, item.rvnm, item.seccd, item.secnum, item.geom))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_id(id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretCrossLine).filter_by(id=id)
    datas.delete()
    session.commit()
    session.close()
    return True


def query_by_id(id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretCrossLine).filter_by(id=id).first()
    session.close()
    return result
if __name__ == '__main__':
    # res = query_by_id(1)
    # for attr, value in res.__dict__.items():
    #     print(attr, value)
    # mas =MascaretCrossLine()
    # mas.id = 9999
    # mas.rvcd = "RV"
    # mas.seccd = "SEC"
    # mas.secnum = "SEC"
    # mas.geom = "MULTILINESTRING((110.372765203069 33.0506853221842, 110.365099531294 33.0530917941344))"
    # list=[]
    # list.append(mas)
    # insert_mascaret_cross_line(list)
    # re1 = query_by_id(9999)
    # for attr, value in re1.__dict__.items():
    #     print(attr, value)
    delete_by_id(999)
    delete_by_id(9999)
