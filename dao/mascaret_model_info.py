

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretModelInfo(Base):
    __tablename__ = 'mascaret_model_info'  # 表名
    model_id = Column(String(30), nullable=True)  # 模型编码
    rvcd = Column(String(20), primary_key=True, nullable=False, unique=True)  # 河流编码
    start_seccd = Column(String(36), nullable=False)  # 起始断面编码
    end_seccd = Column(String(50), nullable=False)  # 终止断面编码


def insert_mascaret_model_info(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_model_info(model_id, rvcd, start_seccd, end_seccd) "
                   "VALUES ('%s', '%s', '%s', '%s');"
                   % (item.model_id, item.rvcd, item.start_seccd, item.end_seccd))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_rvcd(rvcd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretModelInfo).filter_by(rvcd=rvcd)
    datas.delete()
    session.commit()
    session.close()
    return True


def query_by_rvcd(rvcd):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretModelInfo).filter_by(rvcd=rvcd).first()
    session.close()
    return result
