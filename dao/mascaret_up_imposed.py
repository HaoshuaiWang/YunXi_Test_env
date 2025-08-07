
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class MascaretUpImposed(Base):
    __tablename__ = 'mascaret_up_imposed'  # 表名
    id = Column(Integer, primary_key=True)  # 新增代理主键
    model_id = Column(String(50), nullable=False)  # 模型编码
    task_code = Column(String(50), nullable=False)  # 任务编码
    rvcd = Column(String(20), nullable=False)  # 河流编码
    seccd = Column(String(36), nullable=False)  # 断面编码
    time = Column(Integer)  # 时间序列
    discharge = Column(Numeric(10, 2))  # 流量序列(m3/s)


def insert_mascaret_up_imposed(data_list):
    if len(data_list) != 0:
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO mascaret_up_imposed(model_id, task_code, rvcd, seccd, time, discharge) "
                   "VALUES ('%s', '%s', '%s', '%s', %d, %f);"
                   % (item.model_id, item.task_code, item.rvcd, item.seccd, item.time, item.discharge))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(MascaretUpImposed).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True


def query_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    result = session.query(MascaretUpImposed).filter_by(model_id=model_id).all()
    session.close()
    return result
if __name__ == '__main__':
    # mas =MascaretUpImposed()
    # mas.model_id= 'test'
    # mas.task_code = 'test'
    # mas.rvcd = 'test'
    # mas.seccd = 'test'
    # mas.time = 1
    # mas.discharge = 0.0
    # list = []
    # list.append(mas)
    # insert_mascaret_up_imposed(list)
    # query = query_by_model_id(mas.model_id)
    # for attr,values in query[0].__dict__.items():
    #     print(attr, values)
    # delete_by_model_id("test")
    pass
