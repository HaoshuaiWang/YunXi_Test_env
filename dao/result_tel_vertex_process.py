# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024年8月15日09:38:28
# description: telemac顶点时序数据存储表


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine, create_pg_connect

Base = declarative_base()


class VertexProcessElement(Base):
    __tablename__ = 'result_tel_vertex_process'  # 表名
    task_code = Column(String(36), primary_key=True, nullable=True)  # 任务彪马
    vertex_code = Column(Integer, primary_key=True, nullable=True)  # 顶点编码
    time = Column(Integer, primary_key=True, nullable=True)  # 时间步长
    h = Column(Numeric(8, 3))  # 淹没水深（m）
    u = Column(Numeric(8, 3))  # 流速u(m/s)
    v = Column(Numeric(8, 3))  # 流速v(m/s)
    speed = Column(Numeric(8, 3))  # 合并流速
    direction = Column(Numeric(8, 3))  # 方向
    z = Column(Numeric(8, 3))  # 水位 = 水深+dem（m）


def insert_vertex_process(data_list):
    batch_size = 50000
    if len(data_list) != 0:
        task_code = data_list[0][0]
        delete_by_task_code(task_code)
        sql = 'INSERT INTO result_tel_vertex_process(task_code, vertex_code, time, h, u, v, speed, direction, z) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        db = create_pg_connect()
        cursor = db.cursor()
        i = 0
        while i < len(data_list):
            sub_list = data_list[i:i+batch_size]
            cursor.executemany(sql, sub_list)
            db.commit()
            i = i + batch_size
        cursor.close()
        db.close()


def delete_by_task_code(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(VertexProcessElement).filter_by(task_code=task_code)
    datas.delete()
    session.commit()
    session.close()
    return True
