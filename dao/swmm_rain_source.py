# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 15:31
# description: swmm雨量计输入数据实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric, DateTime
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmRainSource(Base):
    __tablename__ = 'swmm_rain_source'  # 表名
    task_code = Column(String(36), primary_key=True, nullable=True)  # 方案编码
    source_name = Column(String(50), primary_key=True, nullable=True)  # 雨量计编码
    time = Column(DateTime, primary_key=True, nullable=True)  # 时间
    value = Column(Numeric(8, 2))  # 降雨值


# 查询任务开始时间
def query_start_time(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmRainSource).filter_by(task_code=task_code).order_by(SwmmRainSource.time).first()
    session.close()
    return data.time


# 查询任务结束时间
def query_end_time(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmRainSource).filter_by(task_code=task_code).order_by(SwmmRainSource.time.desc()).first()
    session.close()
    return data.time


# 查询降雨数据
def query_rain_source(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(SwmmRainSource).filter_by(task_code=task_code).order_by(SwmmRainSource.time.asc())
    session.close()
    return data.all()

