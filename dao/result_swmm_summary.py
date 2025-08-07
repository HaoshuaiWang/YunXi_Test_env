# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:18
# description: swmm一维模型结果统计实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultSwmmSummary(Base):
    __tablename__ = 'result_swmm_summary'  # 表名
    task_code = Column(String(36), primary_key=True, nullable=True)  # 方案编码
    version = Column(Integer)  # 模型版本
    flow_units = Column(String(10))  # 流量单位系统
    pollutant_units = Column(String(10))  # 污染单位系统
    system_units = Column(String(10))  # 污染单位系统
    start_time = Column(DateTime)  # 开始时间
    end_time = Column(DateTime)  # 结束时间
    interval = Column(Integer)  # 时间步长
    links_count = Column(Integer)  # 连接数量
    node_count = Column(Integer)  # 节点数量
    pollutants_count = Column(Integer)  # 污染数量
    sub_catchments = Column(Integer)  # 子汇水区数量


def insert_swmm_summary(data: ResultSwmmSummary):
    if data is not None:
        delete_swmm_summary(data.task_code)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add(data)
        session.commit()
        session.close()


def delete_swmm_summary(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(ResultSwmmSummary).filter_by(task_code=task_code)
    datas.delete()
    session.commit()
    session.close()

