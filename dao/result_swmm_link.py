# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:41
# description: swmm一维模型管段实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultSwmmLink(Base):
    __tablename__ = 'result_swmm_link'  # 表名
    link_id = Column(String(50), primary_key=True, nullable=True)  # 管段编码
    task_code = Column(String(36), primary_key=True, nullable=True)  # 方案编码
    time = Column(DateTime, primary_key=True, nullable=True)  # 时间
    flow_rate = Column(Numeric(10, 4))  # 流量（流量单位）
    flow_depth = Column(Numeric(10, 4))  # 平均水深（m或ft）
    flow_velocity = Column(Numeric(10, 4))  # 流速（m/sec或者ft/sec）
    flow_volume = Column(Numeric(10, 4))  # Froude数（无量纲）
    capacity = Column(Numeric(10, 4))  # 能力（水深与完整水深比）
    pollut_conc = Column(Numeric(10, 4))  # 污染浓度


def insert_link_result(data_list):
    if data_list is not None:
        delete_link_result(data_list[0].task_code)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add_all(data_list)
        session.commit()
        session.close()


def delete_link_result(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(ResultSwmmLink).filter_by(task_code=task_code)
    datas.delete()
    session.commit()
    session.close()
