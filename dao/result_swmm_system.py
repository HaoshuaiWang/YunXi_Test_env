# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:45
# description: swmm一维模型系统过程信息实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultSwmmSystem(Base):
    __tablename__ = 'result_swmm_system'  # 表名
    task_code = Column(String(36), primary_key=True, nullable=True)  # 方案编码
    time = Column(DateTime, primary_key=True, nullable=True)  # 时间
    air_temp = Column(Numeric(12, 4))  # 气温
    rainfall = Column(Numeric(12, 4))  # 降雨
    snow_depth = Column(Numeric(12, 4))  # 总积雪深度
    evap_infil_loss = Column(Numeric(12, 4))  # 蒸发下渗损失
    runoff_flow = Column(Numeric(12, 4))  # 总径流量
    dry_weather_inflow = Column(Numeric(12, 4))  # 总旱季径流量
    gw_inflow = Column(Numeric(12, 4))  # 地下径流量
    rdii_inflow = Column(Numeric(12, 4))  # 总RDII净流量
    direct_inflow = Column(Numeric(12, 4))  # 总直接径流
    total_lateral_inflow = Column(Numeric(12, 4))  # 总外部径流
    flood_losses = Column(Numeric(12, 4))  # 平均损失
    outfall_flows = Column(Numeric(12, 6))  # 总外部洪流
    volume_stored = Column(Numeric(12, 4))  # 排放口总流量
    evap_rate = Column(Numeric(12, 4))  # 蒸发速率
    ptnl_evap_rate = Column(Numeric(12, 4))  # 蓄水容积


def insert_system_result(data_list):
    if data_list is not None:
        delete_system_result(data_list[0].task_code)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add_all(data_list)
        session.commit()
        session.close()


def delete_system_result(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(ResultSwmmSystem).filter_by(task_code=task_code)
    datas.delete()
    session.commit()
    session.close()

