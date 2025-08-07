# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:23
# description: swmm一维模型子汇水区实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultSwmmCatchment(Base):
    __tablename__ = 'result_swmm_catchment'  # 表名
    catchment_id = Column(String(50), primary_key=True, nullable=True)  # 子汇水区编码
    task_code = Column(String(36), primary_key=True, nullable=True)  # 方案编码
    time = Column(DateTime, primary_key=True, nullable=True)  # 时间
    rainfall = Column(Numeric(10, 4))  # 降水量(mm/hr或in/hr)
    snow_depth = Column(Numeric(10, 4))  # 降雪深度（mm或in）
    evap_loss = Column(Numeric(10, 4))  # 蒸发损失（mm/hr或in/hr）
    infil_loss = Column(Numeric(10, 4))  # 下渗损失（mm/hr或in/hr）
    runoff_rate = Column(Numeric(10, 4))  # 径流率
    gw_outflow_rate = Column(Numeric(10, 4))  # 地下水流出率
    gw_table_elev = Column(Numeric(10, 4))  # 地下水标高(m或ft)
    soil_moisture = Column(Numeric(10, 4))  # 土壤水分
    pollut_conc = Column(Numeric(10, 4))  # 污染浓度


def insert_catchment_result(data_list):
    if data_list is not None:
        delete_catchment_result(data_list[0].task_code)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add_all(data_list)
        session.commit()
        session.close()


def delete_catchment_result(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(ResultSwmmCatchment).filter_by(task_code=task_code)
    datas.delete()
    session.commit()
    session.close()

