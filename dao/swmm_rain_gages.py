# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 11:20
# description: swmm模型雨量计实体类

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmRainGages(Base):
    __tablename__ = 'swmm_rain_gages'  # 表名
    rain_id = Column(String(50), primary_key=True, nullable=True)  # 雨量计编码
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    x_coordinate = Column(Numeric(12, 4))  # x坐标
    y_coordinate = Column(Numeric(12, 4))  # x坐标
    description = Column(String(255))  # 描述
    tag = Column(String(50))  # 标签
    rain_format = Column(String(20))  # 降雨类型
    time_interval = Column(String(10))  # 时间间隔
    snow_catch_factor = Column(Numeric(8, 4))  # 降雪因子
    data_source = Column(String(50))  # 数据源
    geom = Column(String)  # 空间字段


def insert_swmm_rain_gages(data_list):
    if len(data_list) != 0:
        model_id = data_list[0].model_id
        delete_by_model_id(model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO swmm_rain_gages(rain_id, model_id,x_coordinate,y_coordinate,rain_format,time_interval,snow_catch_factor,data_source, geom)"
                   " VALUES ('%s','%s',%f,%f,'%s','%s',%f,'%s',%s);"
                   % (item.rain_id, item.model_id, item.x_coordinate, item.y_coordinate, item.rain_format, item.time_interval, item.snow_catch_factor,item.data_source, item.geom))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(SwmmRainGages).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True



