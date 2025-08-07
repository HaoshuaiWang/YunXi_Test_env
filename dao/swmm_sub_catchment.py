# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 11:37
# description: swmm模型子汇水分区实体类

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class SwmmSubCatchment(Base):
    __tablename__ = 'swmm_sub_catchment'  # 表名
    catchment_id = Column(String(50), primary_key=True, nullable=True)  # 子汇水区编码
    model_id = Column(String(50), primary_key=True, nullable=True)  # 模型编码
    description = Column(String(255))  # 描述
    tag = Column(String(50))  # 标签
    rain_id = Column(String(50))  # 雨量计编码
    node_id = Column(String(50))  # 节点编码
    area = Column(Numeric(12, 6))  # 面积（Ha）
    width = Column(Numeric(8, 4))  # 特征宽度（m）
    slope = Column(Numeric(8, 4))  # 坡度（%）
    imperv = Column(Numeric(8, 4))  # 防渗区面积（%）
    n_imperv = Column(Numeric(8, 4))  # 防渗区糙率
    n_perv = Column(Numeric(8, 4))  # 透水区糙率
    dstore_imperv = Column(Numeric(8, 4))  # 防渗区洼地深度（mm）
    dstore_perv = Column(Numeric(8, 4))  # 透水区洼地深度（mm）
    zero_imperv = Column(Numeric(8, 4))  # 无抑制防渗区面积比（%）
    subarea_routing = Column(Numeric(8, 4))  # 区域链接方式
    percent_routed = Column(Numeric(4, 3))  # 径流路径占比（%）
    geom = Column(String)  # 空间字段Polygon


def insert_swmm_catchment(data_list):
    if len(data_list) != 0:
        model_id = data_list[0].model_id
        delete_by_model_id(model_id)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        for item in data_list:
            sql = ("INSERT INTO swmm_sub_catchment(catchment_id,model_id,rain_id,node_id,area,width,slope,imperv,geom)"
                   " VALUES ('%s','%s','%s','%s',%f,%f,%f,%f,%s);"
                   % (item.catchment_id, item.model_id, item.rain_id, item.node_id, item.area, item.width, item.slope,
                      item.imperv, item.geom))
            session.execute(text(sql))
        session.commit()
        session.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(SwmmSubCatchment).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True

