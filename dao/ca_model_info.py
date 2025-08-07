# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024-9-10 10:51:01
# description: 元胞计算器方案实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class CaModelInfo(Base):
    __tablename__ = 'ca_model_info'  # 表名
    model_id = Column(String(50), primary_key=True)  # 模型编码
    srid = Column(String(20))                        # 坐标系id 4549
    model_path = Column(String(255))                 # 模型路径
    dem_path = Column(String(255))                   # 地形文件路径
    max_dt = Column(Numeric(8, 2))                   # 最大时间步长
    min_dt = Column(Numeric(8, 2))                   # 最小时间步长
    roughness = Column(Numeric(6, 4))                # 全局糙率
    boundary_elev = Column(Numeric(10, 2))           # 边界高程
    depth_interval = Column(Numeric(6, 0))           # 水深结果间隔
    velocity_interval = Column(Numeric(6, 0))        # 流速结果间隔


# 查询二维模型工作空间
def query_ca_model_path(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(CaModelInfo).filter_by(model_id=model_id).first()
    model_path = data.model_path
    session.commit()
    session.close()
    return model_path




