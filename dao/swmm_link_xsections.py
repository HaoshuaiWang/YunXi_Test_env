# author: "lzu_xiaoyu"
# email: "1505465303@qq.com"
# date: 2024/3/21 13:26
# description: SWMM模型管道断面形状实体类

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import relationship
from app.utils.db_utils import create_pg_engine
from sqlalchemy.orm import sessionmaker
Base = declarative_base()


class SwmmLinkXsections(Base):
    __tablename__ = 'swmm_link_xsections'

    # 主键联合约束
    link_id = Column(String(50), primary_key=True, nullable=False)  # 管道编码
    model_id = Column(String(50), primary_key=True, nullable=False)  # 模型编码

    # 断面形状参数
    shape = Column(String(50))  # 形状类型
    geom1 = Column(Numeric(8, 2))  # 形状参数1（m）
    geom2 = Column(Numeric(8, 2))  # 形状参数2（m）
    geom3 = Column(Numeric(8, 2))  # 形状参数3（m，修正字段名）
    geom4 = Column(Numeric(8, 2))  # 形状参数4（m）
    barrels = Column(Numeric(4, 0))  # 并行管道数量
    culvert = Column(String(50))  # 涵洞参数（m）





def insert_swmm_link_xsections(data_list):
    """批量插入断面形状数据（会先删除同模型下的旧数据）"""
    if not data_list:
        return

    engine = create_pg_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 删除同模型下的旧数据
        model_id = data_list[0].model_id
        session.query(SwmmLinkXsections) \
            .filter_by(model_id=model_id) \
            .delete()

        # 批量插入新数据
        session.add_all(data_list)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_xsection_by_link(model_id: str, link_id: str) -> SwmmLinkXsections:
    """根据管道ID获取断面形状信息"""
    engine = create_pg_engine()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        return session.query(SwmmLinkXsections) \
            .filter_by(model_id=model_id, link_id=link_id) \
            .first()