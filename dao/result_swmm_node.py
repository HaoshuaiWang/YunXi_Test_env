# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:38
# description: swmm一维模型节点结果实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultSwmmNode(Base):
    __tablename__ = 'result_swmm_node'  # 表名
    node_id = Column(String(50), primary_key=True, nullable=True)  # 节点编码
    task_code = Column(String(36), primary_key=True, nullable=True)  # 方案编码
    time = Column(DateTime, primary_key=True, nullable=True)  # 时间
    invert_depth = Column(Numeric(10, 4))  # 水深(m或ft)
    hydraulic_head = Column(Numeric(10, 4))  # 水头(m或ft)
    ponded_volume = Column(Numeric(10, 4))  # 蓄水水量(m3或ft3)
    lateral_inflow = Column(Numeric(10, 4))  # 支管进流量(流量单位)
    total_inflow = Column(Numeric(10, 4))  # 总净流量(流量单位)
    flooding_losses = Column(Numeric(10, 4))  # 地表溢流(流量单位)
    pollut_conc = Column(Numeric(10, 4))  # 污染浓度(质量/升)


def insert_node_result(data_list):
    if data_list is not None:
        delete_node_result(data_list[0].task_code)
        engine = create_pg_engine()
        session_class = sessionmaker(bind=engine)
        session = session_class()
        session.add_all(data_list)
        session.commit()
        session.close()


def delete_node_result(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(ResultSwmmNode).filter_by(task_code=task_code)
    datas.delete()
    session.commit()
    session.close()


def query_node_result(task_code, node_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(ResultSwmmNode).filter(
        ResultSwmmNode.task_code == task_code, 
        ResultSwmmNode.node_id == node_id
    ).order_by(ResultSwmmNode.time).all()
    data_list = [{
        'node_id': item.node_id,
        'task_code': item.task_code,
        'time': item.time,
        'invert_depth': item.invert_depth,
        'hydraulic_head': item.hydraulic_head,
        'ponded_volume': item.ponded_volume,
        'lateral_inflow': item.lateral_inflow,
        'total_inflow': item.total_inflow,
        'flooding_losses': item.flooding_losses,
        'pollut_conc': item.pollut_conc} for item in data
    ]
    session.close()
    return data_list


def query_node_losses(task_code, node_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    data = session.query(ResultSwmmNode).filter(
        ResultSwmmNode.task_code == task_code,
        ResultSwmmNode.node_id == node_id
    ).order_by(ResultSwmmNode.time).all()
    losses_strings = [str(item.flooding_losses) for item in data]
    session.close()
    return losses_strings

