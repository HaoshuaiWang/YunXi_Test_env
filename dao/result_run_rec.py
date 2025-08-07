# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 14:06
# description: 分析任务基本信息实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultRunRec(Base):
    __tablename__ = 'result_run_rec'                                    # 表名
    task_code = Column(String(36), primary_key=True, nullable=True)     # 任务编码
    task_name = Column(String(100), nullable=True)                      # 任务名称
    model_id = Column(String(30), nullable=True)                        # 模型编码
    task_type = Column(String(10), nullable=True)                       # 模型类型 LIVE：实况任务；PRE：预演任务；
    run_time = Column(DateTime)                                         # 执行时间
    warm_up = Column(DateTime)                                          # 预热期开始时间
    start_time = Column(DateTime)                                       # 预报开始时间
    end_time = Column(DateTime)                                         # 预报结束时间
    intv = Column(Integer)                                              # 时段长(s)
    periods = Column(Integer)                                           # 步数(个)
    run_state = Column(String(2))                                       # 执行状态
    progress_info = Column(String)                                      # 提示信息
    ts = Column(DateTime)                                               # 时间戳


# 查询任务信息
def query_task_info(task_code):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)  # type: ignore
    session = session_class()
    data = session.query(ResultRunRec).filter_by(task_code=task_code).one()
    out = {
        'task_code': data.task_code,
        'task_name': data.task_name,
        'model_id': data.model_id,
        'task_type': data.task_type,
        'run_time': data.run_time,
        'warm_up': data.warm_up,
        'start_time': data.start_time,
        'end_time': data.end_time,
        'intv': data.intv,
        'periods': data.periods,
        'run_state': data.run_state,
        'progress_info': data.progress_info,
        'ts': data.ts
    }
    session.close()
    return out






