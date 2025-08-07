# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/21 13:30
# description: swmm模型字典表实体类


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String

Base = declarative_base()


class SwmmModelDic(Base):
    __tablename__ = 'swmm_model_dic'  # 表名
    dic_code = Column(String(50), primary_key=True, nullable=True)  # 管道编码
    table_name = Column(String(20))  # 表明
    from_node = Column(String(50))  # 流入节点
    to_node = Column(String(50))  # 流出节点




