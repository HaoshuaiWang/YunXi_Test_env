# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024��8��26��09:26:10
# description: �洢telemac���������֮�������ϵ


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from app.utils.db_utils import create_pg_engine, create_pg_connect

Base = declarative_base()


class TelMeshRel(Base):
    __tablename__ = 'tel_mesh_rel'  # ����
    model_id = Column(String(50), primary_key=True, nullable=True)  # ģ�ͱ���
    mesh_code = Column(Integer, primary_key=True, nullable=True)  # �������
    vertex_code = Column(Integer, primary_key=True, nullable=True)  # �������


def insert_tel_mesh_rel(data_list):
    batch_size = 100
    if len(data_list) != 0:
        model_id = data_list[0][0]
        delete_by_model_id(model_id)
        sql = 'INSERT INTO tel_mesh_rel(model_id, mesh_code, vertex_code) VALUES (%s,%s,%s)'
        db = create_pg_connect()
        cursor = db.cursor()
        i = 0
        while i < len(data_list):
            sub_list = data_list[i:i + batch_size]
            cursor.executemany(sql, sub_list)
            db.commit()
            i = i + batch_size
        cursor.close()
        db.close()


def delete_by_model_id(model_id):
    engine = create_pg_engine()
    session_class = sessionmaker(bind=engine)
    session = session_class()
    datas = session.query(TelMeshRel).filter_by(model_id=model_id)
    datas.delete()
    session.commit()
    session.close()
    return True
