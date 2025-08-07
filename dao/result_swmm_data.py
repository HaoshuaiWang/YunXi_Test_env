from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, LargeBinary
from sqlalchemy.orm import sessionmaker
import psycopg2
from app.utils.db_utils import create_pg_engine

Base = declarative_base()


class ResultSwmmData(Base):
    __tablename__ = 'result_swmm_data'
    task_code = Column(String(36), primary_key=True)
    out_file = Column(LargeBinary, nullable=False)
    rpt_file = Column(LargeBinary)


def save_swmm_results(task_code: str, out_file_path: str, rpt_file_path: str = None):
    """安全保存SWMM结果（兼容约束问题）"""
    try:
        with open(out_file_path, 'rb') as f:
            out_data = f.read()

        rpt_data = None
        if rpt_file_path:
            with open(rpt_file_path, 'rb') as f:
                rpt_data = f.read()

        engine = create_pg_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # 方法1：先尝试更新
            existing = session.get(ResultSwmmData, task_code)
            if existing:
                existing.out_file = out_data
                existing.rpt_file = rpt_data
            else:
                # 方法2：如果更新0行，则插入
                session.add(ResultSwmmData(
                    task_code=task_code,
                    out_file=out_data,
                    rpt_file=rpt_data
                ))

            session.commit()
            print(f"成功保存任务 {task_code} 的结果文件")

        except Exception as e:
            session.rollback()
            # 方法3：使用原生SQL回退方案
            conn = engine.raw_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO result_swmm_data (task_code, out_file, rpt_file)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (task_code) 
                    DO UPDATE SET 
                        out_file = EXCLUDED.out_file,
                        rpt_file = EXCLUDED.rpt_file
                """, (
                    task_code,
                    psycopg2.Binary(out_data),
                    psycopg2.Binary(rpt_data) if rpt_data else None
                ))
                conn.commit()
            except Exception as e2:
                conn.rollback()
                raise RuntimeError(f"最终保存失败: {str(e2)}")
            finally:
                conn.close()
        finally:
            session.close()

    except Exception as e:
        raise RuntimeError(f"保存失败: {str(e)}")


# get_swmm_results和delete_swmm_results保持不变...
def get_swmm_results(task_code: str) -> dict:
    """获取SWMM分析结果"""
    engine = create_pg_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        result = session.get(ResultSwmmData, task_code)
        if not result:
            return None
        return {
            'task_code': result.task_code,
            'out_file': result.out_file,
            'rpt_file': result.rpt_file
        }
    finally:
        session.close()


def delete_swmm_results(task_code: str):
    """删除指定方案的结果"""
    engine = create_pg_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        result = session.get(ResultSwmmData, task_code)
        if result:
            session.delete(result)
            session.commit()
    except Exception as e:
        session.rollback()
        raise RuntimeError(f"删除失败: {str(e)}")
    finally:
        session.close()


if __name__ == "__main__":
    try:
        save_swmm_results(
            task_code="model_20230701_001",
            out_file_path="yxswmm.out",
            rpt_file_path="yxswmm.rpt"
        )
        print("结果保存成功")

        # 测试读取
        results = get_swmm_results("model_20230701_001")
        if results:
            with open("restored.out", "wb") as f:
                f.write(results['out_file'])
            if results['rpt_file']:
                with open("restored.rpt", "wb") as f:
                    f.write(results['rpt_file'])
            print("结果读取成功")

    except Exception as e:
        print(f"操作失败: {str(e)}")