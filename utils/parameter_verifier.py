import os
from typing import List, Dict, Optional
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.errors import DatabaseError  # 确保导入DatabaseError
from psycopg2.sql import NULL
from sqlalchemy.exc import DatabaseError
from pathlib import Path
from app.utils.db_utils import create_pg_connect
from configparser import ConfigParser
from psycopg2 import sql
from typing import Optional
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
def isTaskCodeExist(taskCode: str):
    """
    验证任务编码是否存在于数据库中
    Args:
        taskCode: 需要验证的任务编码
    Returns:
        如果存在，返回关联的model_id；否则返回None

    Raises:
        ValueError: 任务编码不存在
        DatabaseError: 数据库操作失败
    """
    conn = None
    cursor = None
    try:
        # 建立数据库连接
        conn = create_pg_connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # 安全的参数化查询
        query = sql.SQL("""
            SELECT model_id 
            FROM result_run_rec 
            WHERE task_code = %s
        """)
        cursor.execute(query, (taskCode,))
        result = cursor.fetchone()  # 使用fetchone()获取单条记录
        if result is None:
            return False,f"任务编码不存在或者为空: {taskCode}"
        return True,"valid"
    except psycopg2.Error as e:
        # 数据库操作异常
        error_msg = f"数据库操作错误: {e}"
        print(error_msg)
        raise DatabaseError(error_msg) from e
    finally:
        # 确保资源释放
        if cursor:
            cursor.close()
        if conn:
            conn.close()
def isModelIdExist(taskCode: str):
    """
    验证任务编码是否存在于数据库中
    Args:
        taskCode: 需要验证的任务编码
    Returns:
        如果存在，返回关联的model_id；否则返回None

    Raises:
        ValueError: 任务编码不存在
        DatabaseError: 数据库操作失败
    """
    conn = None
    cursor = None
    try:
        # 建立数据库连接
        conn = create_pg_connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # 安全的参数化查询
        query = sql.SQL("""
            SELECT * 
            FROM basic_model_info 
            WHERE model_id = %s
        """)
        cursor.execute(query, (taskCode,))
        result = cursor.fetchone()  # 使用fetchone()获取单条记录
        if result is None:
            return False,f"模型编码不存在或者为空: {taskCode}"
        return True,"valid"
    except psycopg2.Error as e:
        # 数据库操作异常
        error_msg = f"数据库操作错误: {e}"
        print(error_msg)
        raise DatabaseError(error_msg) from e
    finally:
        # 确保资源释放
        if cursor:
            cursor.close()
        if conn:
            conn.close()
def is_modelAndTaskode_match(taskCode,ModelId):
    """
        验证任务编码是否存在于数据库中
        Args:
            taskCode: 需要验证的任务编码
        Returns:
            如果存在，返回关联的model_id；否则返回None
        Raises:
            ValueError: 任务编码不存在
            DatabaseError: 数据库操作失败
        """
    conn = None
    cursor = None
    try:
        # 建立数据库连接
        conn = create_pg_connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # 安全的参数化查询
        query = sql.SQL("""
                SELECT model_id 
                FROM result_run_rec 
                WHERE task_code = %s and model_id = %s
            """)
        cursor.execute(query, (taskCode,ModelId,))
        result = cursor.fetchone()  # 使用fetchone()获取单条记录
        if result is None:
            return False,f"任务编码or模型不存在或者二者不匹配: {taskCode}"
        return True,"valid"
    except psycopg2.Error as e:
        # 数据库操作异常
        error_msg = f"数据库操作错误: {e}"
        print(error_msg)
        raise DatabaseError(error_msg) from e
    finally:
        # 确保资源释放
        if cursor:
            cursor.close()
        if conn:
            conn.close()





def is_swmmTaskCode_valid(taskCode: str):
    """
    验证任务编码对应的查询结果是否包含全部指定的source_name
    Args:
        taskCode: 需要验证的任务编码
    Returns:
        若包含所有指定的source_name，返回True；否则返回False
    Raises:
        DatabaseError: 数据库操作失败
    """
    # 定义必须包含的所有source_name（目标列表）
    REQUIRED_SOURCE_NAMES = {
        "2410202A4D_TS",
        "2410202A4E_TS",
        "2410202A53_TS",
        "2410202A54_TS",
        "2410202A64_TS",
        "2410202A69_TS",
        "2410202A6F_TS",
        "2410202A89_TS",
        "61901000_TS",
        "61901305_TS",
        "61924550_TS",
        "Q2450_TS"
    }
    conn = None
    cursor = None
    try:
        # 建立数据库连接
        conn = create_pg_connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # 查询该taskCode对应的所有source_name
        query = sql.SQL("""
            SELECT DISTINCT source_name 
            FROM swmm_rain_source 
            WHERE task_code = %s
        """)
        cursor.execute(query, (taskCode,))
        result = cursor.fetchall()  # 格式：[{'source_name': 'xxx'}, {'source_name': 'yyy'}, ...]
        # 提取查询结果中的source_name，转换为集合（去重且便于比较）
        query_source_names = {row['source_name'] for row in result}
        print(query_source_names)
        # 校验：查询结果是否包含所有必须的source_name
        # 差集为空 → 所有必须的都存在；否则缺失
        missing = REQUIRED_SOURCE_NAMES - query_source_names
        print(missing)
        if missing:
            return False,f"数据不完整，缺少以下source_name：{missing}"
        return True,"valid"

    except psycopg2.Error as e:
        error_msg = f"数据库操作错误: {e}"
        print(error_msg)
        raise DatabaseError(error_msg) from e  # 抛出数据库错误，由调用者处理
    finally:
        # 确保连接关闭
        if cursor:
            cursor.close()
        if conn:
            conn.close()
def is_mascaretModelId_valid(model_id):
    """
    检查Mascaret相关文件是否存在（不执行任何生成操作）

    Args:
        model_id: 模型ID

    Returns:
        Tuple[bool, str]: (是否存在, 消息)
            - 成功：(True, "文件检查通过")
            - 失败：(False, 具体原因，如文件缺失、配置错误等)
    """
    try:
        # 读取配置文件获取工作区路径
        base_path = os.path.dirname(__file__)
        config_path = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
        parser = ConfigParser()
        parser.read(config_path)

        # 提取工作区配置（简化循环为字典推导式）
        section = 'workspace'
        if not parser.has_section(section):
            return False, f"配置文件缺失[{section}]节点：{config_path}"
        conf = {key: value for key, value in parser.items(section)}

        # 确定操作系统对应的工作区路径
        os_type = 'windows' if os.name == 'nt' else 'linux'
        if os_type not in conf:
            return False, f"配置文件[{section}]中缺失{os_type}的路径配置"
        workspace = Path(conf[os_type])

        # 构建文件路径
        model_path = os.path.join(workspace, 'model', model_id)
        src_dir = os.path.join(model_path, 'model1D')
        xcas_target = os.path.join(src_dir, f'mascaret_{model_id}.xcas')
       
        # 精确检查文件是否存在（使用isfile确保是文件）
        xcas_exists = os.path.isfile(xcas_target)
       

        # 生成结果消息
        messages = []
        if not xcas_exists:
            messages.append(f"xcas文件不存在：{xcas_target}")
        

        if not messages:
            return True, "xcas文件存在"
        else:
            return False, "; ".join(messages)

    except Exception as e:
        return False, f"检查失败：{str(e)}"

if __name__ == '__main__':
    modelId = ''
    taskCode = ''
    # isModelIdExist(modelId)
    # isTaskCodeExist(taskCode)
    # is_modelAndTaskode_match("054a1fdb138248ea8228be8a062d4ab","A11_420322_TH")
    is_swmmTaskCode_valid("54a6bad72a194a2bab3c7694ba638d1d")