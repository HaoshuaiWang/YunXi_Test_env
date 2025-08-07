# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2025年6月4日19:17:34
# description:mascaret模型操作接口
import concurrent
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from configparser import ConfigParser
from psycopg2 import sql
from sqlalchemy.dialects.postgresql import psycopg2
from app.utils.db_utils import create_pg_engine, create_pg_connect
import configparser
import psycopg2
from pathlib import Path
import datetime
import shutil
import subprocess
import re
from ..utils import file_utils, parameter_verifier
from ..utils.java_utils import executeCaPostProcessing
from app.telemac.main import execute_telemac_task
executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
def get_last_time_from_loi(loi_path):
    with open(loi_path, encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        raise ValueError(f"{loi_path}文件为空")
    last_line = lines[-1]

    time_value = float(last_line.split()[0])
    return time_value

def update_xcas_file(xcas_path, nb_pas_temps, pas_stock, pas_impression):
    with open(xcas_path) as f:
        content = f.read()

    content = re.sub(r'(<nbPasTemps>)(\d+)(</nbPasTemps>)', rf'\g<1>{nb_pas_temps}\g<3>', content)
    content = re.sub(r'(<pasStock>)(\d+)(/<pasStock>)', rf'\g<1>{pas_stock}\g<3>', content)
    content = re.sub(r'(<pasImpression>)(\d+)(/<pasImpression>)', rf'\g<1>{pas_impression}\g<3>', content)

    with open(xcas_path, 'w') as f:
        f.write(content)


# 初始化河道一维模型
def execute_mascaret_init(model_id):
    create_geometry(model_id)
    create_down_imposed(model_id)


# 河道一维样例计算
def execute_mascaret_example(model_id):
    # TODO 执行一维模型的样例，样例输入可以随机，只要能跑出模型数据入库即可
    aaa = '1'


# 河道一维任务计算
def execute_mascaret_task(model_id,task_code,task_mode):
    """
        :param model_id: 模型编码
        :param task_code: 任务编码
        :param task_mode: 任务类型 mascaret:表示只执行河道一维任务;ca:表示二维采用元胞自动机;telemac:表示二维采用telemac引擎
        :return: 是否执行成功！
        """
    try:
        # TODO 根据task_code获取 mascaret_up_imposed 表信息，写成对应的文件执行算法
        base_path = os.path.dirname(__file__)
        filename = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
        section = 'workspace'
        parser = ConfigParser()
        parser.read(filename)
        conf = {}
        if parser.has_section(section):
            items = parser.items(section)
            for item in items:
                conf[item[0]] = item[1]
        os_type = 'windows' if os.name == 'nt' else 'linux'
        workspace = Path(conf[os_type])
        model_path = os.path.join(workspace, 'model', model_id)
        src_dir = os.path.join(model_path, 'model1D')
        task_dir = os.path.join(src_dir, task_code)
        os.makedirs(task_dir, exist_ok=True)
        '''
        for root, dirs, files in os.walk(src_dir):
            if os.path.abspath(root).startswith(os.path.abspath(task_dir)):
                continue
            for file in files:
                src_path = os.path.join(root, file)
                dst_path = os.path.join(task_dir, file)
                if os.path.abspath(src_path) != os.path.abspath(dst_path):
                    shutil.copy2(src_path, dst_path)
                    print(f"Copied:{src_path} -> {dst_path}")
        '''
        for file in os.listdir(src_dir):
            src_path = os.path.join(src_dir, file)
            if os.path.isfile(src_path):
                dst_path = os.path.join(task_dir, file)
                shutil.copy2(src_path, dst_path)
                print(f"Copied(root):{src_path}->{dst_path}")

        up_dir = os.path.join(src_dir, 'up_imposed_loi')
        if os.path.isdir(up_dir):
            for file in os.listdir(up_dir):
                src_path = os.path.join(up_dir, file)
                if os.path.isfile(src_path):
                    dst_path = os.path.join(task_dir, file)
                    shutil.copy2(src_path, dst_path)
                    print(f"Copied(up_imposed):{src_path}->{dst_path}")
        xcas_target = os.path.join(task_dir, f'mascaret_{model_id}.xcas')
        subprocess.run(["mascaret.py", xcas_target], check=True)
        opt_target = os.path.join(task_dir, f'mascaret_{model_id}.opt')
        file_utils.parse_resultats_to_db(opt_target, model_id, task_code)
        if task_mode == "telemac":
            execute_telemac_task(model_id, task_code)
    except Exception as e:
        print(e)
        return False
    finally:
        executeCaPostProcessing(model_id,task_code,task_mode)
    return True




# 创建几何文件
def create_geometry(model_id):

    # 读取配置文件
    # config = configparser.ConfigParser()
    # config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    # config.read(config_path)
    base_path = os.path.dirname(__file__)
    filename = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
    section = 'workspace'
    parser = ConfigParser()
    parser.read(filename)
    conf = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            conf[item[0]] = item[1]
    # 根据当前操作系统选择工作目录
    os_type = 'windows' if os.name == 'nt' else 'linux'
    workspace = Path(conf[os_type])

    try:
        # 连接数据库
        conn = create_pg_connect()
        cursor = conn.cursor()
        # 构建该model_id的输出目录：{workspace}/model/{model_id}
        model_dir = workspace / "model"/  model_id/"model1D"
        model_dir.mkdir(parents=True, exist_ok=True)
        # 构建输出文件路径
        output_file = model_dir / f'mascaret_{model_id}.geo'
        # 打开输出文件
        with open(output_file, 'w', encoding='utf-8') as f:
            # 添加头部信息
            current_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            f.write(f"#  DATE : {current_time}\n")
            f.write("#  PROJ. : EPSG:4546\n")
            # 查询该model_id下的所有不同的断面编码(seccd)
            query = sql.SQL("""
                SELECT DISTINCT seccd 
                FROM mascaret_cross_info 
                WHERE model_id = %s 
                ORDER BY seccd
            """)
            cursor.execute(query, (model_id,))
            sections = cursor.fetchall()
            # 处理每个断面
            for i, (seccd,) in enumerate(sections):
                # 查询该断面的di, x, y, zb数据
                query = sql.SQL("""
                    SELECT di, x, y, zb
                    FROM mascaret_cross_info 
                    WHERE seccd = %s AND model_id = %s
                    ORDER BY di ASC
                """)
                cursor.execute(query, (seccd, model_id))
                rows = cursor.fetchall()
                if not rows:
                    print(f"断面 {seccd} 在model_id {model_id} 下没有数据")
                    continue
                # 查询该断面的secid（从mascaret_vertical_info表）
                cursor.execute("""
                    SELECT secid
                    FROM mascaret_vertical_info
                    WHERE seccd = %s
                """, (seccd,))
                secid_row = cursor.fetchone()
                if not secid_row:
                    print(f"警告: 断面 {seccd} 在mascaret_vertical_info中没有对应的secid记录")
                    secid = 0.0  # 默认值
                else:
                    secid = secid_row[0]
                # 获取起点数据
                start_row = rows[0]
                start_di, start_x, start_y = start_row[0], start_row[1], start_row[2]
                # 获取终点数据
                end_row = rows[-1]
                end_x, end_y = end_row[1], end_row[2]
                # 获取zb最小的点作为AXE
                rows_sorted_by_zb = sorted(rows, key=lambda x: x[3])
                axe_row = rows_sorted_by_zb[0]
                axe_x, axe_y = axe_row[1], axe_row[2]
                # 写入断面头部信息
                f.write(
                    f"PROFIL {model_id} {seccd} {secid} {start_x} {start_y} {end_x} {end_y} AXE {axe_x} {axe_y}\n")
                # 写入该断面的所有数据点
                min_di = rows[0][0]
                max_di = rows[-1][0]
                for row in rows:
                    di, x, y, zb = row
                    marker = 'T' if (di == min_di or di == max_di) else 'B'
                    f.write(f"{di} {zb} {marker} {x} {y}\n")
        print(f"model_id {model_id} 的所有断面数据已写入 {output_file}")

    except Exception as e:
        print(f"数据库操作错误: {e}")

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# 创建下边界条件写成文件
def create_down_imposed(model_id):
    # TODO 根据model_id 查询mascaret_down_imposed表，创建下边界条件文件
    try:
        # 连接数据库
        conn = create_pg_connect()
        cursor = conn.cursor()

        # 读取配置文件获取工作目录
        base_path = os.path.dirname(__file__)
        filename = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
        section = 'workspace'
        parser = ConfigParser()
        parser.read(filename)
        conf = {}
        if parser.has_section(section):
            items = parser.items(section)
            for item in items:
                conf[item[0]] = item[1]
        # 根据当前操作系统选择工作目录
        os_type = 'windows' if os.name == 'nt' else 'linux'
        workspace = Path(conf[os_type])

        # 创建结果目录：{workspace}/model/{model_id}/model1D
        result_dir = workspace / "model" / model_id / "model1D"
        os.makedirs(result_dir, exist_ok=True)

        # 构建输出文件路径
        output_file = result_dir / "tarage.loi"

        # 查询该model_id的所有水位流量关系数据，按ptno升序排序
        query = sql.SQL("""
               SELECT z, q
               FROM mascaret_down_imposed
               WHERE model_id = %s
               ORDER BY ptno ASC
           """)
        cursor.execute(query, (model_id,))
        rows = cursor.fetchall()

        if not rows:
            print(f"未找到model_id为 {model_id} 的水位流量关系数据")
            return False

        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入文件头部
            f.write("# loi_2_tarage\n")
            f.write("# Cote Debit\n")

            # 写入数据行
            for row in rows:
                z, q = row
                # 写入水位和流量，保留一位小数
                f.write(f" {z:.1f} {q:.1f}\n")

        print(f"model_id {model_id} 的水位流量关系数据已导出到 {output_file}")
        return True

    except Exception as e:
        print(f"导出数据时出错: {e}")
        return False

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def parse_mascaret_result(task_code, slf_path):
    # TODO 解析一维的结果文件，存入 result_mascaret_process 表
    a = 2

if __name__ == "__main__":
    #execute_mascaret_init("A11_420322_AJH")
    execute_mascaret_task("A11_420322_TH", '4a7c93429021491cbacf197f03f84974', 'mascaret')
