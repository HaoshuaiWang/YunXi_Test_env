# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/25 10:14
# description: 文件解析方法类
import datetime
import os

from backports.configparser import ConfigParser
from psycopg2 import sql
from sqlalchemy.dialects.postgresql import psycopg2

from app.utils.db_utils import create_pg_engine, create_pg_connect
import os
import configparser
import psycopg2
from psycopg2 import sql
from pathlib import Path
from datetime import datetime, timedelta
def get_dir_files(folder_path):
    filenames = os.listdir(folder_path)
    return filenames


def get_mascaret_down_imposed(model_id):
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
        print(conf)
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
                f.write(f" {z:.4f} {q:.4f}\n")

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

def get_up_imposed_loi(task_code):
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
        print(conf)
        # 根据当前操作系统选择工作目录
        os_type = 'windows' if os.name == 'nt' else 'linux'
        workspace = Path(conf[os_type])


        # 查询task_code对应的model_id
        cursor.execute("""
            SELECT model_id 
            FROM result_run_rec 
            WHERE task_code = %s
        """, (task_code,))

        result = cursor.fetchone()
        if not result:
            print(f"未找到task_code为 {task_code} 的model_id")
            return False

        model_id = result[0]
        print(f"获取到task_code {task_code} 对应的model_id: {model_id}")

        # 创建结果目录：{workspace}/model/{model_id}/loi
        result_dir = workspace / "model" / model_id / "model1D"/"up_imposed_loi"
        os.makedirs(result_dir, exist_ok=True)

        # 查询指定task_code的所有scs recd
        cursor.execute("""
               SELECT DISTINCT in_code 
               FROM mascaret_source_coupling 
               WHERE model_id = %s 
               ORDER BY in_code
           """, (model_id,))

        recds = [row[0] for row in cursor.fetchall()]
        if not recds:
            print(f"未找到task_code为 {task_code} 的scs数据")
            return False

        # 处理每个recd
        for recd in recds:
            # 查询该recd的时间和out_q数据，按时间排序
            query = sql.SQL("""
                   SELECT dt, out_q
                   FROM result_scs_flow
                   WHERE task_code = %s AND recd = %s
                   ORDER BY dt ASC
               """)
            cursor.execute(query, (task_code, recd))
            rows = cursor.fetchall()

            if not rows:
                recd ='2410202A67'
                query = sql.SQL("""
                                   SELECT dt, outq
                                   FROM result_res_flow
                                   WHERE task_code = %s AND stcd = %s
                                   ORDER BY dt ASC
                               """)
                cursor.execute(query, (task_code, recd))
                rows = cursor.fetchall()
                if not  rows:
                    print(f"recd {recd} 没有数据")
                    continue

            # 提取起始时间
            start_dt = rows[0][0]

            # 构建输出文件路径：{workspace}/model/{model_id}/loi/up_imposed{recd}.loi
            output_file = result_dir / f"up_imposed{recd}.loi"

            with open(output_file, 'w', encoding='utf-8') as f:
                # 写入文件头部
                f.write("# loi_1_hydrogramme\n")
                f.write("# Temps (s) Debit\n")
                f.write("         S\n")

                # 写入数据行，时间转换为相对于起始时间的秒数
                for row in rows:
                    dt, out_q = row
                    # 计算相对时间（秒）
                    relative_time = (dt - start_dt).total_seconds()
                    # 保留一位小数
                    f.write(f"{relative_time:.1f} {out_q+10:.1f}\n")

            print(f"recd {recd} 的数据已导出到 {output_file}")

        print(f"task_code {task_code} (model_id: {model_id}) 的所有recd数据处理完成，结果保存在 {result_dir} 目录")
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
# def get_up_imposed_loi(task_code):
#     try:
#         # 连接数据库
#         # conn = psycopg2.connect(**db_config)
#         conn = create_pg_connect()
#         cursor = conn.cursor()
#
#         # 创建结果目录（如果不存在）：result/task_code
#         task_dir = os.path.join(os.getcwd(), 'result', 'loi', task_code)
#         os.makedirs(task_dir, exist_ok=True)
#
#         # 查询指定task_code的所有recd
#         cursor.execute("""
#                SELECT DISTINCT recd
#                FROM result_scs_flow
#                WHERE task_code = %s
#                ORDER BY recd
#            """, (task_code,))
#
#         recds = [row[0] for row in cursor.fetchall()]
#
#         if not recds:
#             print(f"未找到task_code为 {task_code} 的数据")
#             return False
#
#         # 处理每个recd
#         for recd in recds:
#             # 查询该recd的时间和out_q数据，按时间排序
#             query = sql.SQL("""
#                    SELECT dt, out_q
#                    FROM result_scs_flow
#                    WHERE task_code = %s AND recd = %s
#                    ORDER BY dt ASC
#                """)
#             cursor.execute(query, (task_code, recd))
#             rows = cursor.fetchall()
#
#             if not rows:
#                 print(f"recd {recd} 没有数据")
#                 continue
#
#             # 提取起始时间
#             start_dt = rows[0][0]
#
#             # 构建输出文件路径：result/task_code/recd.dat
#             output_file = os.path.join(task_dir, f"up_imposed{recd}.loi")
#
#             with open(output_file, 'w', encoding='utf-8') as f:
#                 # 写入文件头部
#                 f.write("# loi_1_hydrogramme\n")
#                 f.write("# Temps (s) Debit\n")
#                 f.write("         S\n")
#
#                 # 写入数据行，时间转换为相对于起始时间的秒数
#                 for row in rows:
#                     dt, out_q = row
#                     # 计算相对时间（秒）
#                     relative_time = (dt - start_dt).total_seconds()
#                     # 保留一位小数
#                     f.write(f"{relative_time:.1f} {out_q:.1f}\n")
#
#             print(f"recd {recd} 的数据已导出到 {output_file}")
#
#         print(f"task_code {task_code} 的所有recd数据处理完成，结果保存在 {task_dir} 目录")
#         return True
#
#     except Exception as e:
#         print(f"导出数据时出错: {e}")
#         return False
#
#     finally:
#         # 关闭数据库连接
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()


# def get_mascaret_geo():
#     import psycopg2
#     from psycopg2 import sql
#     import os
#     import datetime
#
#     try:
#         # 连接数据库
#         conn = create_pg_connect()
#         cursor = conn.cursor()
#
#         # 创建结果目录（如果不存在）：result/mascaretGeo
#         result_dir = os.path.join(os.getcwd(), 'result', 'mascaretGeo')
#         os.makedirs(result_dir, exist_ok=True)
#
#         # 构建输出文件路径
#         output_file = os.path.join(result_dir, 'mascaret.geo')
#
#         # 打开输出文件
#         with open(output_file, 'w', encoding='utf-8') as f:
#             # 添加头部信息
#             current_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
#             f.write(f"#  DATE : {current_time}\n")
#             f.write("#  PROJ. : EPSG:4546\n")
#
#             # 查询所有不同的断面编码(seccd)及其对应的model_id
#             cursor.execute("""
#                 SELECT DISTINCT mci.seccd, mci.model_id
#                 FROM mascaret_cross_info mci
#                 WHERE mci.model_id != 'A11_420322_TH'
#                 ORDER BY mci.seccd
#             """)
#             sections = cursor.fetchall()
#
#             # 处理每个断面
#             for i, (seccd, model_id) in enumerate(sections):
#                 # 查询该断面的di, x, y, zb数据
#                 query = sql.SQL("""
#                     SELECT di, x, y, zb
#                     FROM mascaret_cross_info
#                     WHERE seccd = %s AND model_id != 'A11_420322_TH'
#                     ORDER BY di ASC
#                 """)
#                 cursor.execute(query, (seccd,))
#                 rows = cursor.fetchall()
#
#                 if not rows:
#                     print(f"断面 {seccd} 没有数据")
#                     continue
#
#                 # 查询该断面的secid（从mascaret_vertical_info表）
#                 cursor.execute("""
#                     SELECT secid
#                     FROM mascaret_vertical_info
#                     WHERE seccd = %s
#                 """, (seccd,))
#                 secid_row = cursor.fetchone()
#
#                 if not secid_row:
#                     print(f"警告: 断面 {seccd} 在mascaret_vertical_info中没有对应的secid记录")
#                     secid = 0.0  # 默认值
#                 else:
#                     secid = secid_row[0]
#
#                 # 获取起点数据
#                 start_row = rows[0]
#                 start_di, start_x, start_y = start_row[0], start_row[1], start_row[2]
#
#                 # 获取终点数据
#                 end_row = rows[-1]
#                 end_x, end_y = end_row[1], end_row[2]
#
#                 # 获取zb最小的点作为AXE
#                 rows_sorted_by_zb = sorted(rows, key=lambda x: x[3])
#                 axe_row = rows_sorted_by_zb[0]
#                 axe_x, axe_y = axe_row[1], axe_row[2]
#
#                 # 写入断面头部信息（替换start_di为secid）
#                 f.write(f"PROFIL {model_id} {seccd} {secid} {start_x} {start_y} {end_x} {end_y} AXE {axe_x} {axe_y}\n")
#
#                 # 写入该断面的所有数据点
#                 min_di = rows[0][0]
#                 max_di = rows[-1][0]
#
#                 for row in rows:
#                     di, x, y, zb = row
#                     marker = 'T' if (di == min_di or di == max_di) else 'B'
#                     f.write(f"{di} {zb} {marker} {x} {y}\n")
#
#             print(f"所有断面数据已写入 {output_file}")
#
#     except Exception as e:
#         print(f"数据库操作错误: {e}")
#
#     finally:
#         # 关闭数据库连接
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
def get_mascaret_geo():


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

        # 查询所有不同的model_id
        cursor.execute("""
            SELECT DISTINCT model_id 
            FROM mascaret_cross_info 
            WHERE model_id != 'A11_420322_TH'
            ORDER BY model_id
        """)
        model_ids = cursor.fetchall()

        # 处理每个model_id
        for model_id in model_ids:
            model_id = model_id[0]  # 获取实际的model_id值

            # 构建该model_id的输出目录：{workspace}/model/{model_id}
            model_dir = workspace / "model"/  model_id/"model1D"
            model_dir.mkdir(parents=True, exist_ok=True)

            # 构建输出文件路径
            output_file = model_dir / f'mascaret_{model_id}.geo'

            # 打开输出文件
            with open(output_file, 'w', encoding='utf-8') as f:
                # 添加头部信息
                current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                f.write(f"#  DATE : {current_time}\n")
                f.write("#  PROJ. : EPSG:4546\n")

                # 查询该model_id下的所有不同的断面编码(seccd)
                query = sql.SQL("""
                    SELECT DISTINCT seccd 
                    FROM mascaret_cross_info 
                    WHERE model_id = %s AND model_id != 'A11_420322_TH'
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
def get_mascaret_geo1():


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

        # 查询所有不同的model_id
        cursor.execute("""
            SELECT DISTINCT model_id 
            FROM mascaret_cross_info 
            WHERE model_id = 'A11_420322_XH'
            ORDER BY model_id
        """)
        model_ids = cursor.fetchall()

        # 处理每个model_id
        for model_id in model_ids:
            model_id = model_id[0]  # 获取实际的model_id值

            # 构建该model_id的输出目录：{workspace}/model/{model_id}
            model_dir = workspace / "model"/  model_id/"model1D"
            model_dir.mkdir(parents=True, exist_ok=True)

            # 构建输出文件路径
            output_file = model_dir / f'mascaret_{model_id}.geo'

            # 打开输出文件
            with open(output_file, 'w', encoding='utf-8') as f:
                # 添加头部信息
                current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                f.write(f"#  DATE : {current_time}\n")
                f.write("#  PROJ. : EPSG:4546\n")

                # 查询该model_id下的所有不同的断面编码(seccd)
                query = sql.SQL("""
                    SELECT DISTINCT seccd 
                    FROM mascaret_cross_info 
                    WHERE model_id = %s AND model_id != 'A11_420322_TH'
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
                    i=0
                    for row in rows:
                        di, x, y, zb = row
                        marker = 'T' if (di == min_di or di == max_di) else 'B'
                        if i %5 ==0:
                            f.write(f"{di} {zb} {marker} {x} {y}\n")
                        i = i+1
            print(f"model_id {model_id} 的所有断面数据已写入 {output_file}")

    except Exception as e:
        print(f"数据库操作错误: {e}")

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_mascaret_geo(modelId):


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
        model_id = modelId  # 获取实际的model_id值
        # 构建该model_id的输出目录：{workspace}/model/{model_id}
        model_dir = workspace / "model"/  model_id/"model1D"
        model_dir.mkdir(parents=True, exist_ok=True)
        # 构建输出文件路径
        output_file = model_dir / f'mascaret_{model_id}.geo'
        # 打开输出文件
        with open(output_file, 'w', encoding='utf-8') as f:
            # 添加头部信息
            current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
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
                    SELECT di, x, y, zb,pattern
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
                i=0
                for row in rows:
                    di, x, y, zb,pattern = row
                    if i %5 ==0:
                        f.write(f"{di} {zb} {pattern} {x} {y}\n")
                    i = i+1
        print(f"model_id {model_id} 的所有断面数据已写入 {output_file}")

    except Exception as e:
        print(f"数据库操作错误: {e}")

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 使用示例
import psycopg2
import re
import os
from datetime import datetime
import configparser
def insert_up_imposed(task_code):
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
        print(conf)
        # 根据当前操作系统选择工作目录
        os_type = 'windows' if os.name == 'nt' else 'linux'
        workspace = Path(conf[os_type])


        # 查询task_code对应的model_id
        cursor.execute("""
            SELECT model_id 
            FROM result_run_rec 
            WHERE task_code = %s
        """, (task_code,))

        result = cursor.fetchone()
        if not result:
            print(f"未找到task_code为 {task_code} 的model_id")
            return False

        model_id = result[0]
        print(f"获取到task_code {task_code} 对应的model_id: {model_id}")

        # 创建结果目录：{workspace}/model/{model_id}/loi
        result_dir = workspace / "model" / model_id / "model1D"/"up_imposed_loi"
        os.makedirs(result_dir, exist_ok=True)

        # 查询指定task_code的所有scs recd
        cursor.execute("""
               SELECT DISTINCT in_code 
               FROM mascaret_source_coupling 
               WHERE model_id = %s 
               ORDER BY in_code
           """, (model_id,))

        recds = [row[0] for row in cursor.fetchall()]
        if not recds:
            print(f"未找到task_code为 {task_code} 的scs数据")
            return False

        # 处理每个recd
        for recd in recds:
            # 查询该recd的时间和out_q数据，按时间排序
            query = sql.SQL("""
                   SELECT dt, out_q
                   FROM result_scs_flow
                   WHERE task_code = %s AND recd = %s
                   ORDER BY dt ASC
               """)
            cursor.execute(query, (task_code, recd))
            rows = cursor.fetchall()

            if not rows:
                query = sql.SQL("""
                                   SELECT dt, outq
                                   FROM result_res_flow
                                   WHERE task_code = %s AND stcd = %s
                                   ORDER BY dt ASC
                               """)
                cursor.execute(query, (task_code, recd))
                rows = cursor.fetchall()
                if not  rows:
                    print(f"recd {recd} 没有数据")
                    continue

            # 提取起始时间
            start_dt = rows[0][0]

            # 构建输出文件路径：{workspace}/model/{model_id}/loi/up_imposed{recd}.loi
            output_file = result_dir / f"up_imposed{recd}.loi"


            for row in rows:
                dt, out_q = row
                # 计算相对时间（秒）
                relative_time = (dt - start_dt).total_seconds()


            print(f"recd {recd} 的数据已导出到 {output_file}")

        print(f"task_code {task_code} (model_id: {model_id}) 的所有recd数据处理完成，结果保存在 {result_dir} 目录")
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

def parse_resultats_to_db(file_path, model_id, task_code):
    """解析[resultats]区块数据并导入PostgreSQL"""
    # 1. 检查文件存在性
    if not os.path.exists(file_path):
        print(f"错误：文件 {file_path} 不存在")
        return False

    rvcdDic = {"A11_420322_XH":"FF3CDA000002",'A11_420322_AJH':'FF3CD0000001','A11_420322_HH':'FF3CC000000R',
                'A11_420322_WLH':'FF3CDA000001','A11_420322_TH':'AFG24006'}
    rvcd = rvcdDic[model_id]
    # 3. 连接数据库
    try:
        conn = create_pg_connect()
        cursor = conn.cursor()
        print("成功连接数据库")
        # 4. 提取[resultats]数据
        data_rows = _extract_data(file_path)
        if not data_rows:
            print("❗ 未找到[resultats]数据区块")
            return False
        # 5. 批量插入数据
        _insert_data(cursor, data_rows, model_id, rvcd, task_code)
        conn.commit()
        print(f"成功导入 {len(data_rows)} 条记录")
        return True

    except psycopg2.Error as e:
        print(f"数据库操作失败: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def _extract_data(file_path):
    """提取[resultats]区块的有效数据行"""
    in_resultats = False
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # 定位[resultats]区块
            if line.startswith('[resultats]'):
                in_resultats = True
                continue
            if in_resultats and line.startswith('['):
                break
            # 跳过注释行和空行
            if line.startswith('#') or not in_resultats:
                continue
            data.append(line)
    return data


def _insert_data(cursor, rows, model_id, rvcd, task_code):
    """按表结构映射插入数据"""
    insert_sql = """
    INSERT INTO result_mascaret_process (
        model_id, task_code, rvcd, seccd, time, z_ref, z, 
        q_min, q_max, k_min, k_max, fr, v_min, y, q
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    list=[]
    group=-1
    res = queryTimeInfoByTaskCode(task_code)
    int_v,start_time = res['interval'], res['start_time']
    seccdNum =0
    seccdIndexOld=-1;
    for row in rows:
        # 分割列（兼容多空格）
        # 1. 去除行首尾空格和分号
        cleaned_row = row.strip(';')
        cleaned_row = cleaned_row.replace('"', '')

        # 2. 按一个或多个空格分割，并过滤空值
        cols = [col.rstrip(';') for col in re.split(r'\s+', cleaned_row) if col]
        if len(cols) < 14:
            print(f"⚠️ 数据行格式异常，跳过: {row}")
            continue

        # 字段映射（索引0:time, 2:seccd, 4:z_ref, 5:z, 6:q_min, 7:q_max, 8:k_min, 9:k_max, 10:fr, 11:v_min, 12:y, 13:q）
        try:
            # 时间转换（支持秒数/时间字符串）
            time_str = cols[0]
            if(cols[0] not in list):
                list.append(cols[0])
                group += 1
            time_val = start_time + timedelta(minutes=int_v*group)

            # 提取字段
            seccd_index = int(cols[2])
            if seccd_index>seccdIndexOld:
                seccdIndexOld = seccd_index
                seccdNum += 1
            else:
                seccdIndexOld = 0
                seccdNum = 1
            char = ""
            if seccdNum<10:
                char = "0"
            name = {"A11_420322_XH":"XH",'A11_420322_AJH':'AJH','A11_420322_HH':'HH',
                'A11_420322_WLH':'WLH','A11_420322_TH':'TH'}
            seccd = name[model_id] + char+ str(seccdNum)
            print(str(time_val) + seccd)
            z_ref = float(cols[4])
            z = float(cols[5])
            q_min = float(cols[6])
            q_max = float(cols[7])
            k_min = int(float(cols[8]))
            k_max = int(float(cols[9]))
            fr = float(cols[10])
            v_min = float(cols[11])
            y = float(cols[12])
            q = float(cols[13])

            # 执行插入
            cursor.execute(insert_sql, (
                model_id, task_code, rvcd, seccd, time_val,
                z_ref, z, q_min, q_max, k_min, k_max,
                fr, v_min, y, q
            ))

        except (ValueError, IndexError) as e:
            print(f"⚠️ 数据解析失败 [{row}]: {e}")


def queryTimeInfoByTaskCode(task_code):
    """根据任务编码查询时间信息"""
    conn = create_pg_connect()
    cursor = conn.cursor()
    result = None

    try:
        query_sql = """
            SELECT intv, start_time
            FROM result_run_rec
            WHERE task_code = %s
        """
        cursor.execute(query_sql, (task_code,))
        result = cursor.fetchone()  # 获取查询结果

        if result:
            intv, start_time = result
            return {
                "interval": intv,
                "start_time": start_time
            }
        else:
            print(f"警告：未找到task_code为 {task_code} 的时间信息")
            return None

    except Exception as e:
        print(f"查询时间信息失败: {e}")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
def updateMeshVertexArea():
    try:
        # 连接数据库
        conn = create_pg_connect()
        cursor = conn.cursor()


        # 查询该model_id的所有水位流量关系数据，按ptno升序排序

        query = sql.SQL("""
                    SELECT model_id,vertex_code
                    FROM tel_mesh_vertex
                """)
        cursor.execute(query)
        rows = cursor.fetchall()
        total = len(rows)
        if not rows:
            print(f"tel_mesh_vetex数据为空")
            return False
        i=1
        for row in rows:
            modelId,vertexCode = row
            query1 = sql.SQL("""
                        SELECT area
                        FROM tel_mesh_rel
                        where vertex_code = %s AND model_id = %s
                    """)
            cursor.execute(query1, (vertexCode,modelId,))
            areas = cursor.fetchall()
            sum =0
            for area in areas:
                sum = + area[0]
            sum = sum/3
            update = sql.SQL("""
                        UPDATE tel_mesh_vertex
                        SET area = %s
                        WHERE model_id = %s AND vertex_code = %s
                    """)
            cursor.execute(update, (sum,modelId,vertexCode))
            print(f"已更新{i}/{total}条数据")
            i= i+1
        conn.commit()
        return True

    except Exception as e:
        print(f"出错: {e}")
        return False

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()
# 执行入口（替换为实际参数）
if __name__ == "__main__":
    file_path = "AJH.opt"  # 替换为实际文件路径
    model_id = "A11_420322_AJH"
    rvcd = "FF3CD0000001"#  XH FF3CDA000002  WLH FF3CDA000001 HH FF3CC000000R AJH
    task_code = "9b77b583102c40db850e3922c55e2a87"
    taskCode = {"A11_420322_XH":"4cbbd817755b4167abeab0ddf49f0f18",'A11_420322_AJH':'4a7c93429021491cbacf197f03f84974','A11_420322_HH':'d802bea48a5f44e4bef9cac8d42abe35',
                'A11_420322_WLH':'dd9ec190d55a4fe4a95e0a44915a2412'}
    modelId={'xh':'A11_420322_XH','ajh':'A11_420322_AJH','hh':'A11_420322_HH','wlh':'A11_420322_WLH'}
    # for(key,value) in taskCode.items():
    #     get_up_imposed_loi(value)
    # query = queryTimeInfoByTaskCode(task_code)['start_time']
    # print(query)
    # parse_resultats_to_db(file_path, model_id, task_code)
    # task_code = "d5153cd1e73f42669854b61009e96aee"  # 替换为实际的task_code
    # model_id = ["A11_420322_HH","A11_420322_XH","A11_420322_AJH","A11_420322_WLH"]
    get_up_imposed_loi(task_code)
    # # get_mascaret_geo()
    # for model_id in model_id:
    #     get_mascaret_down_imposed(model_id)
    # get_mascaret_geo1()
    # get_mascaret_down_imposed("A11_420322_XH")
    # get_up_imposed_loi("4cbbd817755b4167abeab0ddf49f0f18")
    # updateMeshVertexArea()
    # get_mascaret_geo("A11_420322_TH")
    # get_mascaret_down_imposed("A11_420322_TH")
