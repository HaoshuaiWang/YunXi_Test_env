# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/18 14:28
# description:telemac模型操作接口
import concurrent
import datetime
import shutil
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.utils import parameter_verifier
from pathlib import Path
from configparser import ConfigParser

from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, text
from app.dao.result_tel_vertex_process import VertexProcessElement, insert_vertex_process
from app.dao.swmm_node_junction import query_nearest_node
from app.dao.tel_mesh_rel import insert_tel_mesh_rel
from app.utils.db_utils import create_pg_engine
from app.utils.parserSELAFIN import SELAFIN
from app.utils.zip_utils import unzip_file
from app.dao.tel_model_info import TelModelInfo, insert_tel_model_info, query_tel_model_info
from app.dao.tel_source_coupling import TelSourceCoupling, insert_tel_source_coupling, query_tel_source_coupling
from app.dao.tel_mesh_vertex import insert_tel_mesh_vertex, query_by_vertexCodeAndModelId, TelMeshVertex
from app.dao.tel_mesh_element import insert_tel_mesh_element
import app.dao.swmm_node_junction as junction
import app.dao.swmm_node_outfall as outfall
from  app.dao.swmm_node_junction import SwmmNodeJunction
from app.dao.swmm_node_outfall import SwmmNodeOutfall
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import re
import subprocess
from app.utils.java_utils import executeCaPostProcessing
executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

# 解析二维模型文件
def parse_telemac_file(model_id, zip_path, srid):
    try:
        directory_path = os.path.dirname(zip_path)
        unzip_file(zip_path, directory_path)
        filenames = os.listdir(directory_path)
        for filename in filenames:
            if filename[-4:] == '.cas':
                cas_file_path = os.path.join(directory_path, filename)
                tel_model_info, source_list = parse_cas_file(model_id, cas_file_path)
                insert_tel_model_info(tel_model_info)
                insert_tel_source_coupling(source_list)
            elif filename[-4:] == '.slf':
                slf_file_path = os.path.join(directory_path, filename)
                vertex_list, mesh_list, rel_list = parse_slf_source(model_id, slf_file_path, srid)
                insert_tel_mesh_vertex(vertex_list)
                insert_tel_mesh_element(mesh_list)
                insert_tel_mesh_rel(rel_list)
        os.remove(zip_path)
        print('telemac模型文件初始化成功！')
        return True
    except ValueError:
        print("telemac模型文件初始化失败！")
        return False


# 解析cas模型文件
def parse_cas_file(model_id, cas_file_path):
    tel_model_info = TelModelInfo()
    main_dic = {}  # 模型基础数据
    with open(cas_file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        i = 0
        while i < len(lines):
            if lines[i].startswith('/'):
                i += 1
                continue
            else:
                if '=' in lines[i]:
                    key = lines[i].split('=')[0].strip()
                    main_dic[key] = lines[i].split('=')[1].replace('\n', '').replace('\t', '').strip()
                elif ':' in lines[i]:
                    key = lines[i].split(':')[0].strip()
                    main_dic[key] = lines[i].split(':')[1].replace('\n', '').replace('\t', '').strip()
                else:
                    main_dic[key] = main_dic[key]+lines[i].replace('\n', '').replace('\t', '').strip()
            i += 1
    tel_model_info.model_id = model_id
    tel_model_info.model_path = os.path.dirname(cas_file_path)
    tel_model_info.steering_file = main_dic['STEERING FILE']
    tel_model_info.geometry_file = main_dic['GEOMETRY FILE']
    tel_model_info.source_file = main_dic['SOURCES FILE'].replace("'", "")
    tel_model_info.boundary_file = main_dic['BOUNDARY CONDITIONS FILE']
    tel_model_info.result_file = main_dic['RESULTS FILE']

    nodes = main_dic['GLOBAL NUMBERS OF SOURCE NODES'].split(';')
    source_list = []
    for node in nodes:
        tel_source_coupling = TelSourceCoupling()
        tel_source_coupling.model_id = model_id
        tel_source_coupling.vertex_code = int(node)
        source_list.append(tel_source_coupling)
    return tel_model_info, source_list


# 解析slf几何文件
def parse_slf_source(model_id, slf_file_path, srid):
    print('slf_file_path', slf_file_path)
    vertex_list = []
    element_list = []
    rel_list = []
    slf_file = SELAFIN(slf_file_path)
    x_list = np.array(slf_file.MESHX)
    y_list = np.array(slf_file.MESHY)
    z_list = np.array(slf_file.getVALUES(0)[0])
    # n_list = np.array(slf_file.getVALUES(0)[1])
    index_list = np.array(slf_file.IKLE2)

    i = 0
    while i < x_list.size:
        geom = ('POINT(%f %f)' % (x_list[i], y_list[i]))
        # tel_mesh_vertex = (i, model_id, float(z_list[i]), float(n_list[i]), geom, srid)
        tel_mesh_vertex = (i, model_id, float(z_list[i]), 0, geom, srid)
        vertex_list.append(tel_mesh_vertex)
        i += 1

    i = 0
    while i < index_list.size/3:
        p1 = int(index_list[i][0])
        p2 = int(index_list[i][1])
        p3 = int(index_list[i][2])
        tel_mesh_rel1 = (model_id, i, p1)
        tel_mesh_rel2 = (model_id, i, p2)
        tel_mesh_rel3 = (model_id, i, p3)
        rel_list.append(tel_mesh_rel1)
        rel_list.append(tel_mesh_rel2)
        rel_list.append(tel_mesh_rel3)
        mean_z = (z_list[p1]+z_list[p2]+z_list[p3])/3
        x1 = x_list[p1]
        y1 = y_list[p1]
        x2 = x_list[p2]
        y2 = y_list[p2]
        x3 = x_list[p3]
        y3 = y_list[p3]
        wkt = ("POLYGON((%f %f,%f %f,%f %f,%f %f))" % (x1, y1, x2, y2, x3, y3, x1, y1))
        tel_mesh_element = (i, model_id, float(mean_z), wkt, srid)
        element_list.append(tel_mesh_element)
        i += 1
    return vertex_list, element_list, rel_list


# 创建一二维耦合关系
# def create_coupling(model_id,model_mode):
#     try:
#         engine = create_pg_engine()
#         session_class = sessionmaker(bind=engine)
#         session = session_class()
#         if model_mode == "swmm":
#             # TODO 一维管网的耦合 1、根据model_id查询 swmm_node_junction和 swmm_node_outfall 排水口表；2、查询tel_mesh_vertex最近的点,放到list里；3、入库
#             coupling_list_all = []
#             juctionList = session.query(SwmmNodeJunction).filter_by(model_id = model_id).all()
#             outfallList = session.query(SwmmNodeOutfall).filter_by(model_id = model_id).all()
#             lenj = len(juctionList)
#             leno = len(outfallList)
#             print(f"junList 一共 {lenj}条数据 ")
#             print(f"outfallList 一共{leno}条数据")
#             for i in range(len(juctionList)):
#                 sql = ("SELECT b.vertex_code,st_distance(a.geom, b.geom) FROM swmm_node_junction a, tel_mesh_vertex b "
#                        "WHERE  a.node_id='%s' AND a.model_id ='%s' ORDER BY st_distance") % (juctionList[i].node_id, model_id)
#                 data = session.execute(text(sql))
#                 telMeshVertexCode = data.first().vertex_code
#                 # telMeshVertexEntity = query_by_vertexCodeAndModelId(telMeshVertexCode, model_id)
#                 entity = TelSourceCoupling()
#                 entity.model_id = model_id
#                 entity.vertex_code = telMeshVertexCode
#                 entity.node_code=juctionList[i].node_id
#                 entity.coup_type=model_mode
#                 coupling_list_all.append(entity)
#                 print(f"正在处理swmm_node_junction:{i+1}/{lenj} :{telMeshVertexCode}")
#             for i in range(len(outfallList)):
#                 sql = ("SELECT b.vertex_code,st_distance(a.geom, b.geom) FROM swmm_node_outfall a, tel_mesh_vertex b "
#                        "WHERE  a.node_id='%s' AND a.model_id ='%s' ORDER BY st_distance") % (outfallList[i].node_id, model_id)
#                 data = session.execute(text(sql))
#                 telMeshVertexCode= data.first().vertex_code
#                 # telMeshVertexCode = outfall.query_nearest_vertexCode(model_id,outfallList[i].node_id)
#                 # telMeshVertexEntity = query_by_vertexCodeAndModelId(telMeshVertexCode, model_id)
#                 entity = TelSourceCoupling()
#                 entity.model_id = model_id
#                 entity.vertex_code = telMeshVertexCode
#                 entity.node_code=outfallList[i].node_id
#                 entity.coup_type=model_mode
#                 coupling_list_all.append(entity)
#                 print(f"正在处理swmm_node_outfall:{i + 1}/{leno}")
#             insert_tel_source_coupling(coupling_list_all)
#             # coupling_list = query_tel_source_coupling(model_id)
#             # for i in range(len(coupling_list)):
#             #     entity = TelSourceCoupling()
#             #     item = coupling_list[i]
#             #     vertex_code = item.vertex_code
#             #     entity.model_id = model_id
#             #     entity.vertex_code = vertex_code
#             #     entity.node_code = query_nearest_node(model_id, vertex_code)
#             #     coupling_list_all.append(entity)
#             # insert_tel_source_coupling(coupling_list_all)
#             print('创建一二维耦合关系成功！')
#             session.close()
#             return True
#         elif model_mode == "scs":
#             # TODO 水文的耦合方式  待实现
#
#
#
#
#
#             return True
#     except ValueError:
#         print("创建一二维耦合关系失败！")
#         session.close()
#         return False
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text


def process_node(session_factory, node_id, model_id, node_type, total, index):
    """处理单个节点，查找最近的顶点"""
    try:
        with session_factory() as session:
            # 根据节点类型选择相应的表
            table_name = "swmm_node_junction" if node_type == "junction" else "swmm_node_outfall"

            sql = f"""
                SELECT b.vertex_code, st_distance(a.geom, b.geom) as dist
                FROM {table_name} a, tel_mesh_vertex b 
                WHERE a.node_id = :node_id AND a.model_id = :model_id
                ORDER BY dist
                LIMIT 1
            """

            result = session.execute(text(sql), {"node_id": node_id, "model_id": model_id})
            row = result.first()

            if row:
                vertex_code = row.vertex_code
                print(f"正在处理{node_type}:{index}/{total} :{vertex_code}")
                return vertex_code
            return None
    except Exception as e:
        print(f"处理节点 {node_id} 时出错: {str(e)}")
        return None


def create_coupling(model_id, model_mode):
    # 一二维耦合关系入库
    try:
        if model_mode == "swmm":
            engine = create_pg_engine()
            session_factory = sessionmaker(bind=engine)
            session = session_factory()

            # 查询所有节点
            juctionList = session.query(SwmmNodeJunction).filter_by(model_id=model_id).all()
            outfallList = session.query(SwmmNodeOutfall).filter_by(model_id=model_id).all()
            all_nodes = [
                            (node.node_id, "junction") for node in juctionList
                        ] + [
                            (node.node_id, "outfall") for node in outfallList
                        ]

            total_nodes = len(all_nodes)
            print(f"总共需要处理 {total_nodes} 个节点")

            # 使用线程池并行处理节点
            coupling_list_all = []
            max_workers = min(32, total_nodes)  # 限制最大线程数

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_node = {
                    executor.submit(
                        process_node,
                        session_factory,
                        node_id,
                        model_id,
                        node_type,
                        total_nodes,
                        i + 1  # 索引从1开始
                    ): (node_id, node_type)
                    for i, (node_id, node_type) in enumerate(all_nodes)
                }

                # 收集结果
                for future in future_to_node:
                    node_id, node_type = future_to_node[future]
                    vertex_code = future.result()

                    if vertex_code:
                        entity = TelSourceCoupling()
                        entity.model_id = model_id
                        entity.vertex_code = vertex_code
                        entity.node_code = node_id
                        entity.coup_type = model_mode
                        coupling_list_all.append(entity)

            # 批量插入
            if coupling_list_all:
                insert_tel_source_coupling(coupling_list_all)
                print(f'成功创建{len(coupling_list_all)}条一二维耦合关系！')
            else:
                print('未找到匹配的一二维耦合关系')

            session.close()
            return True
        elif model_mode == "scs":
            # TODO 水文的耦合方式  待实现
            return True
    except Exception as e:
        print(f"创建一二维耦合关系失败：{str(e)}")
        if 'session' in locals() and session:
            session.close()
        return False

def generate_relation_and_source_file(relation_meta, source_path, coupling_type, model_id, task_code):
    """
    生成relation.txt和source.txt，source.txt格式为：
    T Q(1) Q(2) ... Q(n)
    S m3/s ...
    0 0.1 0.2 ...
    ...
    task_code是任务编号，用于查询result_swmm_node表和result_scs_flow表，task_code需要从一维模型中获取。
    """
    import numpy as np
    from app.dao.tel_source_coupling import query_tel_source_coupling
    from app.utils.db_utils import create_pg_connect

    data = query_tel_source_coupling(model_id)
    if coupling_type == "swmm":
        data = [item for item in data if getattr(item, 'coup_type', None) == 'swmm']
        # 写relation.txt
        with open(relation_meta, 'w', encoding='utf-8') as f:
            f.write("node_code,vertex_code\n")
            for item in data:
                f.write(f"{item.node_code},{item.vertex_code}\n")
        # vertex顺序
        vertex_list = [item.vertex_code for item in data]
        node_list = [item.node_code for item in data]

        # 一次性查询所有节点所有时刻的 flooding_losses
        conn = create_pg_connect()
        cursor = conn.cursor()
        # 查询所有相关节点的所有时刻数据
        sql = """
            SELECT node_id, time, flooding_losses
            FROM result_swmm_node
            WHERE task_code = %s AND node_id = ANY(%s)
            ORDER BY time ASC
        """
        cursor.execute(sql, (task_code, node_list))
        rows = cursor.fetchall()
        # 组织数据：{dt: {node_code: flooding_losses}}
        time_dict = {}
        for node_id, time, flooding_losses in rows:
            if time not in time_dict:
                time_dict[time] = {}
            time_dict[time][node_id] = float(flooding_losses or 0)
        time_list = sorted(time_dict.keys())
        # 按时间和node顺序组装二维数组
        all_losses = []
        for time in time_list:
            losses = [time_dict[time].get(node, 0.0) for node in node_list]
            all_losses.append(losses)
        all_losses = np.array(all_losses)  # shape: (时刻数, vertex数)
        # 写source.txt
        with open(source_path, 'w', encoding='utf-8') as f:
            f.write('T ' + ' '.join([f'Q({i+1})' for i in range(len(vertex_list))]) + '\n')
            f.write('S ' + ' '.join(['m3/s']*len(vertex_list)) + '\n')
            for i, t in enumerate(time_list):
                t_sec = int((t - time_list[0]).total_seconds())
                vals = ' '.join([f'{v:.3f}' for v in all_losses[i]])
                f.write(f'{t_sec} {vals}\n')
        cursor.close()
        conn.close()
        return True
    elif coupling_type == "scs":
        data = [item for item in data if getattr(item, 'coup_type', None) == 'scs']
        # 写relation.txt
        with open(relation_meta, 'w', encoding='utf-8') as f:
            f.write("recd,vertex_code\n")
            for item in data:
                f.write(f"{item.node_code},{item.vertex_code}\n")
        # vertex顺序
        vertex_list = [item.vertex_code for item in data]
        recd_list = [item.node_code for item in data]
        # 构建断面到vertex的映射
        recd_to_vertex = {}
        for item in data:
            recd_to_vertex.setdefault(item.node_code, []).append(item.vertex_code)
        # 查询所有断面的流量，按时间对齐
        import psycopg2
        from app.utils.db_utils import create_pg_connect
        conn = create_pg_connect()
        cursor = conn.cursor()
        # 获取所有断面的所有时刻
        cursor.execute("SELECT DISTINCT dt FROM result_scs_flow WHERE task_code=%s ORDER BY dt ASC", (task_code,))
        time_rows = cursor.fetchall()
        time_list = [row[0] for row in time_rows]
        # 构建 recd->时刻->流量
        recd_q = {}
        for recd in recd_to_vertex:
            cursor.execute("SELECT dt, out_q FROM result_scs_flow WHERE task_code=%s AND recd=%s ORDER BY dt ASC", (task_code, recd))
            rows = cursor.fetchall()
            q_map = {row[0]: float(row[1]) for row in rows}
            recd_q[recd] = q_map
        # 按vertex顺序，组装每个时刻的流量
        with open(source_path, 'w', encoding='utf-8') as f:
            f.write('T ' + ' '.join([f'Q({i+1})' for i in range(len(vertex_list))]) + '\n')
            f.write('S ' + ' '.join(['m3/s']*len(vertex_list)) + '\n')
            # 插入-30秒全0行
            f.write('-30 ' + ' '.join(['0']*len(vertex_list)) + '\n')
            for t in time_list:
                vals = []
                for recd, vlist in recd_to_vertex.items():
                    q = recd_q.get(recd, {}).get(t, 0.0)
                    avg_q = q / len(vlist) if vlist else 0.0
                    for v in vlist:
                        vals.append((vertex_list.index(v), avg_q))
                # 按vertex顺序排序
                vals_sorted = [0.0]*len(vertex_list)
                for idx, val in vals:
                    vals_sorted[idx] = val
                t_sec = int((t - time_list[0]).total_seconds())
                f.write(f'{t_sec} ' + ' '.join([f'{v:.3f}' for v in vals_sorted]) + '\n')
        cursor.close()
        conn.close()
        return True
    else:
        return False

def replace_block(lines, key, new_block):
    """
    用于替换以 key 开头的区块，区块以下一个大写字母开头的 key 或文件结尾为止。
    替换时不会引入多余空行。
    """
    import re
    new_lines = []
    i = 0
    n = len(lines)
    key_pattern = re.compile(rf'^{re.escape(key)}\s*:', re.IGNORECASE)
    block_start = None

    # 找到 key 行
    while i < n:
        if key_pattern.match(lines[i]):
            block_start = i
            break
        i += 1

    if block_start is None:
        # key 不存在，直接在末尾添加
        if not lines[-1].endswith('\n'):
            lines[-1] += '\n'
        new_lines = lines + [f"{key}: {new_block}\n"]
        return new_lines

    # 找到区块结束
    block_end = block_start + 1
    while block_end < n:
        if re.match(r'^[A-Z][A-Z\s]*\s*:', lines[block_end]):
            break
        block_end += 1

    # 拼接新内容
    new_lines = lines[:block_start]
    # 新区块内容
    if new_block and new_block.strip():
        new_block_lines = [f"{key}: {new_block.splitlines()[0]}\n"]
        for line in new_block.splitlines()[1:]:
            new_block_lines.append(line.rstrip('\n') + '\n')
        new_lines.extend(new_block_lines)
    else:
        new_lines.append(f"{key}:\n")
    # 拼接剩余内容
    new_lines.extend(lines[block_end:])
    return new_lines

def update_simulation_cas(sim_cas_path, relation_csv_path, source_txt_path):
    # 1. 读取simulation.cas，获取TIME STEP
    with open(sim_cas_path, 'r', encoding='utf-8') as f:
        cas_lines = f.readlines()
    time_step = 1
    for line in cas_lines:
        if line.strip().startswith('TIME STEP'):
            try:
                time_step = float(line.split(':')[1].strip())
            except Exception:
                time_step = 1
            break
    # 2. 读取source.txt，获取最大时间
    with open(source_txt_path, 'r', encoding='utf-8') as f:
        source_lines = f.readlines()
        time_strs = [line.split()[0] for line in source_lines if line and line[0].isdigit()]
    end_time = float(time_strs[-1]) if time_strs else 0
    number_of_time_steps = int(end_time // time_step) if end_time else 0
    graphic_printout_period = max(1, number_of_time_steps // 24)
    listing_printout_period = graphic_printout_period
    # 3. 读取relation.csv，获取vertex_code
    with open(relation_csv_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]
        vertex_codes = [line.strip().split(',')[1] for line in lines if line.strip() and len(line.strip().split(',')) > 1 and line.strip().split(',')[1]]
    # 4. 组织GLOBAL NUMBERS OF SOURCE NODES，每4个一行，行末加分号，无多余空分号
    global_nodes_lines = []
    for i in range(0, len(vertex_codes), 4):
        group = [v for v in vertex_codes[i:i+4] if v]
        if group:
            line = ';'.join(group)
            global_nodes_lines.append(line)
    # 每行末尾加分号，最后一行不加
    if global_nodes_lines:
        global_nodes_str = '\n'.join([l+';' for l in global_nodes_lines[:-1]] + [global_nodes_lines[-1]])
    else:
        global_nodes_str = ''
    # 5. WATER DISCHARGE OF SOURCES，同上
    water_discharge_lines = []
    for i in range(0, len(vertex_codes), 4):
        group = [v for v in vertex_codes[i:i+4] if v]
        if group:
            line = ';'.join(['0']*len(group))
            water_discharge_lines.append(line)
    if water_discharge_lines:
        water_discharge_str = '\n'.join([l+';' for l in water_discharge_lines[:-1]] + [water_discharge_lines[-1]])
    else:
        water_discharge_str = ''
    # 6. 替换simulation.cas相关字段
    cas_lines = replace_block(cas_lines, 'GLOBAL NUMBERS OF SOURCE NODES', global_nodes_str)
    cas_lines = replace_block(cas_lines, 'WATER DISCHARGE OF SOURCES', water_discharge_str)
    # 替换其他字段
    cas_lines = replace_block(cas_lines, 'NUMBER OF TIME STEPS', str(number_of_time_steps))
    cas_lines = replace_block(cas_lines, 'GRAPHIC PRINTOUT PERIOD', str(graphic_printout_period))
    cas_lines = replace_block(cas_lines, 'LISTING PRINTOUT PERIOD', str(listing_printout_period))
    # 7. 写回
    with open(sim_cas_path, 'w', encoding='utf-8') as f:
        f.writelines(cas_lines)

def preprocess_model_data(model_id, task_code):
    data = query_tel_model_info(model_id)
    

    steering_meta = os.path.join(data.model_path, data.steering_file)
    #geometry_meta = os.path.join(data.model_path, data.geometry_file)
    #boundary_meta = os.path.join(data.model_path, data.boundary_file)
    relation_meta = os.path.join(data.model_path, data.relation_file)
    source_meta = os.path.join(data.model_path, data.source_file)
    #result_meta = os.path.join(data.model_path, data.result_file)
    #task_path = os.path.join(os.path.dirname(steering_meta), task_code)
    #print(task_path) 
    #if os.path.exists(task_path):
    #    shutil.rmtree(task_path)
    #os.mkdir(task_path)

    # TODO: data.couping_type 确定耦合的流量过程从那张表取 MASCARET:从result_mascaret_process表取； SWMM:从result_swmm_node表取
    coupling_type = data.coupling_type
    print("coupling_type", coupling_type)
    generate_relation_and_source_file(relation_meta, source_meta, coupling_type, model_id, task_code)
    print("create relation and source file success!")
    update_simulation_cas(steering_meta, relation_meta, source_meta)
    return steering_meta

def execute_telemac_task(model_id, task_code):
    try:
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
        src_dir = os.path.join(model_path, 'model2D')
        slf_path = os.path.join(src_dir, "results.slf")
        steering_path = preprocess_model_data(model_id, task_code)
        task_path = os.path.join(os.path.dirname(steering_path), task_code)
        subprocess.run(["telemac2d.py", "--ncsize", "8", "-w", task_path, steering_path], check=True, cwd=os.path.dirname(steering_path))
        parse_slf_result(task_code, slf_path)
        return True
    except Exception as e:
        print(e)
        return False
    finally:
        executeCaPostProcessing(model_id,task_code,"telemac")


def parse_slf_result(task_code, slf_path):
    try:
        slf = SELAFIN(slf_path)
        times = slf.tags['times']
        lastTimes = times[-24:]
        var_names = [item.rstrip() for item in slf.VARNAMES]
        h = del_slf_to_array(slf, 'WATER DEPTH', lastTimes, var_names)[::-1]
        u = del_slf_to_array(slf, 'VELOCITY U', lastTimes, var_names)[::-1]
        v = del_slf_to_array(slf, 'VELOCITY V', lastTimes, var_names)[::-1]
        z = del_slf_to_array(slf, 'FREE SURFACE', lastTimes, var_names)[::-1]
        vertex_process_list = []
        for i in range(len(z)):
            for j in range(len(z[i])):
                item = (task_code, i, int(lastTimes[j]), float(h[i][j]), float(u[i][j]), float(v[i][j]), 0, 0, float(z[i][j]))
                vertex_process_list.append(item)
            if i%500 ==0:
              print(f"已插入{i}/{len(z)}组数据")
        insert_vertex_process(vertex_process_list)
        print('解析slf结果文件成功！')
        return True
    except ValueError:
        print("解析slf结果文件失败！")
        return False


def del_slf_to_array(slf, variable, times, var_names):
    # 返回特定 variable 的所有值
    frames = range(0, len(times) - 1, 1)
    # frames = [0,5]
    variable = variable.encode()
    z_var = []
    for i in frames:
        z_var.append(slf.getVariablesAt(i, [var_names.index(variable)]))
    z_var = np.squeeze(z_var, axis=None)
    # z_var = np.insert(z_var, 0, [slf.MESHX, slf.MESHY], axis=0)    # 用于辨别点的位置，暂时不使用
    z_var = np.rot90(z_var, k=1, axes=(0, 1))
    return z_var

def computeAreaByvertexCode(model_id, vertex_code):
    #todo 得到tel_mesh_vertex数据
    pass
        #todo 根据model_id,vertex_code找到tel_mesh_rel里面的area列表，求和乘以三分之一



if __name__ == '__main__':
    # aaa = '55ea4be579cc4ec5a8dd9e4d3f4bfd17'
    # bbb = 'D:\\cloudFloodServer-workspace\\model\\55ea4be579cc4ec5a8dd9e4d3f4bfd17\\model2D\\model2D.zip'
    # parse_telemac_file(aaa, bbb, '3857')
    # create_coupling(aaa)
    # model_id_p = '55ea4be579cc4ec5a8dd9e4d3f4bfd17'
    # task_code_p = '7f6801ef154040fe802848ca0c3a6fc0'
    # execute_telemac_task(model_id_p, task_code_p)

    # 团洲垸解析
    # aaa = 'tzy123'
    # bbb = 'D:\\CloudMusic\\jihe.zip'
    # parse_telemac_file(aaa, bbb, '4547')
    
    # aaa = 'tzy123task'
    # bbb = 'E:\\work\\2024\\12-团洲垸样例\\data\\模型数据\\r2d.slf'
    # parse_slf_result(aaa, bbb)

    # # 郧西模型解析
    # aaa = 'yx123'
    # bbb = 'D:\\modelBase\\th\\geom_input.zip'
    # parse_telemac_file(aaa, bbb, '4546')
    # create_coupling("A4_420322_YX","swmm")
    # execute_telemac_task("A11_420322_WLH","7b255b252fe44e86825d4834e1deafb7")
    # slf = SELAFIN("results_XH_6h.slf")
    # i=1
    # i=2
    execute_telemac_task("A4_420322_YX","c20782e7ec67451e9531ab54270bed9e")




