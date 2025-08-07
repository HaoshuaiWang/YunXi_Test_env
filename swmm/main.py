# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/18 14:27
# description:swmm模型操作接口
import concurrent
import os
import shutil
import sys
import uuid
from configparser import ConfigParser
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import requests
from pyswmm import Simulation, Nodes, Links
from pyswmm import Output
from app.utils.java_utils import executeCaPostProcessing
from app.telemac.main import execute_telemac_task
from app.utils import parameter_verifier
from app.cellAuto.cellAuto_main import execute_ca_task, execute_swmm_ca_task
from app.dao.result_swmm_catchment import insert_catchment_result
from app.dao.result_swmm_link import insert_link_result
from app.dao.result_swmm_node import insert_node_result
from app.dao.result_swmm_summary import insert_swmm_summary
from app.dao.result_swmm_system import insert_system_result
from app.dao.swmm_link_xsections import insert_swmm_link_xsections
from app.dao.swmm_model_info import insert_swmm_model_info, query_inp_path
from app.dao.swmm_rain_gages import insert_swmm_rain_gages
from app.dao.swmm_rain_source import query_start_time, query_end_time, query_rain_source
from app.dao.swmm_sub_catchment import insert_swmm_catchment
from app.dao.swmm_node_junction import insert_swmm_node_junction
from app.dao.swmm_node_outfall import insert_swmm_node_outfall
from app.dao.swmm_link_conduit import insert_swmm_link_conduit
from app.swmm.parse_inp import *
from app.swmm.parse_out import *
from app.utils.db_utils import query_server_host


current_file = os.path.abspath(__file__)
# 获取当前脚本所在目录（src目录）
current_dir = os.path.dirname(current_file)
# 获取父目录（项目根目录）
parent_dir = os.path.dirname(current_dir)
# 获取并添加根目录到系统路径
sys.path.append(parent_dir)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)

# 解析一维模型文件√
def parse_swmm_file(file_path, model_id, srid):
    try:
        main_dic = {}  # 模型基础数据
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            sub_list = []
            i = 0
            while i < len(lines):
                sub_list.append(lines[i])
                if lines[i] == '\n':
                    key = sub_list[0].split('\t')[0].replace('\n', '')
                    main_dic[key] = sub_list
                    sub_list = []
                i += 1
        # 打印前5个键值对用于调试
        print("main_dic内容预览:")
        for idx, (k, v) in enumerate(main_dic.items()):
            if idx >= 200: break
            print(f"{k}: {len(v)}行数据")
        model_info = parse_inp_option(model_id, file_path, srid, main_dic)
        gages_list = parse_swmm_gages(model_id, srid, main_dic)
        catchment_list = parse_swmm_catchment(model_id, srid, main_dic)
        junction_list, outfall_list = parse_swmm_node(model_id, srid, main_dic)
        conduit_list = parse_swmm_conduit(model_id, srid, main_dic)
        xsections_list = parse_swmm_link_xsections(model_id, srid, main_dic)
        insert_swmm_link_xsections(xsections_list)
        insert_swmm_model_info(model_info)
        insert_swmm_rain_gages(gages_list)
        insert_swmm_catchment(catchment_list)
        insert_swmm_node_junction(junction_list)
        insert_swmm_node_outfall(outfall_list)
        insert_swmm_link_conduit(conduit_list)
        print("swmm model parse success!")
        return True
    except ValueError:
        print("swmm model parse failed!")
        return False


# 执行一维样例模型√
def execute_swmm_example(model_id):
    sim = None  # 提前定义sim变量，避免作用域问题
    try:
        inp_file = query_inp_path(model_id)
        example_inp = create_example_inp(inp_file)
        sim = Simulation(example_inp)  # 初始化sim对象
        sim.execute()
        print()
        print("swmm example execute success!")
        return True
    except ValueError as ex:
        print()
        print(f"swmm example execute failed! {str(ex)}")  # 修复字符串拼接问题
        return False
    except Exception as e:
        print(f"Unexpected error: {str(e)}")  # 捕获其他可能的异常
        return False
    finally:
        if sim is not None:  # 检查sim是否被正确初始化
            sim.close()  # 关闭sim对象，确保资源释放


# 执行一维模型
def execute_swmm_task(model_id, task_code, task_mode):
    """
    执行swmm一维模型（异步版本）
    :param model_id: 模型编码
    :param task_code: 任务编码
    :param task_mode: 任务类型 swmm:表示只执行一维管网模型;ca:表示二维采用元胞自动机;telemac:表示二维采用telemac引擎
    :return: 是否执行成功！
    """
    sim = None
    from main import update_model_state
    try:
        inp_path, out_path = create_boundary(model_id, task_code)
        print(f"模型输入路径: {inp_path}")

        with Simulation(inp_path) as sim:
            sim.execute()

        node_result_list = parse_out_file(task_code, out_path)
        print("swmm task execute success!")

        if task_mode == "ca":
            ca_input = generate_ca_input(node_result_list)
            outputPath = execute_swmm_ca_task(model_id, task_code, ca_input)
            print("cellAuto task execute success!")

            # 调用java后处理方法
            flag = execute_ca_pp(model_id, task_code, outputPath)
            if flag:
                print("cellAuto post processing success!")
            else:
                print("cellAuto post processing failed!")

        elif task_mode == "telemac":
            execute_telemac_task(model_id, task_code)

    except ValueError as ex:
        print(f"swmm task execute failed: {str(ex)}")
    except Exception as ex:
        print(f"未知错误: {str(ex)}")
    finally:
        if task_mode =="swmm":
            executeCaPostProcessing(model_id,task_code,task_mode)
        if sim:
            sim.close()

        # 执行一维模型



def execute_swmm_task_bak(model_id, task_code):
    try:
        inp_path, out_path = create_boundary(model_id, task_code)

        with Simulation(inp_path) as sim:
            l1 = Links(sim)["177"]
            j1 = Nodes(sim)["32"]
            i = 0
            for step in sim:
                print(str(i) + "\t" + str(sim.current_time) + "\t" + str(j1.total_outflow))
                # print(str(i) + "\t" + str(sim.current_time) + "\t" + str(j1.total_outflow) + "\t" + str(l1.depth))
                i = i + 1

            # sim.execute()
        # parse_out_file(task_code, out_path)
        print()
        print("swmm model execute success!")
        return True
    except ValueError as ex:
        print()
        print("swmm model execute failed!" + ex)
        return False


# 创建样例任务
def create_example_inp(inp_file):
    # 确保路径分隔符统一使用os.path.sep，兼容不同操作系统
    inp_name = inp_file.split(os.path.sep)[-1]

    # 生成唯一ID作为任务目录名
    uid = str(uuid.uuid4()).replace('-', '')

    # 修改任务目录位置，避免与原始inp文件目录冲突
    # 使用项目临时目录或自定义临时目录，而不是输入文件所在目录
    base_temp_dir = os.path.join(os.getcwd(), "temp_swmm")
    os.makedirs(base_temp_dir, exist_ok=True)  # 确保临时目录存在

    # 构建任务目录
    task_path = os.path.join(base_temp_dir, uid)

    # 确保任务目录不存在，如果存在则删除
    if os.path.exists(task_path):
        shutil.rmtree(task_path)

    # 创建新的任务目录
    os.mkdir(task_path)

    # 构建目标文件路径
    task_file = os.path.join(task_path, inp_name)

    # 复制文件
    shutil.copy2(inp_file, task_file)  # 使用copy2保留更多文件元数据

    return task_file


def create_boundary(model_id, task_code):
    inp_file = query_inp_path(model_id)
    inp_name = inp_file.split(os.path.sep)[-1]
    # 使用项目根目录下的temp_swmm目录，但添加额外的子目录避免冲突
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
    task_path = os.path.join(src_dir, task_code)

    # 确保任务目录不存在，如果存在则删除
    if os.path.exists(task_path):
        shutil.rmtree(task_path)
    # 创建新的任务目录
    os.mkdir(task_path)

    # 构建目标文件路径
    task_file = os.path.join(task_path, inp_name)
    out_file = os.path.join(task_path, inp_name.split('.')[0] + '.out')

    # 关键修改：添加路径检查，防止相同文件错误
    if os.path.abspath(inp_file) == os.path.abspath(task_file):
        unique_inp_name = f"{uuid.uuid4().hex[:8]}_{inp_name}"
        task_file = os.path.join(task_path, unique_inp_name)
        print(f"警告: 源文件和目标文件路径冲突，使用新文件名: {unique_inp_name}")

    # 复制文件
    shutil.copy2(inp_file, task_file)
    # 使用二进制模式复制文件

    dt_format2 = '%m/%d/%Y\t%H:%M'
    dt_format3 = '%m/%d/%Y\t%H:%M:%S'

    # 查询开始时间和结束时间，并添加空值检查
    start_time = query_start_time(task_code)
    end_time = query_end_time(task_code)

    # 检查查询结果是否为空
    if start_time is None:
        raise ValueError(f"未找到任务 {task_code} 的开始时间")

    if end_time is None:
        raise ValueError(f"未找到任务 {task_code} 的结束时间")

    rain_source = query_rain_source(task_code)
    str_start_date, str_start_time = start_time.strftime(dt_format3).split()
    str_end_date, str_end_time = end_time.strftime(dt_format3).split()

    series = list()
    for item in rain_source:
        series.append(f'{item.source_name}\t\t{item.time.strftime(dt_format2)}\t\t{item.value}')

    str_series = '\n'.join(series)
    factory_sub = FactorySub()

    with open(task_file) as f:
        text = f.read()

    text = factory_sub.sub_start_date(text, str_start_date)
    text = factory_sub.sub_start_time(text, str_start_time)
    text = factory_sub.sub_report_start_date(text, str_start_date)
    text = factory_sub.sub_report_start_time(text, str_start_time)
    text = factory_sub.sub_end_date(text, str_end_date)
    text = factory_sub.sub_end_time(text, str_end_time)
    text = factory_sub.sub_timeseries(text, str_series)

    with open(task_file, 'w') as f:
        f.write(text)

    return task_file, out_file


# 解析输出文件
def parse_out_file(task_code, out_path):
    with Output(out_path) as out:
        summary_result = parse_summary_result(task_code, out)
        catchment_result_list = parse_catchment_result(task_code, out)
        link_result_list = parse_link_result(task_code, out)
        node_result_list = parse_node_result(task_code, out)
        system_result_list = parse_system_result(task_code, out)
        insert_swmm_summary(summary_result)
        insert_catchment_result(catchment_result_list)
        insert_link_result(link_result_list)
        insert_node_result(node_result_list)
        insert_system_result(system_result_list)
    return node_result_list


def generate_ca_input(node_result_list):
    ca_input = {}
    # 初始化分组字典
    grouped_dict = {}
    for item in node_result_list:
        node_id = item.node_id
        if node_id not in grouped_dict:
            grouped_dict[node_id] = []
        grouped_dict[node_id].append(item)

    for node_id in grouped_dict:
        losses_strings = [str(item.flooding_losses) for item in grouped_dict[node_id]]
        ca_input[node_id] = losses_strings
    return ca_input
def execute_ca_pp(model_id, task_code, result_path):
    """
    cellAuto后处理方法（调用java项目）
    :param model_id: 模型编码
    :param task_code: 任务编码
    :param result_path: 结果文件路径
    :return: 是否执行完成
    """
    server = query_server_host() + "urbanFlood/floodManager/executeCaPostProcessing"
    params = {
        'modelId': model_id,
        'taskCode': task_code,
        'resultPath': result_path
    }
    response = requests.get(server, params=params)
    return response.json()

    
if __name__ == '__main__':
    model_id = 'A4_420322_YX'
    task_code = '06713640077a43fdabed59c1fc0d16a6'
    # result_path = '3'
    # parse_swmm_file("C:\\Users\\ASUS\\flood-control-mod-v2\\郧西swmm (1).inp","A4_420322_YX","4546")
    execute_swmm_task(model_id, task_code, 'telemac')

    #print(sys.path)