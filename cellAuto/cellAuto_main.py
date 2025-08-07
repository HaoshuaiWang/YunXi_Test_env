# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024年9月10日10:31:51
# description:元胞自动机操作逻辑
import os
import re
import shutil
import subprocess
import uuid
import sys

from pathlib import Path
from app.dao.ca_model_info import query_ca_model_path
from app.dao.ca_source_coupling import query_ca_coupling
from app.dao.result_run_rec import query_task_info
from app.dao.result_swmm_node import query_node_result, query_node_losses


# 执行二维样例模型
def execute_ca_example(model_id):
    try:
        model_path = query_ca_model_path(model_id)
        task_path = create_ca_workspace(model_path)
        inputPath = os.path.join(task_path, 'input')
        os.makedirs(inputPath)
        outputPath = os.path.join(task_path, 'output')
        os.makedirs(outputPath)

        # Create inflow.csv for 2D simulation
        periods, reportSteps, inflowCsvList = write_example_inflow(model_id, inputPath)

        # Modify 2D configure file
        timeSim = str(periods * reportSteps)
        inflowCsvListStr = ','.join(inflowCsvList)
        model2DPath = os.path.join(task_path, 'model2D.csv')
        modify_config(model2DPath, timeSim, inflowCsvListStr)

        if sys.platform.startswith('win'):
            # win下的执行方法
            fileName = Path(__file__).parent.parent.parent / 'resources' / 'caflood.exe'
            # Execute 2D simulation and save the log to the txt
            logPath = os.path.join(task_path, 'log.txt')
            with open(logPath, "w") as f:
                subprocess.call([fileName, '/WCA2D', task_path, 'model2D.csv', outputPath], stdout=f)
            # 删除测试文件夹
            # shutil.rmtree(task_path)
            print()
            print("cellAuto example execute success (Windows)!")
            return True
        elif sys.platform.startswith('linux'):
            # linux下的执行方法
            fileName = Path(__file__).parent.parent.parent / 'resources' / 'caflood.exe'
            # Execute 2D simulation and save the log to the txt
            logPath = os.path.join(task_path, 'log.txt')
            with open(logPath, "w") as f:
                subprocess.call([fileName, '/WCA2D', task_path, 'model2D.csv', outputPath], stdout=f)
            # 删除测试文件夹
            # shutil.rmtree(task_path)
            print()
            print("cellAuto example execute success (Linux)!")
            return True
        else:
            print()
            print("unknown os!")
            return False

    except ValueError as ex:
        print()
        print("cellAuto example execute failed!" + ex)
        return False


# 执行二维模型
def execute_swmm_ca_task(model_id, task_code, ca_input):
    try:
        model_path = query_ca_model_path(model_id)
        task_path = create_ca_workspace(model_path)
        inputPath = os.path.join(task_path, 'input')
        os.makedirs(inputPath)
        outputPath = os.path.join(task_path, 'output')
        os.makedirs(outputPath)

        # Create inflow.csv for 2D simulation
        periods, reportSteps, inflowCsvList = write_task_inflow_from_swmm(model_id, task_code, inputPath, ca_input)

        # Modify 2D configure file
        timeSim = str(periods * reportSteps)
        inflowCsvListStr = ','.join(inflowCsvList)
        model2DPath = os.path.join(task_path, 'model2D.csv')
        modify_config(model2DPath, timeSim, inflowCsvListStr)

        if sys.platform.startswith('win'):
            # win下的执行方法
            fileName = Path(__file__).parent.parent.parent / 'resources' / 'caflood.exe'
            # Execute 2D simulation and save the log to the txt
            logPath = os.path.join(task_path, 'log.txt')
            with open(logPath, "w") as f:
                subprocess.call([fileName, '/WCA2D', task_path, 'model2D.csv', outputPath], stdout=f)
            # 删除测试文件夹
            # shutil.rmtree(task_path)
            print()
            print("cellAuto example execute success (Windows)!")
            return outputPath
        elif sys.platform.startswith('linux'):
            # linux下的执行方法
            fileName = Path(__file__).parent.parent.parent / 'resources' / 'caflood.exe'
            # Execute 2D simulation and save the log to the txt
            logPath = os.path.join(task_path, 'log.txt')
            with open(logPath, "w") as f:
                subprocess.call([fileName, '/WCA2D', task_path, 'model2D.csv', outputPath], stdout=f)
            # 删除测试文件夹
            # shutil.rmtree(task_path)
            print()
            print("cellAuto example execute success (Linux)!")
            return outputPath
        else:
            print()
            print("unknown os!")
            return False

    except ValueError as ex:
        print()
        print("cellAuto example execute failed!" + ex)
        return False


# 执行二维模型(数据源从库里取)
def execute_ca_task(model_id, task_code):
    """
    从数据库取数据生成输入文件计算二维模型（生成input文件速度好慢 o(╥﹏╥)o） 放弃
    :param model_id: 模型编码
    :param task_code: 任务编码
    :return:
    """
    try:
        model_path = query_ca_model_path(model_id)
        task_path = create_ca_workspace(model_path)
        inputPath = os.path.join(task_path, 'input')
        os.makedirs(inputPath)
        outputPath = os.path.join(task_path, 'output')
        os.makedirs(outputPath)

        # Create inflow.csv for 2D simulation
        periods, reportSteps, inflowCsvList = write_task_inflow(model_id, task_code, inputPath)

        # Modify 2D configure file
        timeSim = str(periods * reportSteps)
        inflowCsvListStr = ','.join(inflowCsvList)
        model2DPath = os.path.join(task_path, 'model2D.csv')
        modify_config(model2DPath, timeSim, inflowCsvListStr)

        # Execute 2D simulation and save the log to the txt
        logPath = os.path.join(task_path,  'log.txt')
        with open(logPath, "w") as f:
                subprocess.call(['caflood.exe', '/WCA2D', task_path, 'model2D.csv', outputPath], stdout=f)

        print()
        print("cellAuto example execute success!")
        return True
    except ValueError as ex:
        print()
        print("cellAuto example execute failed!" + ex)
        return False


# 创建二维样例模型
def create_ca_workspace(model_file):
    uid = str(uuid.uuid4()).replace('-', '')
    task_path = os.path.join(os.path.dirname(model_file), uid)
    if os.path.exists(task_path):
        shutil.rmtree(task_path)
    os.mkdir(task_path)
    source_dir = Path(model_file).parent
    source_dem = os.path.join(source_dir, 'model2D.asc')
    source_wd = os.path.join(source_dir, 'WDRaster.csv')
    source_vel = os.path.join(source_dir, 'VELRaster.csv')
    source_wdp = os.path.join(source_dir, 'WDPoints.csv')
    task_file = os.path.join(task_path, "model2D.csv")   # 模型配置文件
    dem_file = os.path.join(task_path, "model2D.asc")    # 地形配置文件
    wd_file = os.path.join(task_path, "WDRaster.csv")    # 水深配置文件
    vel_file = os.path.join(task_path, "VELRaster.csv")  # 流向配置文件
    wdp_file = os.path.join(task_path, "WDPoints.csv")  # 水深点配置文件
    shutil.copy(model_file, task_file)
    shutil.copy(source_dem, dem_file)
    shutil.copy(source_wd, wd_file)
    shutil.copy(source_vel, vel_file)
    shutil.copy(source_wdp, wdp_file)
    return task_path


# 创建二维模型样例入流文件
def write_example_inflow(model_id, inputDir):
    periods = 12
    reportSteps = 300
    inflowCsvList = list()
    timeList = [(i + 1) * reportSteps for i in range(periods)]
    overflowList = [1000 for i in range(periods)]
    timeStr = ','.join(list(map(str, timeList)))
    inflowStr = ','.join(list(map(str, overflowList)))
    dataList = query_ca_coupling(model_id)
    for item in dataList:
        nodeName = item['node_id']
        inflowCsv = os.path.join(inputDir, rf'{nodeName}.csv').replace('\\', '/')
        levels = inflowCsv.split("/")[-2:]
        inflowCsvName = os.sep.join(levels)
        inflowCsvList.append(inflowCsvName)
        zoneList = [item['x'], item['y'], 1, 1]
        zoneStr = ','.join(list(map(str, zoneList)))
        inflowDict = {}
        inflowDict['Event Name'] = 'Inflow Boundary Condition'
        inflowDict['Inflow (cumecs)'] = inflowStr
        inflowDict['Time (seconds)'] = timeStr
        inflowDict['Zone (tlx tly w h)'] = zoneStr
        with open(inflowCsv, 'w') as file:
            for key, values in inflowDict.items():
                file.write(f'{key},{values}\n')

    return periods, reportSteps, inflowCsvList


# 创建二维模型计算入流文件
def write_task_inflow(model_id, task_code, inputDir):
    data = query_task_info(task_code)
    periods = data.get("periods")
    reportSteps = data.get("intv")
    inflowCsvList = list()
    timeList = [(i + 1) * reportSteps for i in range(periods)]
    timeStr = ','.join(list(map(str, timeList)))
    dataList = query_ca_coupling(model_id)
    for item in dataList:
        nodeName = item['node_id']
        inflowCsv = os.path.join(inputDir, rf'{nodeName}.csv').replace('\\', '/')
        levels = inflowCsv.split("/")[-2:]
        inflowCsvName = os.sep.join(levels)
        inflowCsvList.append(inflowCsvName)
        zoneList = [item['x'], item['y'], 1, 1]
        overflowList = query_node_losses(task_code, nodeName)

        # overflowList = query_overflow_List(task_code, nodeName)
        zoneStr = ','.join(list(map(str, zoneList)))
        inflowStr = ','.join(list(map(str, overflowList)))
        inflowDict = {}
        inflowDict['Event Name'] = 'Inflow Boundary Condition'
        inflowDict['Inflow (cumecs)'] = inflowStr
        inflowDict['Time (seconds)'] = timeStr
        inflowDict['Zone (tlx tly w h)'] = zoneStr
        with open(inflowCsv, 'w') as file:
            for key, values in inflowDict.items():
                file.write(f'{key},{values}\n')

    return periods, reportSteps, inflowCsvList


def write_task_inflow_from_swmm(model_id, task_code, inputDir, ca_input):
    data = query_task_info(task_code)
    periods = data.get("periods")
    reportSteps = data.get("intv")
    inflowCsvList = list()
    timeList = [(i + 1) * reportSteps for i in range(periods)]
    timeStr = ','.join(list(map(str, timeList)))
    dataList = query_ca_coupling(model_id)
    for item in dataList:
        nodeName = item['node_id']
        inflowCsv = os.path.join(inputDir, rf'{nodeName}.csv').replace('\\', '/')
        levels = inflowCsv.split("/")[-2:]
        inflowCsvName = os.sep.join(levels)
        inflowCsvList.append(inflowCsvName)
        zoneList = [item['x'], item['y'], 1, 1]
        overflowList = ca_input[nodeName]
        # overflowList = query_overflow_List(task_code, nodeName)
        zoneStr = ','.join(list(map(str, zoneList)))
        inflowStr = ','.join(list(map(str, overflowList)))
        inflowDict = {}
        inflowDict['Event Name'] = 'Inflow Boundary Condition'
        inflowDict['Inflow (cumecs)'] = inflowStr
        inflowDict['Time (seconds)'] = timeStr
        inflowDict['Zone (tlx tly w h)'] = zoneStr
        with open(inflowCsv, 'w') as file:
            for key, values in inflowDict.items():
                file.write(f'{key},{values}\n')

    return periods, reportSteps, inflowCsvList


# 查询溢流信息
def query_overflow_List(task_code, node_id):
    dataList = query_node_result(task_code, node_id)
    overflowList = list()
    for item in dataList:
        overflowList.append(item['flooding_losses'])
    return overflowList


# 修改二维配置信息
def modify_config(model2DPath, timeStr, inflowStr):
    #
    # Input:timeStr, inflowStr
    # Output:none
    # Purpose:modify the config file:modify the end time and inflow csv path.
    #
    with open(model2DPath, 'r') as file:
        content = file.read()

    patternTime = r'^(Time End\s+\(seconds\)\s+,).*$'
    modifiedTime = re.sub(patternTime, rf'\1 {timeStr}', content, flags=re.MULTILINE)

    # InflowCSV = "Inflow Event CSV," + inflowStr + "\n"
    InflowCSV = "Inflow Event CSV," + inflowStr
    # modifiedInflow = modifiedTime.replace("Inflow Event CSV,\n", InflowCSV)
    modifiedInflow = modifiedTime.replace("Inflow Event CSV,", InflowCSV)

    # patternInflow = r'^(Inflow Event CSV\s*,).*$'
    # modifiedInflow = re.sub(patternInflow, rf'\1 {inflowStr}', modifiedTime, flags=re.MULTILINE)

    with open(model2DPath, 'w') as file:
        file.write(modifiedInflow)
