# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/22 9:28
# description: 解析swmm模型文件类
import logging

from app.dao.swmm_link_xsections import SwmmLinkXsections
from app.dao.swmm_model_info import SwmmModelInfo
from app.dao.swmm_node_junction import SwmmNodeJunction
from app.dao.swmm_node_outfall import SwmmNodeOutfall
from app.dao.swmm_rain_gages import SwmmRainGages
from app.dao.swmm_sub_catchment import SwmmSubCatchment
from app.dao.swmm_link_conduit import SwmmLinkConduit
import re


class FactorySub:
    def __init__(self):
        pattern_timeseries = re.compile(r'(\[TIMESERIES]\s*?\n.*?\n.*?\n).+?(\[)', re.DOTALL)

        def sub_timeseries(text, repl):
            new_text, number_of_subs_made = pattern_timeseries.subn(
                lambda match_obj: f'{match_obj.group(1)}{repl}\n\n{match_obj.group(2)}',
                text)
            assert number_of_subs_made == 1
            return new_text
        self.sub_timeseries = sub_timeseries

        pattern_start_date = re.compile(r'^(START_DATE).*?$', re.MULTILINE)
        pattern_start_time = re.compile(r'^(START_TIME).*?$', re.MULTILINE)
        pattern_report_start_date = re.compile(r'^(REPORT_START_DATE).*?$', re.MULTILINE)
        pattern_report_start_time = re.compile(r'^(REPORT_START_TIME).*?$', re.MULTILINE)
        pattern_end_date = re.compile(r'^(END_DATE).*?$', re.MULTILINE)
        pattern_end_time = re.compile(r'^(END_TIME).*?$', re.MULTILINE)

        def _util_sub(pattern, text, repl):
            new_text, number_of_subs_made = pattern.subn(lambda match_obj: f'{match_obj.group(1)}\t\t{repl}', text)
            assert number_of_subs_made == 1
            return new_text

        def sub_start_date(text, repl):
            return _util_sub(pattern_start_date, text, repl)

        def sub_start_time(text, repl):
            return _util_sub(pattern_start_time, text, repl)

        def sub_report_start_date(text, repl):
            return _util_sub(pattern_report_start_date, text, repl)

        def sub_report_start_time(text, repl):
            return _util_sub(pattern_report_start_time, text, repl)

        def sub_end_date(text, repl):
            return _util_sub(pattern_end_date, text, repl)

        def sub_end_time(text, repl):
            return _util_sub(pattern_end_time, text, repl)

        self.sub_start_date = sub_start_date
        self.sub_start_time = sub_start_time
        self.sub_report_start_date = sub_report_start_date
        self.sub_report_start_time = sub_report_start_time
        self.sub_end_date = sub_end_date
        self.sub_end_time = sub_end_time

        pattern_source_nodes = re.compile(r'(GLOBAL NUMBERS OF SOURCE NODES=)[\d\s;]*\n')
        pattern_water_discharge = re.compile(r'(WATER DISCHARGE OF SOURCES=)[\d\s;]*\n')

        def sub_source_nodes(text, repl):
            new_text, number_of_subs_made = pattern_source_nodes.subn(
                lambda match_obj: f'{match_obj.group(1)}{repl}\n',
                text)
            assert number_of_subs_made == 1
            return new_text

        def sub_water_discharge(text, repl):
            new_text, number_of_subs_made = pattern_water_discharge.subn(
                lambda match_obj: f'{match_obj.group(1)}{repl}\n\n',
                text)
            assert number_of_subs_made == 1
            return new_text

        self.sub_source_nodes = sub_source_nodes
        self.sub_water_discharge = sub_water_discharge

        pattern_number_time_steps = re.compile(r'^(NUMBER OF TIME STEPS:).*?$', re.MULTILINE)

        def sub_number_time_steps(text, repl):
            new_text, number_of_subs_made = pattern_number_time_steps.subn(
                lambda match_obj: f'{match_obj.group(1)}\t\t\t{repl}',
                text)
            assert number_of_subs_made == 1
            return new_text

        self.sub_number_time_steps = sub_number_time_steps


# 解析模型设置数据
def parse_inp_option(model_id, file_path, srid, main_dic):
    swmm_model_info = SwmmModelInfo()
    swmm_model_info.model_id = model_id
    swmm_model_info.srid = srid
    swmm_model_info.inp_path = file_path
    option = main_dic['[OPTIONS]']
    for line in option:
        if line.startswith('FLOW_UNITS'):
            swmm_model_info.flow_unit = re.split(r'\s+', line)[1]
        if line.startswith('INFILTRATION'):
            swmm_model_info.infiltration_model = re.split(r'\s+', line)[1]
        if line.startswith('FLOW_ROUTING'):
            swmm_model_info.routing_model = re.split(r'\s+', line)[1]
        if line.startswith('MIN_SLOPE'):
            min_slope = re.split(r'\s+', line)[1]
            swmm_model_info.mini_conduit_slope = float(min_slope)
        if line.startswith('ALLOW_PONDING'):
            swmm_model_info.allow_ponding = re.split(r'\s+', line)[1]
            break
    return swmm_model_info


# 解析模型雨量站数据
def parse_swmm_gages(model_id, srid, main_dic):
    # 解析空间数据
    symbols = main_dic['[SYMBOLS]']
    x_dic = {}
    y_dic = {}
    i = 3
    while i < len(symbols):
        if symbols[i] == '\n':
            break
        else:
            item = re.split(r'\s+', symbols[i])
            x_dic[item[0]] = float(item[1])
            y_dic[item[0]] = float(item[2])
        i += 1
    # 解析属性数据

    swmm_gages = main_dic['[RAINGAGES]']
    gages_list = []
    i = 3
    while i < len(swmm_gages):
        if swmm_gages[i] == '\n':
            break
        else:
            gages_item = SwmmRainGages()
            item = re.split(r'\s+', swmm_gages[i])
            gages_item.model_id = model_id
            gages_item.rain_id = item[0]
            gages_item.x_coordinate = x_dic[item[0]]
            gages_item.y_coordinate = y_dic[item[0]]
            gages_item.rain_format = item[1]
            gages_item.time_interval = item[2]
            gages_item.snow_catch_factor = float(item[3])
            gages_item.data_source = item[5]
            gages_item.geom = ("st_transform(st_pointfromtext('POINT(%f %f)',%d),4490)"
                               % (x_dic[item[0]], y_dic[item[0]], int(srid)))
            gages_list.append(gages_item)
        i += 1
    return gages_list


# 解析子汇水分区数据
def parse_swmm_catchment(model_id, srid, main_dic):
    # 解析空间数据
    polygons = main_dic['[Polygons]']
    wkt_dic = {}
    i = 3
    temp_code = ''
    while i < len(polygons):
        if polygons[i] == '\n':
            break
        else:
            item = re.split(r'\s+', polygons[i])
            code = item[0]
            cords = item[1] + ' ' + item[2]
            if code != temp_code:
                temp_code = code
                wkt_dic[code] = cords
            else:
                wkt_dic[code] = wkt_dic[code] + ',' + cords
        i += 1
    for code in wkt_dic:
        wkt = wkt_dic[code]
        fp = wkt.split(',')[0]
        wkt_dic[code] = 'POLYGON((' + wkt_dic[code] + ',' + fp + '))'

    # 解析属性数据

    if '[SUBCATCHMENTS]' not in main_dic:
        print("SUBCATCHMENTS不存在")
        return []
    swmm_catchment = main_dic['[SUBCATCHMENTS]']
    print(f"swmm_catchment维度: {len(swmm_catchment)}行数据")  # 打印总行数
    print("前100行数据预览:")
    for idx, line in enumerate(swmm_catchment[:100]):  # 打印前100行
        print(f"{idx}: {line.strip()}")
    print("\n后100行数据预览:")
    for idx, line in enumerate(swmm_catchment[-100:], start=len(swmm_catchment) - 100):  # 打印后100行
        print(f"{idx}: {line.strip()}")
    catchment_list = []
    i = 4
    while i < len(swmm_catchment):

        if swmm_catchment[i] == '\n':
            break
        else:
            catchment_item = SwmmSubCatchment()
            item = re.split(r'\s+', swmm_catchment[i])
            if len(item) < 7:  # 检查是否有足够的字段
                print(f"第{i}行数据缺少字段: {swmm_catchment[i].strip()}")
                i += 1
                continue
            catchment_item.model_id = model_id
            catchment_item.catchment_id = item[0]
            catchment_item.rain_id = item[1]
            catchment_item.node_id = item[2]
            catchment_item.area = float(item[3])
            catchment_item.imperv = float(item[4])
            catchment_item.width = float(item[5])
            catchment_item.slope = float(item[6])
            catchment_item.geom = ("st_transform(st_geometryfromtext('%s',%d),4490)" % (wkt_dic[item[0]], int(srid)))
            catchment_list.append(catchment_item)
        i += 1
    return catchment_list


# 解析节点数据
def parse_swmm_node(model_id, srid, main_dic):
    # 解析空间数据
    coordinates = main_dic['[COORDINATES]']
    wkt_dic = {}
    i = 3
    while i < len(coordinates):
        if coordinates[i] == '\n':
            break
        else:
            item = re.split(r'\s+', coordinates[i])
            code = item[0]
            cords = item[1] + ' ' + item[2]
            wkt_dic[code] = 'POINT(' + cords + ')'
        i += 1

    # 解析节点属性数据
    if '[JUNCTIONS]' not in main_dic:
        print("JUNCTIONS不存在")
        return [], []
    junctions = main_dic['[JUNCTIONS]']
    print(f"junctions维度: {len(junctions)}行数据")  # 打印总行数
    print("前100行数据预览:")
    for idx, line in enumerate(junctions[:100]):  # 打印前100行
        print(f"{idx}: {line.strip()}")
    junctions_list = []
    i = 3
    while i < len(junctions):
        if junctions[i] == '\n':
            break
        else:
            try:
                junctions_item = SwmmNodeJunction()
                item = junctions[i].split(' ')
                item = list(filter(None, item))
                if len(item) < 6:  # 检查字段数量
                    logging.warning(f"第{i}行节点数据不完整: {junctions[i].strip()}")
                    i += 1
                    continue

                junctions_item.model_id = model_id
                junctions_item.node_id = item[0]
                junctions_item.elevation = float(item[1])
                junctions_item.max_depth = float(item[2])
                junctions_item.init_depth = float(item[3])
                junctions_item.sur_depth = float(item[4])
                junctions_item.ponded_area = float(item[5])

                if item[0] not in wkt_dic:
                    logging.warning(f"节点{item[0]}缺少坐标数据")
                    i += 1
                    continue

                junctions_item.geom = (
                        "st_transform(st_geometryfromtext('%s',%d),4490)" % (wkt_dic[item[0]], int(srid)))
                junctions_list.append(junctions_item)
            except Exception as e:
                logging.error(f"处理第{i}行节点数据时出错: {str(e)}")
        i += 1

    # 解析排水口属性数据
    outfalls = main_dic['[OUTFALLS]']
    outfall_list = []
    i = 3
    while i < len(outfalls):
        if outfalls[i] == '\n':
            break
        else:
            try:
                outfall_item = SwmmNodeOutfall()
                item = re.split(r'\s+', outfalls[i])
                if len(item) < 3:  # 检查字段数量
                    logging.warning(f"第{i}行排水口数据不完整: {outfalls[i].strip()}")
                    i += 1
                    continue

                outfall_item.model_id = model_id
                outfall_item.node_id = item[0]
                outfall_item.elevation = float(item[1])
                outfall_item.type = item[2]

                if item[0] not in wkt_dic:
                    logging.warning(f"排水口{item[0]}缺少坐标数据")
                    i += 1
                    continue

                outfall_item.geom = ("st_transform(st_geometryfromtext('%s',%d),4490)" % (wkt_dic[item[0]], int(srid)))
                outfall_list.append(outfall_item)
            except Exception as e:
                logging.error(f"处理第{i}行排水口数据时出错: {str(e)}")
        i += 1
    return junctions_list, outfall_list

def parse_swmm_link_xsections(model_id, srid, main_dic):

    # 解析管道属性数据
    xsection_list = []
    i = 3
    xsection = main_dic['[XSECTIONS]']
    while i < len(xsection):
        if xsection[i] == '\n':
            break
        else:
            xsection_item = SwmmLinkXsections()
            item = re.split(r'\s+', xsection[i])
            xsection_item.model_id = model_id
            xsection_item.link_id = item[0]
            xsection_item.shape = item[1]
            xsection_item.geom1 = item[2]
            xsection_item.geom2 = float(item[3])
            xsection_item.geom3 = float(item[4])
            xsection_item.geom4 = float(item[5])
            xsection_item.barrels = float(item[6])
            xsection_item.culvert = item[7]
            xsection_list.append(xsection_item)
            print(xsection_list[-1].geom2)
        i += 1

    return xsection_list
# 解析模型管线数据
def parse_swmm_conduit(model_id, srid, main_dic):
    # 解析管道空间数据
    vertices = main_dic['[VERTICES]']
    wkt_dic = {}
    i = 3
    temp_code = ''
    while i < len(vertices):
        if vertices[i] == '\n':
            break
        else:
            item = re.split(r'\s+', vertices[i])
            code = item[0]
            cords = item[1] + ' ' + item[2]
            if code != temp_code:
                temp_code = code
                wkt_dic[code] = cords
            else:
                wkt_dic[code] = wkt_dic[code] + ',' + cords
        i += 1

    # 获取节点数据
    coord_dic = query_node_coordinates(main_dic)

    # 解析管道属性数据
    conduit_list = []
    i = 3
    conduit = main_dic['[CONDUITS]']
    while i < len(conduit):
        if conduit[i] == '\n':
            break
        else:
            conduit_item = SwmmLinkConduit()
            item = re.split(r'\s+', conduit[i])
            conduit_item.model_id = model_id
            conduit_item.link_id = item[0]
            conduit_item.from_node = item[1]
            conduit_item.to_node = item[2]
            conduit_item.length = float(item[3])
            conduit_item.roughness = float(item[4])
            conduit_item.in_offset = float(item[5])
            conduit_item.out_offset = float(item[6])
            conduit_item.init_flow = float(item[7])
            conduit_item.max_flow = float(item[8])
            from_node = coord_dic[item[1]]
            to_node = coord_dic[item[2]]
            if item[0] in wkt_dic:
                line = 'LINESTRING(' + from_node + ',' + wkt_dic[item[0]] + ',' + to_node + ')'
            else:
                line = 'LINESTRING(' + from_node + ',' + to_node + ')'
            conduit_item.geom = ("st_transform(st_geometryfromtext('%s',%d),4490)" % (line, int(srid)))
            conduit_list.append(conduit_item)
        i += 1
    return conduit_list
# 查询模型的所有node数据
def query_node_coordinates(main_dic):
    coordinates = main_dic['[COORDINATES]']
    coord_dic = {}
    i = 3
    while i < len(coordinates):
        if coordinates[i] == '\n':
            break
        else:
            item = re.split(r'\s+', coordinates[i])
            code = item[0]
            coord_dic[code] = item[1] + ' ' + item[2]
        i += 1
    return coord_dic