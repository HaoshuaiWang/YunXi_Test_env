# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/26 15:35
# description: 解析swmm输出文件


from app.dao.result_swmm_catchment import ResultSwmmCatchment
from app.dao.result_swmm_link import ResultSwmmLink
from app.dao.result_swmm_node import ResultSwmmNode
from app.dao.result_swmm_summary import ResultSwmmSummary
from app.dao.result_swmm_system import ResultSwmmSystem


# 解析swmm统计数据
def parse_summary_result(task_code, out):
    summary_result = ResultSwmmSummary()
    summary_result.task_code = task_code
    summary_result.version = out.version
    summary_result.flow_units = out.units['flow']
    summary_result.pollutant_units = out.units['pollutant']
    summary_result.system_units = out.units['system']
    summary_result.start_time = out.times[0]
    summary_result.end_time = out.times[-1]
    summary_result.links_count = len(out.links)
    summary_result.node_count = len(out.nodes)
    summary_result.pollutants_count = len(out.pollutants)
    summary_result.sub_catchments = len(out.subcatchments)
    return summary_result


# 解析子汇水分区数据
def parse_catchment_result(task_code, out):
    catchment_result_list = []
    for item in out.subcatchments:
        for time in out.times:
            catchment_result = ResultSwmmCatchment()
            data = out.subcatch_result(item, time)
            catchment_result.catchment_id = item
            catchment_result.task_code = task_code
            catchment_result.time = time
            for attribute in data:
                if attribute.name == 'RAINFALL':
                    catchment_result.rainfall = data[attribute]
                elif attribute.name == 'SNOW_DEPTH':
                    catchment_result.snow_depth = data[attribute]
                elif attribute.name == 'EVAP_LOSS':
                    catchment_result.evap_loss = data[attribute]
                elif attribute.name == 'INFIL_LOSS':
                    catchment_result.infil_loss = data[attribute]
                elif attribute.name == 'RUNOFF_RATE':
                    catchment_result.runoff_rate = data[attribute]
                elif attribute.name == 'GW_OUTFLOW_RATE':
                    catchment_result.gw_outflow_rate = data[attribute]
                elif attribute.name == 'GW_TABLE_ELEV':
                    catchment_result.gw_table_elev = data[attribute]
                elif attribute.name == 'SOIL_MOISTURE':
                    catchment_result.soil_moisture = data[attribute]
            catchment_result_list.append(catchment_result)
    return catchment_result_list


# 解析管线数据
def parse_link_result(task_code, out):
    link_result_list = []
    for item in out.links:
        for time in out.times:
            link_result = ResultSwmmLink()
            link_result.link_id = item
            link_result.task_code = task_code
            link_result.time = time
            data = out.link_result(item, time)
            for attribute in data:
                if attribute.name == 'FLOW_RATE':
                    link_result.flow_rate = data[attribute]
                elif attribute.name == 'FLOW_DEPTH':
                    link_result.flow_depth = data[attribute]
                elif attribute.name == 'FLOW_VELOCITY':
                    link_result.flow_velocity = data[attribute]
                elif attribute.name == 'FLOW_VOLUME':
                    link_result.flow_volume = data[attribute]
                elif attribute.name == 'CAPACITY':
                    link_result.capacity = data[attribute]
            link_result_list.append(link_result)
    return link_result_list


# 解析节点数据
def parse_node_result(task_code, out):
    node_result_list = []
    for item in out.nodes:
        for time in out.times:
            node_result = ResultSwmmNode()
            node_result.node_id = item
            node_result.task_code = task_code
            node_result.time = time
            data = out.node_result(item, time)
            for attribute in data:
                if attribute.name == 'INVERT_DEPTH':
                    node_result.invert_depth = data[attribute]
                elif attribute.name == 'HYDRAULIC_HEAD':
                    node_result.hydraulic_head = data[attribute]
                elif attribute.name == 'PONDED_VOLUME':
                    node_result.ponded_volume = data[attribute]
                elif attribute.name == 'LATERAL_INFLOW':
                    node_result.lateral_inflow = data[attribute]
                elif attribute.name == 'TOTAL_INFLOW':
                    node_result.total_inflow = data[attribute]
                elif attribute.name == 'FLOODING_LOSSES':
                    node_result.flooding_losses = data[attribute]
            node_result_list.append(node_result)
    return node_result_list


# 解析系统过程数据
def parse_system_result(task_code, out):
    system_result_list = []
    for time in out.times:
        system_result = ResultSwmmSystem()
        system_result.task_code = task_code
        system_result.time = time
        data = out.system_result(time)
        for attribute in data:
            if attribute.name == 'AIR_TEMP':
                system_result.air_temp = data[attribute]
            elif attribute.name == 'RAINFALL':
                system_result.rainfall = data[attribute]
            elif attribute.name == 'SNOW_DEPTH':
                system_result.snow_depth = data[attribute]
            elif attribute.name == 'EVAP_INFIL_LOSS':
                system_result.evap_infil_loss = data[attribute]
            elif attribute.name == 'RUNOFF_FLOW':
                system_result.runoff_flow = data[attribute]
            elif attribute.name == 'DRY_WEATHER_INFLOW':
                system_result.dry_weather_inflow = data[attribute]
            elif attribute.name == 'GW_INFLOW':
                system_result.gw_inflow = data[attribute]
            elif attribute.name == 'RDII_INFLOW':
                system_result.rdii_inflow = data[attribute]
            elif attribute.name == 'DIRECT_INFLOW':
                system_result.direct_inflow = data[attribute]
            elif attribute.name == 'TOTAL_LATERAL_INFLOW':
                system_result.total_lateral_inflow = data[attribute]
            elif attribute.name == 'FLOOD_LOSSES':
                system_result.flood_losses = data[attribute]
            elif attribute.name == 'OUTFALL_FLOWS':
                system_result.outfall_flows = data[attribute]
            elif attribute.name == 'VOLUME_STORED':
                system_result.volume_stored = data[attribute]
            elif attribute.name == 'EVAP_RATE':
                system_result.evap_rate = data[attribute]
            elif attribute.name == 'PTNL_EVAP_RATE':
                system_result.ptnl_evap_rate = data[attribute]
        system_result_list.append(system_result)
    return system_result_list

if __name__ == "__main__":
    pass