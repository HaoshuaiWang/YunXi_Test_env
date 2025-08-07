# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/18 14:03
# description:数据库读取工具
import os

import psycopg2
from configparser import ConfigParser
from sqlalchemy import create_engine


# 连接Postgres数据库
def create_pg_connect():
    base_path = os.path.dirname(__file__)
    filename = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
    section = 'postgresql'
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
        schema = db.pop('schema', 'public')
    else:
        raise Exception('文件 {1} 中未找到 {0} 配置信息！'.format(section, filename))
    connection = psycopg2.connect(**db)
    with connection.cursor() as cursor:
        cursor.execute(f"SET search_path TO {schema};")
    print("PG数据库连接成功！")
    return connection


# 创建PG引擎
def create_pg_engine():
    base_path = os.path.dirname(__file__)
    filename = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
    section = 'postgresql'
    parser = ConfigParser()
    parser.read(filename, encoding='utf-8')
    db_info = {}
    engine_line = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db_info.update({item[0]: item[1]})
    else:
        raise Exception('文件 {1} 中未找到 {0} 配置信息！'.format(section, filename))
    engine_line = engine_line.format(username=db_info['user'], password=db_info['password'], host=db_info['host'],
                                     port=db_info['port'], database=db_info['database'])
    pg_engine = create_engine(
        engine_line,
        pool_size=20,  # 连接池大小
        max_overflow=30,  # 最大溢出连接数
        pool_timeout=30,  # 连接获取超时时间
        pool_recycle=3600  # 连接回收时间
    )
    return pg_engine


def query_server_host():
    base_path = os.path.dirname(__file__)
    filename = os.path.join(os.path.dirname(os.path.dirname(base_path)), 'config.ini')
    parser = ConfigParser()
    parser.read(filename)
    value = parser.get("urbanFlood-api", "apiHost")
    return value

