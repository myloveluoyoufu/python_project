import os
import pymysql
import yaml

def get_yaml_data(yaml_file=None):
    if not yaml_file:
        current_path = os.path.abspath(".")
        yaml_file = os.path.join(current_path, "config.yaml")
    # 打开yaml文件
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    # 将yaml数据转化为字典或列表
    data = yaml.load(file_data, Loader=yaml.FullLoader)
    return data


def get_mysql_connect(mysql_name=None):
    config_data = get_yaml_data()
    if not mysql_name:
        mysql_name = config_data['mysql_connect']
    connection = pymysql.connect(**config_data['mysql'][mysql_name])
    return connection


def close_connect(conn,cursor):
    conn.close()
    cursor.close()
    pass
