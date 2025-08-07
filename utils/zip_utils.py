# author:"lzu_xiaoyu"
# email:"1505465303@qq.com"
# date:2024/3/25 10:06
# description: 压缩文件管理类

import zipfile


def unzip_file(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


