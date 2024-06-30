import ray
import re

# 初始化 Ray
ray.init()


# 定义一个 Ray 任务来处理文件的每一行
@ray.remote
def find_abbreviations(line):
    # 使用正则表达式匹配缩写，如 pr., dr. 等
    abbreviations = re.findall(r'\b[a-zA-Z]+\.', line)
    return abbreviations


def process_file(file_path):
    results = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # 为每一行文本启动一个 Ray 任务
            result = find_abbreviations.remote(line)
            results.append(result)

    # 收集所有结果
    results = ray.get(results)

    # 打印所有识别的缩写
    for result in results:
        if result:
            print(result)


# 替换为你的文件路径
file_path = 'your_file_path'
process_file(file_path)
