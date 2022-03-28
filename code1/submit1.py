# -*- coding:utf-8 -*-
from multiprocessing import Process, Pipe,Pool,Manager
import threading
import numpy as np
import time
import configparser
import re
import sys

# 串行版本-pypy加速
# 解决问题汇总：
# 1.解决ini读取问题
# 2.解决用户需要不能整除问题
# 3.解决报表问题（不够完善）
# 4.测试开很多进程
# 5.解决qos约束
# 6.增加客户映射
# 7.增加时间排序
# 8.并行调整为串行 回退-> 到3进程
# 9.尝试用\r\n输出 回退-> 到\n
# 10.观察到代码没有合理关闭 添加 f.close()
# 11.观察realines的特性！
# 12.测试字符ID问题->无问题
# 13.增加捕获异常，检测系统能否正常退出
# 14.和节点排序问题 ing
# 15.精细化处理/n
# 16.优化递推逻辑
# 17.增加cost优化

# 文件读取
def read_csv(path):
    data = np.loadtxt(open(path, "rb"), delimiter=",", skiprows=0, dtype=np.str_,
                      encoding='utf-8')
    my_name = data[0, 1:]
    my_id = data[1:, 0]
    my_matrix = data[1:, 1:]
    my_matrix = my_matrix.astype(np.float64)
    # for str in my_name:
    #     print(list(str))
    # for str in my_id:
    #     print(list(str))
    return my_name, my_id, my_matrix


# 根据qos置换到0-1
def open_off(data, THRE):
    # 这边阈值化反逻辑：注意
    temp1 = np.where(data < THRE, data, 0)
    temp2 = np.where(data >= THRE, temp1, 1)
    return temp2


# 遍历寻找可行解
def AllocatebyTime(t, NUMT, NUMJ, NUMI, bandwidth, demand, qos_01, res_all, Recur_Flag= False):
    # 正式处理（按时刻）
    res = np.zeros((NUMJ, NUMI))
    for i in range(NUMI):
        demand_i = demand[t, i]
        try:
            AllocatebyTime_RA(i, NUMJ, NUMI, bandwidth, demand_i, qos_01, res)
        except:
            print('error')
            Recur_Flag = True
    res_all[t] = res
    return Recur_Flag


def AllocatebyTime_RA(i, NUMJ, NUMI, bandwidth, demand_i, qos_01, res):
    num_available = sum(qos_01[:, i])
    # 这边需要做一个保护机制，万一程序无穷大
    if num_available > 0:
        avg_allocate = int(demand_i / num_available)  # 可能导致不完全相等
        gap = demand_i - avg_allocate * num_available
        # #1.处理平均值->缩小问题规模:带宽允许均分就均分，不允许就进入递归
        for j in range(NUMJ):
            if sum(res[j, :]) + avg_allocate * qos_01[j, i] <= bandwidth[j]:
                res[j, i] = res[j, i] + avg_allocate * qos_01[j, i]
            else:
                AllocatebyTime_RA(i, NUMJ, NUMI, bandwidth, avg_allocate, qos_01, res)
        for j in range(NUMJ):
            if qos_01[j, i] > 0:
                if sum(res[j, :]) + gap * qos_01[j, i] <= bandwidth[j]:
                    res[j, i] = res[j, i] + gap * qos_01[j, i]
                else:
                    AllocatebyTime_RA(i, NUMJ, NUMI, bandwidth, gap, qos_01, res)
                break


def Solution2txt(data, demand_name, bandwidth_id, pid):
    (NUMT, NUMJ, NUMI) = data.shape
    txt_list = []
    for t in range(NUMT):
        for i in range(NUMI):
            temptxt = demand_name[i] + ':'
            for j in range(NUMJ):
                if data[t, j, i] > 0:
                    temptxt = temptxt + '<%s,%d>,' % (bandwidth_id[j], data[t, j, i])
            if len(temptxt) > 4:
                temptxt = temptxt[0:-1]
            txt_list.append(temptxt)
    f = open(r'./output/%d.txt' % pid, "w", newline='', encoding='utf-8')
    delimiter = '\n'
    f.write(delimiter.join(txt_list))
    f.close()

def HBtxt(num):
    data_all = []
    for i in range(1, 1 + num):
        # print(i)
        f = open(r'./output/%d.txt' % i, "r", newline='', encoding='utf-8')  # 设置文件对象
        data = f.readlines()  # 直接将文件中按行读到list里，效果与方法2一样
        data[-1] = data[-1] + '\n'
        f.close()  # 关闭文件
        if i == 0:
            data_all = data
        else:
            data_all.extend(data)
    data_all[-1] = data[-1].replace('\n', '')
    f = open(r'./output/solution.txt', "w", newline='', encoding='utf-8')
    f.write(''.join(data_all))
    f.close()

def Solution2txts(data, demand_name, bandwidth_id):
    (NUMT, NUMJ, NUMI) = data.shape
    txt_list = []
    for t in range(NUMT):
        for i in range(NUMI):
            temptxt = demand_name[i] + ':'
            for j in range(NUMJ):
                if data[t, j, i] > 0:
                    temptxt = temptxt + '<%s,%d>,' % (bandwidth_id[j], data[t, j, i])
            if len(temptxt) > 4:
                temptxt = temptxt[0:-1]
            txt_list.append(temptxt)
    f = open(r'./output/solution.txt', "w", newline='', encoding='utf-8')
    delimiter = '\n'
    f.write(delimiter.join(txt_list))
    f.close()

def Solution2txts(data, demand_name, bandwidth_id):
    (NUMT, NUMJ, NUMI) = data.shape
    txt_list = []
    for t in range(NUMT):
        for i in range(NUMI):
            temptxt = demand_name[i] + ':'
            for j in range(NUMJ):
                if data[t, j, i] > 0:
                    temptxt = temptxt + '<%s,%d>,' % (bandwidth_id[j], data[t, j, i])
            if len(temptxt) > 4:
                temptxt = temptxt[0:-1]
            txt_list.append(temptxt)
    f = open(r'./output/solution.txt', "w", newline='', encoding='utf-8')
    delimiter = '\n'
    f.write(delimiter.join(txt_list))
    f.close()


def ID_match(waitarr, alreadyarr):
    name_index = []
    for name in alreadyarr:
        name_index.append(list(waitarr).index(name))
    return name_index


def Handle_rn(data):
    for i in range(len(data)):
        data[i] = data[i].strip().replace('\n', '').replace('\r', '')
    return data


#  处理时间序列
def Handle_time(mystr_list):
    time_list = []
    for mystr in mystr_list:
        temp = re.findall(r"\d+", mystr)
        time = (((float(temp[0]) * 12 + float(temp[1])) * 31 + float(temp[2])) * 24 + float(temp[3])) * 60 + float(
            temp[4])
        time_list.append(time)
    return time_list


def run_proc(NUMT, NUMJ, NUMI, bandwidth, demand, qos_01, pid, demand_name, bandwidth_id, band):
    res_all = np.zeros((NUMT, NUMJ, NUMI))
    """子进程要执行的代码"""
    print('主程序%d开始运行...' % pid)
    (start, end) = band
    print('主程序%d运行中...' % pid)
    for i in range(start, end):
        Recur_Flag = AllocatebyTime(i, NUMT, NUMJ, NUMI, bandwidth, demand, qos_01, res_all)
    print('主程序%d结束...' % pid)
    Solution2txt(res_all[start:end, :, :], demand_name, bandwidth_id, pid)
    return Recur_Flag


if __name__ == '__main__':
    outpath = r'./output/solution.txt'
    configPath = r"./data/config.ini"
    conf = configparser.ConfigParser()
    conf.read(configPath)
    THRE = int(conf.get("config", "qos_constraint"))

    start_time = time.time()

    # 读取数据和预处理
    (demand_name, demand_id, demand) = read_csv(r'./data/demand.csv')  # 100*10
    (qos_name, qos_id, qos) = read_csv(r'./data/qos.csv')  # 100*10
    (_, bandwidth_id, bandwidth) = read_csv(r'./data/site_bandwidth.csv')  # 100*1

    # 刷新一下可能的命名错误
    demand_name = Handle_rn(demand_name)
    qos_name = Handle_rn(qos_name)
    bandwidth_id = Handle_rn(bandwidth_id)

    # 常数区
    (NUMT, NUMI) = demand.shape
    NUMJ = bandwidth_id.shape[0]
    qos_01 = open_off(qos, THRE)

    # 增加节点排序
    prior_list = []
    for i in range(NUMI):
        prior = sum(qos_01[:, i])
        prior_list.append(prior)
    prior_list = np.array(prior_list)
    prior_index = np.argsort(prior_list)

    # 调换节点，优先满足qos可用节点少的用户
    qos_01 = qos_01[:, prior_index]
    qos_name = qos_name[prior_index]

    # 增加带宽排序
    prior_band_index = np.argsort(-bandwidth.flatten())
    # 带宽排序：按照顺序调换优先级
    bandwidth = bandwidth[prior_band_index,:]
    bandwidth_id = bandwidth_id[prior_band_index]

    # 用户名：以qos_name为基准，匹配demand_name
    name_index = ID_match(waitarr=demand_name, alreadyarr=qos_name)
    demand = demand[:, name_index]
    demand_name = demand_name[name_index]

    # 节点名：以bandwidth_id为基准，匹配qos_id
    id_index = ID_match(waitarr=qos_id, alreadyarr=bandwidth_id)
    qos_01 = qos_01[id_index, :]
    qos_id = qos_id[id_index]

    # 时间排序:按照顺序调换处理顺序
    demand_id_Indexed = np.argsort(np.array(Handle_time(demand_id)))  # 升序排列
    demand = demand[demand_id_Indexed, :]
    demand_id = demand_id[demand_id_Indexed]
    # print(demand_id)
    # print(demand)

    # Num_recurs = int(NUMT/100)
    # NumPid = 12
    # # 进程池中从无到有创建Numpid个进程,以后一直是这Numpid个进程在执行任务
    # ProcessPools = Pool(NumPid)
    # Res_Pool = []
    # for i in range(Num_recurs):
    #     # print(i)
    #     # 异步运行，根据进程池中有的进程数，每次最多n个子进程在异步执行
    #     band= (int(NUMT / Num_recurs * i), int(NUMT / Num_recurs * (i+1)))
    #     # print(band)
    #     res=ProcessPools.apply_async(run_proc,args=(NUMT, NUMJ, NUMI, bandwidth, demand, qos_01, (i+1), demand_name, bandwidth_id,band))
    #     Res_Pool.append(res)

    # # 保障和等待？
    # ProcessPools.close()
    # ProcessPools.join()
    # # 获取系统返回数据
    # Recur_Flags = []
    # for i in range(Num_recurs):
    #     Recur_Flag = Res_Pool[i].get()
    #     Recur_Flags.append(Recur_Flag)
    #
    # # 检测系统能否正常推出
    # for Recur_Flag in Recur_Flags:
    #     if Recur_Flag ==True:
    #         print(sys.exit(-1))

    # Solution2txts(res_all_all, demand_name, bandwidth_id)
    # print(Num_recurs)
    # HBtxt(Num_recurs)
    res_all = np.zeros((NUMT, NUMJ, NUMI))
    for i in range(NUMT):
        Recur_Flag = AllocatebyTime(i, NUMT, NUMJ, NUMI, bandwidth, demand, qos_01, res_all)
    Solution2txts(res_all, demand_name, bandwidth_id)

    end_time = time.time()
    print(end_time - start_time)
