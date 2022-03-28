import numpy as np
import configparser
import time
import re


# import sys


class Server(object):
    def __init__(self, key, uplimit=0):
        self.id = key
        self.connectedTo = {}
        self.sumConnected = 0
        self.uplimit = uplimit

    def addNeighbor(self, nbr, weight=0):
        self.connectedTo[nbr] = weight
        self.sumConnected = self.sumConnected + weight

    def addAllocate(self, nbr, weight=0):
        self.connectedTo[nbr], self.sumConnected = weight, self.sumConnected - self.connectedTo[nbr] + weight

    def getConnections(self):
        return self.connectedTo.keys()  # .keys (因为是一个字典)

    def getId(self):
        return self.id

    def getSum(self):
        return self.sumConnected

    def getWeight(self, nbr):
        return self.connectedTo[nbr]

    def freshUplimit(self, uplimit):
        self.uplimit = uplimit

    def freshWeight(self):
        for key, _ in self.connectedTo.items():
            self.connectedTo[key] = 0

    def connect2Txt(self):
        # return ','.join ['<%s,%d>' % (key,self.connectedTo[key]) for key,_ in self.connectedTo.items()]
        strList = []
        for key, _ in self.connectedTo.items():
            if self.connectedTo[key] > 0:
                strList.append('<%s,%d>' % (key, self.connectedTo[key]))
        return ','.join(strList)

    def __str__(self):
        return str(self.id)


# 构建伪链式结构
class Linesd(object):
    def __init__(self, NUMI, NUMJ, demand_name, qos_id, qos_pos_list, demand, bandwidth):
        self.NUMI = NUMI
        self.NUMJ = NUMJ
        self.demand_name = demand_name
        self.qos_id = qos_id
        self.qos_pos_list = qos_pos_list
        self.demand = demand
        self.bandwidth = bandwidth
        self.Demand_Dict = {}
        self.Server_Dict = {}
        self.makeLine()
        self.makeNet()
        self.makeNote()
        self.allocate_schedule = {}

    # 创建基本节点：后期可能发生变化
    def makeLine(self):
        for i in range(self.NUMI):
            self.Demand_Dict[self.demand_name[i]] = Server(self.demand_name[i])
        for j in range(self.NUMJ):
            self.Server_Dict[qos_id[j]] = Server(self.qos_id[j], self.bandwidth[j])

    # 创建节点间关系图:后期不会发生变化
    def makeNet(self):
        for i in range(self.NUMI):
            qos_pos = self.qos_pos_list[i]
            for j in qos_pos:
                self.Demand_Dict[self.demand_name[i]].addNeighbor(self.Server_Dict[qos_id[j]])
                self.Server_Dict[self.qos_id[j]].addNeighbor(self.Demand_Dict[self.demand_name[i]])

    def freshDemand(self, t):
        for i in range(self.NUMI):
            self.Demand_Dict[self.demand_name[i]].freshUplimit(demand[t, i])

    def freshWeight(self):
        for i in range(self.NUMI):
            self.Demand_Dict[self.demand_name[i]].freshWeight()

    # 创建图提示
    def makeNote(self):
        print('-----------------图结构构建结束-----------------------')

    def getDemandDict(self):
        return self.Demand_Dict

    def getServerDict(self):
        return self.Server_Dict

    def allocateI_avg(self, t, i):
        self.freshDemand(t)
        self.freshWeight()
        demand_i = self.Demand_Dict[self.demand_name[i]].uplimit
        # print(type(self.Demand_Dict[self.demand_name[i]].connectedTo))
        num_available = len(self.Demand_Dict[self.demand_name[i]].connectedTo)
        avg_allocate = int(demand_i / num_available)
        gap = demand_i - avg_allocate * num_available
        for Server, weight in self.Demand_Dict[self.demand_name[i]].connectedTo.items():
            # print('分配allocate')
            id = Server.getId()
            # print(id)
            # print(self.Server_Dict[id].sumConnected)
            if self.Server_Dict[id].sumConnected + avg_allocate <= self.Server_Dict[id].uplimit:
                # 对偶写法
                new_weight = avg_allocate + self.Demand_Dict[self.demand_name[i]].getWeight(self.Server_Dict[id])
                self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)

        for Server, weight in self.Demand_Dict[self.demand_name[i]].connectedTo.items():
            # print('分配gap')
            id = Server.getId()
            # print(id)
            if self.Server_Dict[id].sumConnected + gap <= self.Server_Dict[id].uplimit:
                # 对偶写法
                new_weight = gap + self.Demand_Dict[self.demand_name[i]].getWeight(self.Server_Dict[id])
                self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)
                gap = gap - gap
            else:
                margin = self.Server_Dict[id].uplimit - self.Server_Dict[id].sumConnected
                # 对偶写法
                new_weight = margin + self.Demand_Dict[self.demand_name[i]].getWeight(self.Server_Dict[id])
                self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)
                gap = gap - margin
            if gap == 0:
                break
        return self.Demand_Dict[self.demand_name[i]]

    def allocateI_prior(self, t, i):
        self.freshDemand(t)
        demand_i = self.Demand_Dict[self.demand_name[i]].uplimit - self.Demand_Dict[self.demand_name[i]].sumConnected
        # num_available = len(self.Demand_Dict[self.demand_name[i]].connectedTo)  # 暂时不考虑用户无可用节点情形
        for Server, weight in self.Demand_Dict[self.demand_name[i]].connectedTo.items():
            id = Server.getId()
            # 整个支路能吃下
            if 0 <= self.Server_Dict[id].sumConnected + demand_i <= self.Server_Dict[id].uplimit:
                # 当前节点够减（直接清空需求）
                if self.Server_Dict[id].connectedTo[self.Demand_Dict[self.demand_name[i]]] + demand_i>=0:
                    # 对偶写法
                    new_weight = demand_i + self.Demand_Dict[self.demand_name[i]].getWeight(self.Server_Dict[id])
                    self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                    self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)
                    demand_i = demand_i - demand_i
                # 当前节点不够减（有多少先减了）
                else:
                    # 对偶写法
                    margin = self.Server_Dict[id].connectedTo[self.Demand_Dict[self.demand_name[i]]]
                    new_weight = 0
                    self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                    self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)
                    demand_i = demand_i + margin
            # 整个支路不够加
            elif self.Server_Dict[id].sumConnected + demand_i > self.Server_Dict[id].uplimit:
                # 当前节点有多少先加了
                margin = self.Server_Dict[id].uplimit - self.Server_Dict[id].sumConnected
                # 对偶写法
                new_weight = margin + self.Demand_Dict[self.demand_name[i]].getWeight(self.Server_Dict[id])
                self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)
                demand_i = demand_i - margin
            # 整个支路不够减
            else:
                # 当前节点有多少先减了
                margin = self.Server_Dict[id].connectedTo[self.Demand_Dict[self.demand_name[i]]]
                # 对偶写法
                new_weight = 0
                self.Demand_Dict[self.demand_name[i]].addAllocate(self.Server_Dict[id], new_weight)
                self.Server_Dict[id].addAllocate(self.Demand_Dict[self.demand_name[i]], new_weight)
                demand_i = demand_i + margin

            if demand_i == 0:
                break
        return self.Demand_Dict[self.demand_name[i]]


# 文件读取
def read_csv(path):
    data = np.loadtxt(open(path, "rb"), delimiter=",", skiprows=0, dtype=np.str_,
                      encoding='utf-8')
    my_name = data[0, 1:]
    my_id = data[1:, 0]
    my_matrix = data[1:, 1:]
    my_matrix = my_matrix.astype(int)
    return my_name, my_id, my_matrix


# 根据qos置换到0-1
def open_off(data, THRE):
    # 这边阈值化反逻辑：注意
    temp1 = np.where(data < THRE, data, 0)
    temp2 = np.where(data >= THRE, temp1, 1)
    return temp2


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


def Dict_txt(dict):
    # print(dict.connect2Txt())
    # print(dict.getId()+':'+dict.connect2Txt()+'\n')
    return dict.getId() + ':' + dict.connect2Txt() + '\n'


def Out2txt(outpath, str):
    f = open(outpath, "w", newline='', encoding='utf-8')
    f.write(str)
    f.close()


if __name__ == '__main__':
    # 变量管理
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
    temp1 = time.time()
    print('读取文件时间:%s' % (temp1 - start_time))
    # 刷新一下可能的命名错误
    demand_name = Handle_rn(demand_name)
    qos_name = Handle_rn(qos_name)
    bandwidth_id = Handle_rn(bandwidth_id)
    temp2 = time.time()
    print('修正命名错误:%s' % (temp2 - temp1))

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
    temp3 = time.time()
    print('节点排序:%s' % (temp3 - temp2))
    # 调换节点，优先满足qos可用节点少的用户
    qos_01 = qos_01[:, prior_index]
    qos_name = qos_name[prior_index]

    # 增加带宽排序
    prior_band_index = np.argsort(-bandwidth.flatten())
    # 带宽排序：按照顺序调换优先级
    bandwidth = bandwidth[prior_band_index, :]
    bandwidth_id = bandwidth_id[prior_band_index]
    temp4 = time.time()
    print('带宽排序:%s' % (temp4 - temp3))

    # 用户名：以qos_name为基准，匹配demand_name
    name_index = ID_match(waitarr=demand_name, alreadyarr=qos_name)
    demand = demand[:, name_index]
    demand_name = demand_name[name_index]
    # 节点名：以bandwidth_id为基准，匹配qos_id
    id_index = ID_match(waitarr=qos_id, alreadyarr=bandwidth_id)
    qos_01 = qos_01[id_index, :]
    qos_id = qos_id[id_index]
    temp5 = time.time()
    print('节点匹配:%s' % (temp5 - temp4))

    # 为了减少循环次数，直接记录qos_01可用位置
    qos_pos_list = []
    for i in range(NUMI):
        qos_pos = np.where(qos_01[:, i] > 0)[0]
        qos_pos_list.append(qos_pos)
    temp6 = time.time()
    print('记录qos可用点时间:%s' % (temp6 - temp5))

    # 时间排序:按照顺序调换处理顺序
    demand_id_Indexed = np.argsort(np.array(Handle_time(demand_id)))  # 升序排列
    demand = demand[demand_id_Indexed, :]
    demand_id = demand_id[demand_id_Indexed]
    temp7 = time.time()
    print('时间排序时间:%s' % (temp7 - temp6))
    print("全部准备工作时间:%s" % (time.time() - start_time))

    print('-----------------图结构构建开始-----------------------')
    basic_net = Linesd(NUMI, NUMJ, demand_name, qos_id, qos_pos_list, demand, bandwidth)
    demand_dict = basic_net.getDemandDict()
    server_dict = basic_net.getServerDict()

    # 测试基本结构设计是否正确
    # for key, val in demand_dict.items():
    #     print(key)
    #     print(val)
    # demand_dict['L'].addNeighbor(server_dict['Dw'], weight=10)
    # demand_dict['L'].addAllocate(server_dict['Dw'], weight=100)
    # server_dict['Dw'].addNeighbor(demand_dict['L'], weight=10)
    # print(demand_dict['L'].getSum())
    # print(server_dict['Dw'].getSum())
    # # Server.connectedTo
    # print(len(server_dict['Dw'].connectedTo))

    # 开始分配工作
    print('-----------------正式分配工作开始-----------------------')
    # for t in range(NUMT):
    #     AllocatebyTime(i, NUMT, NUMJ, NUMI, bandwidth, demand, qos_01, res_all, qos_pos_list, memos, init_flag)
    # temp9 = time.time()
    # for id in demand_dict['L'].getConnections:
    #     print(id)
    # print(demand_dict['L'].getConnections)
    temp3_1 = time.time()
    Out_str = []
    for t in range(NUMT):
        for i in range(NUMI):
            Res = basic_net.allocateI_prior(t, i)
            Out_str.append(Dict_txt(Res))
            # print(t, i)
    temp3_2 = time.time()
    print('分配时间：%s' % (temp3_2 - temp3_1))
    Out2txt(outpath, ''.join(Out_str))
    temp3_3 = time.time()
    print('写入时间:%s' % (temp3_3 - temp3_2))
