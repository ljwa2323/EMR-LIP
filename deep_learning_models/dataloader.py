import os
import pandas as pd
import numpy as np
import torch
import torch.nn as nn

class MIMIC4(object):
    """
    这个类是用来处理MIMIC4数据集的. 

    __init__函数是初始化函数,它会读取指定的csv文件,并根据cls参数来筛选数据. 
    如果cls不为None,那么它会选择cls列等于cls的数据; 否则,它会选择所有的数据. 

    len函数返回所有数据的数量. 

    get_1data函数是用来获取指定索引的数据. 
    它会读取指定文件夹下的各种csv文件,并将它们转换为torch tensor. 

    get_batch_data函数是用来获取一批数据. 
    它会遍历指定的索引列表,对每个索引调用get_1data函数,然后将结果添加到批次列表中. 

    iterate_batch函数是用来迭代批次数据的. 
    它首先根据shuffle参数来决定是否打乱数据的顺序,然后根据size参数来决定每个批次的大小,
    最后它会依次返回每个批次的数据. 
    """
    def __init__(self, root, id_file, id_col=None, cls_col=None, cls=None, keep=True):

        super().__init__()
        assert id_col is not None, "id_col 不能为 None"
        self.root = root
        if cls is not None:
            ds = pd.read_csv(id_file, header=0)
            self.all_pid = ds.loc[np.where(ds.iloc[:, cls_col] == cls)[0], id_col].tolist()
            print("all_pid 的数量:", len(self.all_pid))
        else:
            ds = pd.read_csv(id_file, header=0)
            self.all_pid = ds.loc[:, id_col].tolist()
            print("all_pid 的数量:", len(self.all_pid))
        self.keep = keep

    def len(self):
        return len(self.all_pid)

    def get_1data(self, ind):

        folder_path = os.path.join(self.root, str(self.all_pid[ind]))

        ds_static = pd.read_csv(os.path.join(folder_path, "static.csv"), header=0)

        ds_lab_x = pd.read_csv(os.path.join(folder_path, "lab_x.csv"), header=0)
        ds_lab_m = pd.read_csv(os.path.join(folder_path, "lab_m.csv"), header=0)
        ds_lab_dt = pd.read_csv(os.path.join(folder_path, "lab_dt.csv"), header=0)

        ds_vit_x = pd.read_csv(os.path.join(folder_path, "vital_x.csv"), header=0)
        ds_vit_m = pd.read_csv(os.path.join(folder_path, "vital_m.csv"), header=0)
        ds_vit_dt = pd.read_csv(os.path.join(folder_path, "vital_dt.csv"), header=0)

        ds_trt_x = pd.read_csv(os.path.join(folder_path, "trt_x.csv"), header=0)
        ds_trt_m = pd.read_csv(os.path.join(folder_path, "trt_m.csv"), header=0)
        ds_trt_dt = pd.read_csv(os.path.join(folder_path, "trt_dt.csv"), header=0)

        ds_y = pd.read_csv(os.path.join(folder_path, "y.csv"), header=0)

        x_static = torch.tensor(ds_static.values).float()
        
        t_lab = torch.tensor(ds_lab_x.iloc[:,0:1].values).float()
        x_lab =  torch.tensor(ds_lab_x.iloc[:,1:].values).float()
        m_lab =  torch.tensor(ds_lab_m.iloc[:,1:].values).float()
        dt_lab =  torch.tensor(ds_lab_dt.iloc[:,1:].values).float()
        t_lab_diff = ds_lab_x.iloc[:,0:1].diff()
        t_lab_diff = t_lab_diff.fillna(0)
        dt_lab1 = torch.tensor(t_lab_diff.values).float()

        t_vit = torch.tensor(ds_vit_x.iloc[:,0:1].values).float()
        x_vit =  torch.tensor(ds_vit_x.iloc[:,1:].values).float()
        m_vit =  torch.tensor(ds_vit_m.iloc[:,1:].values).float()
        dt_vit =  torch.tensor(ds_vit_dt.iloc[:,1:].values).float()
        t_vit_diff = ds_vit_x.iloc[:,0:1].diff()
        t_vit_diff = t_vit_diff.fillna(0)
        dt_vit1 = torch.tensor(t_vit_diff.values).float()

        t_trt = torch.tensor(ds_trt_x.iloc[:,0:1].values).float()
        x_trt =  torch.tensor(ds_trt_x.iloc[:,1:].values).float()
        m_trt =  torch.tensor(ds_trt_m.iloc[:,1:].values).float()
        dt_trt =  torch.tensor(ds_trt_dt.iloc[:,1:].values).float()
        t_trt_diff = ds_trt_x.iloc[:,0:1].diff()
        t_trt_diff = t_trt_diff.fillna(0)
        dt_trt1 = torch.tensor(t_trt_diff.values).float()

        y = torch.tensor(ds_y.values).float()

        return x_lab, x_vit, x_trt, x_static, \
                m_lab, m_vit, m_trt, \
                dt_lab, dt_vit, dt_trt, \
                dt_lab1, dt_vit1, dt_trt1, \
                t_lab, t_vit, t_trt, \
                y

    def get_batch_data(self, inds):

        batches = []
        ids1 = []
        for i in range(len(inds)):
            data = self.get_1data(inds[i])
            if data is None:
                continue
            else:
                batches.append(data)
                ids1.append(inds[i])
        return batches, ids1

    def iterate_batch(self, size, shuffle=True):

        if size > self.len():
            raise ValueError("batch size 大于了总样本数")

        if shuffle:
            all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
        else:
            all_ids = list(range(self.len()))

        if self.len() % size == 0:
            n = self.len() // size
        else:
            n = self.len() // size + 1

        for i in range(n):
            if i == n:
                yield self.get_batch_data(all_ids[(size * i):])
            else:
                yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))])
        return


class MIMIC4_1(object):
    """
    这个类是用来处理MIMIC4数据集的. 

    __init__函数是初始化函数,它会读取指定的csv文件,并根据cls参数来筛选数据. 
    如果cls不为None,那么它会选择cls列等于cls的数据; 否则,它会选择所有的数据. 

    len函数返回所有数据的数量. 

    get_1data函数是用来获取指定索引的数据. 
    它会读取指定文件夹下的各种csv文件,并将它们转换为torch tensor. 

    get_batch_data函数是用来获取一批数据. 
    它会遍历指定的索引列表,对每个索引调用get_1data函数,然后将结果添加到批次列表中. 

    iterate_batch函数是用来迭代批次数据的. 
    它首先根据shuffle参数来决定是否打乱数据的顺序,然后根据size参数来决定每个批次的大小,
    最后它会依次返回每个批次的数据. 
    """
    def __init__(self, root, id_file, id_col=None, cls_col=None, cls=None, keep=True):

        super().__init__()
        assert id_col is not None, "id_col 不能为 None"
        self.root = root
        if cls is not None:
            ds = pd.read_csv(id_file, header=0)
            self.all_pid = ds.loc[np.where(ds.iloc[:, cls_col] == cls)[0], id_col].tolist()
            print("all_pid 的数量:", len(self.all_pid))
        else:
            ds = pd.read_csv(id_file, header=0)
            self.all_pid = ds.loc[:, id_col].tolist()
            print("all_pid 的数量:", len(self.all_pid))
        self.keep = keep

    def len(self):
        return len(self.all_pid)

    def get_1data(self, ind):

        folder_path = os.path.join(self.root, str(self.all_pid[ind]))

        ds_static = pd.read_csv(os.path.join(folder_path, "static.csv"), header=0)

        ds_lab_x = pd.read_csv(os.path.join(folder_path, "lab_x.csv"), header=0)
        ds_lab_m = pd.read_csv(os.path.join(folder_path, "lab_m.csv"), header=0)
        ds_lab_dt = pd.read_csv(os.path.join(folder_path, "lab_dt.csv"), header=0)

        ds_vit_x = pd.read_csv(os.path.join(folder_path, "vital_x.csv"), header=0)
        ds_vit_m = pd.read_csv(os.path.join(folder_path, "vital_m.csv"), header=0)
        ds_vit_dt = pd.read_csv(os.path.join(folder_path, "vital_dt.csv"), header=0)

        ds_trt_x = pd.read_csv(os.path.join(folder_path, "trt_x.csv"), header=0)
        ds_trt_m = pd.read_csv(os.path.join(folder_path, "trt_m.csv"), header=0)
        ds_trt_dt = pd.read_csv(os.path.join(folder_path, "trt_dt.csv"), header=0)

        ds_y = pd.read_csv(os.path.join(folder_path, "y.csv"), header=0)

        x_static = torch.tensor(ds_static.values).float()
        
        t_lab = torch.tensor(ds_lab_x.iloc[:,0:1].values).float()
        x_lab =  torch.tensor(ds_lab_x.iloc[:,1:].values).float()
        m_lab =  torch.tensor(ds_lab_m.iloc[:,1:].values).float()
        dt_lab =  torch.tensor(ds_lab_dt.iloc[:,1:].values).float()
        t_lab_diff = ds_lab_x.iloc[:,0:1].diff()
        t_lab_diff = t_lab_diff.fillna(0)
        dt_lab1 = torch.tensor(t_lab_diff.values).float()

        t_vit = torch.tensor(ds_vit_x.iloc[:,0:1].values).float()
        x_vit =  torch.tensor(ds_vit_x.iloc[:,1:].values).float()
        m_vit =  torch.tensor(ds_vit_m.iloc[:,1:].values).float()
        dt_vit =  torch.tensor(ds_vit_dt.iloc[:,1:].values).float()
        t_vit_diff = ds_vit_x.iloc[:,0:1].diff()
        t_vit_diff = t_vit_diff.fillna(0)
        dt_vit1 = torch.tensor(t_vit_diff.values).float()

        t_trt = torch.tensor(ds_trt_x.iloc[:,0:1].values).float()
        x_trt =  torch.tensor(ds_trt_x.iloc[:,1:].values).float()
        m_trt =  torch.tensor(ds_trt_m.iloc[:,1:].values).float()
        dt_trt =  torch.tensor(ds_trt_dt.iloc[:,1:].values).float()
        t_trt_diff = ds_trt_x.iloc[:,0:1].diff()
        t_trt_diff = t_trt_diff.fillna(0)
        dt_trt1 = torch.tensor(t_trt_diff.values).float()

        y = torch.tensor(ds_y.values).float()

        return torch.cat([x_lab, x_vit, x_trt], dim=-1),  \
                torch.cat([m_lab, m_vit, m_trt], dim=-1), \
                torch.cat([dt_lab, dt_vit, dt_trt], dim=-1), \
                dt_lab1, t_lab, x_static, y

    def get_batch_data(self, inds):

        batches = []
        ids1 = []
        for i in range(len(inds)):
            data = self.get_1data(inds[i])
            if data is None:
                continue
            else:
                batches.append(data)
                ids1.append(inds[i])
        return batches, ids1

    def iterate_batch(self, size, shuffle=True):

        if size > self.len():
            raise ValueError("batch size 大于了总样本数")

        if shuffle:
            all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
        else:
            all_ids = list(range(self.len()))

        if self.len() % size == 0:
            n = self.len() // size
        else:
            n = self.len() // size + 1

        for i in range(n):
            if i == n:
                yield self.get_batch_data(all_ids[(size * i):])
            else:
                yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))])
        return

class EICU_1(object):
    """
    这个类是用来处理EICU数据集的. 

    __init__函数是初始化函数,它会读取指定的csv文件,并根据cls参数来筛选数据. 
    如果cls不为None,那么它会选择cls列等于cls的数据; 否则,它会选择所有的数据. 

    len函数返回所有数据的数量. 

    get_1data函数是用来获取指定索引的数据. 
    它会读取指定文件夹下的各种csv文件,并将它们转换为torch tensor. 

    get_batch_data函数是用来获取一批数据. 
    它会遍历指定的索引列表,对每个索引调用get_1data函数,然后将结果添加到批次列表中. 

    iterate_batch函数是用来迭代批次数据的. 
    它首先根据shuffle参数来决定是否打乱数据的顺序,然后根据size参数来决定每个批次的大小,
    最后它会依次返回每个批次的数据. 
    """
    def __init__(self, root, id_file, id_col=None, cls_col=None, cls=None, keep=True):

        super().__init__()
        assert id_col is not None, "id_col 不能为 None"
        self.root = root
        if cls is not None:
            ds = pd.read_csv(id_file, header=0)
            self.all_pid = ds.loc[np.where(ds.iloc[:, cls_col] == cls)[0], id_col].tolist()
            print("all_pid 的数量:", len(self.all_pid))
        else:
            ds = pd.read_csv(id_file, header=0)
            self.all_pid = ds.loc[:, id_col].tolist()
            print("all_pid 的数量:", len(self.all_pid))
        self.keep = keep

    def len(self):
        return len(self.all_pid)

    def get_1data(self, ind):

        folder_path = os.path.join(self.root, str(self.all_pid[ind]))

        ds_static = pd.read_csv(os.path.join(folder_path, "static.csv"), header=0)

        ds_lab_x = pd.read_csv(os.path.join(folder_path, "lab_x.csv"), header=0)
        ds_lab_m = pd.read_csv(os.path.join(folder_path, "lab_m.csv"), header=0)
        ds_lab_dt = pd.read_csv(os.path.join(folder_path, "lab_dt.csv"), header=0)

        ds_vit_x = pd.read_csv(os.path.join(folder_path, "vital_x.csv"), header=0)
        ds_vit_m = pd.read_csv(os.path.join(folder_path, "vital_m.csv"), header=0)
        ds_vit_dt = pd.read_csv(os.path.join(folder_path, "vital_dt.csv"), header=0)

        ds_trt_x = pd.read_csv(os.path.join(folder_path, "trt_x.csv"), header=0)
        ds_trt_m = pd.read_csv(os.path.join(folder_path, "trt_m.csv"), header=0)
        ds_trt_dt = pd.read_csv(os.path.join(folder_path, "trt_dt.csv"), header=0)

        ds_y = pd.read_csv(os.path.join(folder_path, "y.csv"), header=0)

        x_static = torch.tensor(ds_static.values).float()
        
        t_lab = torch.tensor(ds_lab_x.iloc[:,0:1].values).float()
        x_lab =  torch.tensor(ds_lab_x.iloc[:,1:].values).float()
        m_lab =  torch.tensor(ds_lab_m.iloc[:,1:].values).float()
        dt_lab =  torch.tensor(ds_lab_dt.iloc[:,1:].values).float()
        t_lab_diff = ds_lab_x.iloc[:,0:1].diff()
        t_lab_diff = t_lab_diff.fillna(0)
        dt_lab1 = torch.tensor(t_lab_diff.values).float()

        t_vit = torch.tensor(ds_vit_x.iloc[:,0:1].values).float()
        x_vit =  torch.tensor(ds_vit_x.iloc[:,1:].values).float()
        m_vit =  torch.tensor(ds_vit_m.iloc[:,1:].values).float()
        dt_vit =  torch.tensor(ds_vit_dt.iloc[:,1:].values).float()
        t_vit_diff = ds_vit_x.iloc[:,0:1].diff()
        t_vit_diff = t_vit_diff.fillna(0)
        dt_vit1 = torch.tensor(t_vit_diff.values).float()

        t_trt = torch.tensor(ds_trt_x.iloc[:,0:1].values).float()
        x_trt =  torch.tensor(ds_trt_x.iloc[:,1:].values).float()
        m_trt =  torch.tensor(ds_trt_m.iloc[:,1:].values).float()
        dt_trt =  torch.tensor(ds_trt_dt.iloc[:,1:].values).float()
        t_trt_diff = ds_trt_x.iloc[:,0:1].diff()
        t_trt_diff = t_trt_diff.fillna(0)
        dt_trt1 = torch.tensor(t_trt_diff.values).float()

        y = torch.tensor(ds_y.values).float()

        return torch.cat([x_lab, x_vit, x_trt], dim=-1),  \
                torch.cat([m_lab, m_vit, m_trt], dim=-1), \
                torch.cat([dt_lab, dt_vit, dt_trt], dim=-1), \
                dt_lab1, t_lab, x_static, y

    def get_batch_data(self, inds):

        batches = []
        ids1 = []
        for i in range(len(inds)):
            data = self.get_1data(inds[i])
            if data is None:
                continue
            else:
                batches.append(data)
                ids1.append(inds[i])
        return batches, ids1

    def iterate_batch(self, size, shuffle=True):

        if size > self.len():
            raise ValueError("batch size 大于了总样本数")

        if shuffle:
            all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
        else:
            all_ids = list(range(self.len()))

        if self.len() % size == 0:
            n = self.len() // size
        else:
            n = self.len() // size + 1

        for i in range(n):
            if i == n:
                yield self.get_batch_data(all_ids[(size * i):])
            else:
                yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))])
        return


if __name__ == "__main__":
    
    dataset = EICU_1("/home/luojiawei/Benchmark_project_data/eicu_data/patient_folders_1/",
                    "/home/luojiawei/Benchmark_project_data/eicu_data/train_set.csv",
                    id_col="patienthealthsystemstayid",
                    cls_col = 27,
                    cls = 1)
    
    datas = dataset.get_1data(0)

    print("--")
