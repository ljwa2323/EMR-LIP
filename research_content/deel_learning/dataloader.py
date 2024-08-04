import os
import pandas as pd
import numpy as np
import torch
import torch.nn as nn


class MIMIC_IV(object):

    def __init__(self, root, id_file, id_col=None, 
                 mod_col=None, mod=None, cls_col=None, cls=None, stat_path=None):

        super().__init__()
        assert id_col is not None, "id_col 不能为 None"
        self.root = root
        ds = pd.read_csv(id_file, header=0)
        if mod is not None:
            ds = ds[ds.loc[:, mod_col].isin(mod)]
        if cls is not None:
            ds = ds[ds.loc[:, cls_col].isin(cls)]
        print("all_id 的数量:", len(ds))

        self.all_id = ds.loc[:, id_col].tolist()
        if stat_path:
            self.stat_x = self.get_stat_info(stat_path)

    def get_stat_info(self, path):
        stat_x = torch.tensor(pd.read_csv(os.path.join(path, "X_stat_share.csv"), header=0).iloc[:,1:].values).float()
        return stat_x

    def normalize(self, x, m, s):
        return (x - m) / s

    def unnormalize(self, x, m, s):
        return (x * s) + m

    def len(self):
        return len(self.all_id)

    def get_1data(self, ind, normalize=False):

        folder_path = os.path.join(self.root, str(self.all_id[ind]))

        ds_x = pd.read_csv(os.path.join(folder_path, "dynamic.csv"), header=0)
        ds_mask = pd.read_csv(os.path.join(folder_path, "mask.csv"), header=0)
        ds_y_mat = pd.read_csv(os.path.join(folder_path, "y_mat.csv"), header=0)
        ds_y_static = pd.read_csv(os.path.join(folder_path, "y_static.csv"), header=0)

        x = torch.tensor(ds_x.iloc[:,1:].values).float()
        mask = torch.tensor(ds_mask.iloc[:,1:].values).float()

        t_list = torch.tensor(ds_x.iloc[:,0:1].values).float()
        y_mat =  torch.tensor(ds_y_mat.values).float()
        y_static =  torch.tensor(ds_y_static.values).float()

        if normalize:
            x = self.normalize(x, self.stat_x[:, 0], self.stat_x[:, 1])

        return x, mask, y_mat, y_static, t_list

    def get_batch_data(self, inds, normalize=False):

        batches = []
        ids1 = []
        for i in range(len(inds)):
            data = self.get_1data(inds[i], normalize)
            if data is None:
                continue
            else:
                batches.append(data)
                ids1.append(inds[i])
        return batches, ids1

    def iterate_batch(self, size, shuffle=True, normalize=False):

        if size > self.len():
            size = self.len()  # 如果batch size大于总样本数，设置为总样本数

        if shuffle:
            all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
        else:
            all_ids = list(range(self.len()))

        if self.len() % size == 0:
            n = self.len() // size
        else:
            n = self.len() // size + 1

        for i in range(n):
            if i == n - 1:  # 修改条件，确保最后一个batch正确处理
                yield self.get_batch_data(all_ids[(size * i):], normalize)
            else:
                yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))], normalize)
        return


class EICU_CRD(object):

    def __init__(self, root, id_file, id_col=None, 
                 mod_col=None, mod=None, cls_col=None, cls=None, stat_path=None):

        super().__init__()
        assert id_col is not None, "id_col 不能为 None"
        self.root = root
        ds = pd.read_csv(id_file, header=0)
        if mod is not None:
            ds = ds[ds.loc[:, mod_col].isin(mod)]
        if cls is not None:
            ds = ds[ds.loc[:, cls_col].isin(cls)]
        print("all_id 的数量:", len(ds))

        self.all_id = ds.loc[:, id_col].tolist()
        if stat_path:
            self.stat_x = self.get_stat_info(stat_path)

    def get_stat_info(self, path):
        stat_x = torch.tensor(pd.read_csv(os.path.join(path, "X_stat_share.csv"), header=0).iloc[:,1:].values).float()
        return stat_x

    def normalize(self, x, m, s):
        return (x - m) / s

    def unnormalize(self, x, m, s):
        return (x * s) + m

    def len(self):
        return len(self.all_id)

    def get_1data(self, ind, normalize=False):

        folder_path = os.path.join(self.root, str(self.all_id[ind]))

        ds_x = pd.read_csv(os.path.join(folder_path, "dynamic.csv"), header=0)
        ds_mask = pd.read_csv(os.path.join(folder_path, "mask.csv"), header=0)
        ds_y_mat = pd.read_csv(os.path.join(folder_path, "y_mat.csv"), header=0)
        ds_y_static = pd.read_csv(os.path.join(folder_path, "y_static.csv"), header=0)

        x = torch.tensor(ds_x.iloc[:,1:].values).float()
        mask = torch.tensor(ds_mask.iloc[:,1:].values).float()

        t_list = torch.tensor(ds_x.iloc[:,0:1].values).float()
        y_mat =  torch.tensor(ds_y_mat.values).float()
        y_static =  torch.tensor(ds_y_static.values).float()

        if normalize:
            x = self.normalize(x, self.stat_x[:, 0], self.stat_x[:, 1])

        return x, mask, y_mat, y_static, t_list

    def get_batch_data(self, inds, normalize=False):

        batches = []
        ids1 = []
        for i in range(len(inds)):
            data = self.get_1data(inds[i], normalize)
            if data is None:
                continue
            else:
                batches.append(data)
                ids1.append(inds[i])
        return batches, ids1

    def iterate_batch(self, size, shuffle=True, normalize=False):

        if size > self.len():
            size = self.len()  # 如果batch size大于总样本数，设置为总样本数

        if shuffle:
            all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
        else:
            all_ids = list(range(self.len()))

        if self.len() % size == 0:
            n = self.len() // size
        else:
            n = self.len() // size + 1

        for i in range(n):
            if i == n - 1:  # 修改条件，确保最后一个batch正确处理
                yield self.get_batch_data(all_ids[(size * i):], normalize)
            else:
                yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))], normalize)
        return

if __name__ == "__main__":
    
    from model import *
    # dataset = MIMIC_IV("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids/",
    #                   "/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv",
    #                   id_col="stay_id",
    #                   mod_col="set",
    #                   mod = [1],
    #                   cls_col = None,
    #                   cls = None,
    #                   stat_path = "/home/luojiawei/EMR_LIP/stat_info_mimic_iv"
    #                   )
    
    dataset = MIMIC_IV("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share/",
                      "/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv",
                      id_col="stay_id",
                      mod_col="set",
                      mod = [1],
                      cls_col = None,
                      cls = None,
                      stat_path = "/home/luojiawei/EMR_LIP/stat_info_mimic_iv"
                      )
    print("--")
    datas = dataset.get_1data(1900, normalize=True)
    