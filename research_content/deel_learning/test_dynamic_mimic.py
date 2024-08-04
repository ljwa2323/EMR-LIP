import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

from dataloader import MIMIC_IV
from model import *

from tqdm import tqdm
import numpy as np
import json
import random
import pandas as pd

# -------  环境变量 -----------
model_name = "biattlstm"
y_name = "death_24h"
y_index = 1
device = torch.device("cuda:2")
pre_param = None
file_para = f"/home/luojiawei/EMR_LIP/deep_learning/saved_param/{model_name}_mimic_{y_name}_dym.pth"
file_out = f"//home/luojiawei/EMR_LIP/结果文件夹/模型性能/test_{model_name}_mimic_{y_name}_dym.csv"

# -------  模型参数 -----------
input_size_list = [55 + 49]
hidden_size_list = [256]
output_size_list = [1]
type_list = ["cat"]
model_list = ["biattlstm"]

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
model.load_state_dict(torch.load(file_para))
model = model.to(device)

print(f"task: {y_name}")

dataset_te = MIMIC_IV("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids/",
                "/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv",
                id_col="stay_id",
                mod_col="set",
                mod = [2],
                cls_col = None,
                cls = None,
                stat_path = "/home/luojiawei/EMR_LIP/stat_info_mimic_iv"
                )


print("开始测试")


y_true, y_pred, id_list, t_list_1 = [], [], [], []
with torch.no_grad():
    for i in tqdm(range(dataset_te.len())):
        datas = dataset_te.get_1data(i,normalize=True)

        x, mask, y_mat, _, t_list = datas
        x1 = torch.cat([x, mask], dim=-1).to(device)
        t_list = t_list.to(device)
        y_mat = y_mat.to(device)
        for j in range(1, y_mat.shape[0]):

            indices = torch.where(t_list[:,0] <= y_mat[j, 0])[0]
            if len(indices) == 0:
                continue
            x1_ = x1[indices]
            x1_ = x1_.unsqueeze(0)
            yhat_list = model([x1_])
            y_pred.append(yhat_list[0])
            y_true.append(y_mat[j:(j+1),y_index:(y_index+1)])
            t_list_1.append(y_mat[j, 0])
            id_list.append(dataset_te.all_id[i])

    y_pred = torch.cat(y_pred, dim=0)
    if output_size_list[0] == 1:
        y_true = torch.cat(y_true, dim=0)
    else:
        y_true = torch.cat(y_true, dim=0).to(torch.int64).reshape(-1)

# 将模型输出转换为概率
t_list_1 = torch.stack(t_list_1)  # 使用stack而不是cat
t_list_1 = t_list_1.cpu().numpy()
y_pred_prob = y_pred.cpu().numpy()
y_true_np = y_true.cpu().numpy()

# 整理成 DataFrame
df = pd.DataFrame(y_pred_prob, columns=[f'y_pred_prob_{i}' for i in range(y_pred_prob.shape[1])])
df['y_true'] = y_true_np.flatten()
df['time'] = t_list_1
df['stid'] = id_list

# 输出到文件
df.to_csv(file_out, index=False)
print(f"测试完成，结果已保存到 {file_out}")