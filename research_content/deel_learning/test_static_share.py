import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

from dataloader import EICU_CRD, MIMIC_IV
from model import *

from tqdm import tqdm
import numpy as np
import json
import random
import pandas as pd

# -------  环境变量 -----------
model_name = "lstm"
end_point = 12
y_name = "hospitaldischargestatus"
y_index = 5
device = torch.device("cuda:1")
pre_param = None
file_para = f"/home/luojiawei/EMR_LIP/deep_learning/saved_param_share/{model_name}_eicu_{y_name}.pth"
file_out = f"/home/luojiawei/EMR_LIP/结果文件夹/模型性能_share/test_{model_name}_eicu_to_mimic_{y_name}.csv"

# -------  模型参数 -----------
input_size_list = [34 + 33]
hidden_size_list = [256]
output_size_list = [1]
type_list = ["cat"]
model_list = ["lstm"]

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
model.load_state_dict(torch.load(file_para))
model = model.to(device)

print(f"task: {y_name}")

dataset_te = MIMIC_IV("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share/",
                "/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv",
                id_col="stay_id",
                mod_col="set",
                mod = [2],
                cls_col = None,
                cls = None,
                stat_path = "/home/luojiawei/EMR_LIP/stat_info_eicu_crd"
                )


print("开始测试")

running_loss = 0.0
y_true, y_pred, id_list = [], [], []
with torch.no_grad():

    for i in tqdm(range(dataset_te.len())):
        datas = dataset_te.get_1data(i,normalize=True)

        x, mask, _, y_static, t_list = datas
        t_mask = torch.where(t_list <= end_point)[0]  # 使用 where 获取索引号
        if len(t_mask) == 0:  # 如果 t_mask 的长度为0，即没有满足的，那么就 continue
            continue
        x = x[t_mask].unsqueeze(0).to(device)  # 筛选并添加批次维度
        m = mask[t_mask].unsqueeze(0).to(device)  # 筛选并添加批次维度
        x1 = torch.cat([x, m], dim=-1)
        x1 = x1.to(device)
        yhat_list = model([x1])
        y_pred.append(yhat_list[0])
        y_true.append(y_static[:,y_index:(y_index+1)])
        id_list.append(dataset_te.all_id[i])

    y_pred = torch.cat(y_pred, dim=0)
    y_true = torch.cat(y_true, dim=0)
    if output_size_list[0] == 1:
        y_true = (y_true > 0).float().to(device)
    else:
        y_true = y_true.to(torch.int64).reshape(-1).to(device)

# 将模型输出转换为概率
y_pred_prob = y_pred.cpu().numpy()
y_true_np = y_true.cpu().numpy()

# 整理成 DataFrame
df = pd.DataFrame(y_pred_prob, columns=[f'y_pred_prob_{i}' for i in range(y_pred_prob.shape[1])])
df['y_true'] = y_true_np.flatten()
df.insert(0, 'stid', id_list)

# 输出到文件
df.to_csv(file_out, index=False)
print(f"测试完成，结果已保存到 {file_out}")