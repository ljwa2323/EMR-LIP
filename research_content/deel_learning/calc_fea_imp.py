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

from utils import calculate_integrated_gradients


# -------  环境变量 -----------
model_name = "lstm"
end_point = 12
y_name = "hospital_expire_flag"
y_index = 0
n_samples = 500
device = torch.device("cuda:1")
file_para = f"/home/luojiawei/EMR_LIP/deep_learning/saved_param_share/{model_name}_mimic_{y_name}.pth"
file_out = f'/home/luojiawei/EMR_LIP/结果文件夹/fea_imp_folder/fea_imp_{model_name}_mimic_{y_name}.npy'

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
                stat_path = "/home/luojiawei/EMR_LIP/stat_info_mimic_iv"
                )

fea_imp = []

random.seed(42)
for i in tqdm(random.sample(range(dataset_te.len()), n_samples), desc="处理进度"): 

    x, mask, _, y_static, t_list = dataset_te.get_1data(i, normalize=True)

    t_mask = torch.where(t_list <= end_point)[0]  # 使用 where 获取索引号
    if len(t_mask) == 0:  # 如果 t_mask 的长度为0，即没有满足的，那么就 continue
        continue
    x = x[t_mask].unsqueeze(0).to(device)  # 筛选并添加批次维度
    m = mask[t_mask].unsqueeze(0).to(device)  # 筛选并添加批次维度
    x1 = torch.cat([x, m], dim=-1)
    x1 = x1.to(device)

    grads = calculate_integrated_gradients([x1], model, y_index, steps=50, device=device)
    grads[0] = grads[0].sum((0,1))
    fea_imp.append(grads[0])


fea_imp = np.stack(fea_imp, axis=0)
np.save(file_out, fea_imp)
print(f"特征重要性文件已保存至: {file_out}")
