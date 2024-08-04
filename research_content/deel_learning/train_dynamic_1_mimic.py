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

from sklearn.metrics import accuracy_score, recall_score, \
                            precision_score, f1_score, roc_auc_score, \
                            average_precision_score

# -------  环境变量 -----------
model_name = "lstm"
y_name = "death_24h"
y_index = 1
device = torch.device("cuda:1")
pre_param = None
file_out = f"/home/luojiawei/EMR_LIP/结果文件夹/模型性能_share/{model_name}_mimic_{y_name}_dym.csv"

# -------  模型参数 -----------
input_size_list = [34 + 33]
hidden_size_list = [256]
output_size_list = [1]
type_list = ["cat"]
model_list = ["lstm"]

loss_fn = nn.BCEWithLogitsLoss(pos_weight = torch.tensor([0.9783 / (1-0.9783)]).to(device))
# loss_fn = nn.CrossEntropyLoss(weight=torch.tensor([0.0655,0.5027,0.4318]).to(device))

print(f"task: {y_name}")

dataset_tr = MIMIC_IV("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share/",
                "/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv",
                id_col="stay_id",
                mod_col="set",
                mod = [1],
                cls_col = None,
                cls = None,
                stat_path = "/home/luojiawei/EMR_LIP/stat_info_mimic_iv"
                )


dataset_va = MIMIC_IV("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share/",
                "/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv",
                id_col="stay_id",
                mod_col="set",
                mod = [3],
                cls_col = None,
                cls = None,
                stat_path = "/home/luojiawei/EMR_LIP/stat_info_mimic_iv"
                )



# -------- 训练参数 ------------
EPOCHS = 100
lr = 0.001
weight_decay = 0.001
batch_size = 300
batch_size_val = 300
# best_loss = float("inf")
best_auc = 0.5
no_improvement_count = 0
max_iter_train = 10
max_iter_val = 10
tol_count = 10


model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
if pre_param is not None:
    model.load_state_dict(torch.load(pre_param, map_location=device))
    print("Loaded pre-trained model parameters from", pre_param)
model = model.to(device)


optimizer = optim.Adam(model.parameters(), 
                       lr=lr, 
                       weight_decay=weight_decay)



print("开始训练...")

for epoch in range(EPOCHS):
    print(f"task: {y_name}")
    running_loss = 0.0
    data_loader = dataset_tr.iterate_batch(batch_size, normalize=True)
    
    counter = 1
    while True:
        # 如果 counter 超过了最大迭代次数，跳出循环，进入下一个 epoch
        if counter > max_iter_train:
            break
        try:
            data_batch, ids = next(data_loader)
        except StopIteration:
            # 当 data_loader 迭代完毕后，重置它
            data_loader = data_loader.iterate_batch(batch_size, normalize=True)
            data_batch, ids = next(data_loader)
            # break

        running_loss = 0.0

        y_true, y_pred = [], []
        for i in tqdm(range(len(data_batch))):

            datas = data_batch[i]
            
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

        y_pred = torch.cat(y_pred, dim=0)
        if output_size_list[0] == 1:
            y_true = torch.cat(y_true, dim=0)
        else:
            y_true = torch.cat(y_true, dim=0).to(torch.int64).reshape(-1)

        loss = loss_fn(y_pred, y_true)
        running_loss += loss.cpu().item()
        
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        print_info = "Epoch: {}, counter: {}, Loss: {:.6f}".format(epoch, counter, running_loss)
        # for i, loss in enumerate(loss_batch):
        #     print_info += ", Task {} Loss: {:.6f}".format(i, loss.cpu().item())
        print(print_info)

        counter += 1

    # 每轮 epoch 完了以后评估测试集表现，若 loss_val 有改善，则保存模型参数
    # 若连续两轮 loss_val 都没有改善，则终止训练
    # 在每轮 epoch 完成后，计算测试集上的 loss
    print("开始在验证集上测试...")
    running_loss = 0.0
    y_true, y_pred = [], []

    with torch.no_grad():
        # 在循环开始前初始化列表
        data_loader = dataset_va.iterate_batch(batch_size_val, normalize=True)
        counter = 1
        while True:
            if counter > max_iter_val:
                break
            try:
                data_batch, ids_val = next(data_loader)
            except StopIteration:
                break
            for i in tqdm(range(len(data_batch))):
                datas = data_batch[i]

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
                    
            counter += 1

        y_pred = torch.cat(y_pred, dim=0)
        if output_size_list[0] == 1:
            y_true = torch.cat(y_true, dim=0)
        else:
            y_true = torch.cat(y_true, dim=0).to(torch.int64).reshape(-1)

        loss = loss_fn(y_pred, y_true)
        running_loss += loss.cpu().item()

        # 将模型输出转换为概率
        y_pred_prob = y_pred.cpu().numpy()
        y_true_np = y_true.cpu().numpy()
        
        try:
            # 计算AUC
            if output_size_list[0] == 1:
                auc = roc_auc_score(y_true_np, y_pred_prob)
            else:
                auc = roc_auc_score(y_true_np, y_pred_prob, average='macro', multi_class='ovr')
            print("Valid loss: {:.6f}, AUC: {:.6f}".format(running_loss, auc))

            # 如果 AUC 有改善，则保存模型参数
            if auc > best_auc:
                best_auc = auc  # 更新最佳AUC
                torch.save(model.state_dict(), f"/home/luojiawei/EMR_LIP/deep_learning/saved_param_share/{model_name}_mimic_{y_name}_dym" + ".pth")
                no_improvement_count = 0
                print("参数已更新")
            else:
                no_improvement_count += 1
        except ValueError as e:
            print("AUC 计算失败: ", e)
            # 保存模型参数，但不更新 best_auc
            torch.save(model.state_dict(), f"/home/luojiawei/EMR_LIP/deep_learning/saved_param_share/{model_name}_mimic_{y_name}_1_dym" + ".pth")
            print("由于 AUC 计算失败，模型参数已保存，但未更新 best_auc")
        
        # 若连续两轮 AUC 都没有改善，则终止训练
        if no_improvement_count == tol_count:
            break

