import torch
import torch.nn as nn
import torch.nn.functional as F

from gru_d import *
from t_lstm import *
from mlp import *
from transformer import *
from lstm import *
from gru import *

MODEL_LIST = {"gru_d":GRUD, \
              "t_lstm":TLSTM, \
              "transformer":TransformerModel, \
              "mlp":MLP, \
              "lstm": LSTM, \
              "gru": GRU}

class PredModel(nn.Module):
    def __init__(self, input_size_list, hidden_size_list, output_size_list, model_list, \
                 pred_dim):
        super().__init__()
        self.models = nn.ModuleList()
        for i, model_name in enumerate(model_list):
            model_class = MODEL_LIST[model_name]
            self.models.append(model_class(input_size_list[i], hidden_size_list[i], output_size_list[i]))

        self.input_size_list = input_size_list
        self.output_size_list = output_size_list
        self.hidden_size_list = hidden_size_list
        self.model_list = model_list
        
        self.pred_module = MLP(sum(self.output_size_list), 200, 1)

    def forward(self, x_list, m_list, dt_list, dt_list1, t_list, x_mean_list):

        outputs = []
        for i, model_name in enumerate(self.model_list):
            if model_name == 'gru_d':
                output = self.models[i](x_list[i], m_list[i], dt_list[i], dt_list1[i], x_mean_list[i])
            elif model_name == 't_lstm':
                output = self.models[i](x_list[i], dt_list1[i])
            elif model_name == 'transformer':
                output = self.models[i](x_list[i], m_list[i], t_list[i])
            elif model_name == 'mlp':
                output = self.models[i](x_list[i]).squeeze(1)
            elif model_name == 'lstm':
                output = self.models[i](x_list[i], m_list[i], t_list[i])
            elif model_name == 'gru':
                output = self.models[i](x_list[i], m_list[i], t_list[i])
            outputs.append(output)

        final_output = self.pred_module(torch.cat(outputs, dim=-1))
        return final_output

    def count_parameters(self):
        return sum(p.numel() for p in self.parameters() if p.requires_grad)

if __name__ == "__main__":

    import sys
    sys.path.append("/home/luojiawei/Benchmark_project/deep_learning_models")
    from dataloader import MIMIC4
    from model import *

    from tqdm import tqdm
    import numpy as np

    dataset_pos = MIMIC4("/home/luojiawei/Benchmark_project_data/mimiciv_data/patient_folders/",
                        "/home/luojiawei/Benchmark_project_data/mimiciv_data/train_set.csv",
                        id_col="hadm_id",
                        cls_col = 12,
                        cls = 1)
    
    datas = dataset_pos.get_1data(0)

    x_list = [datas[i].unsqueeze(0) for i in range(0, 4)]
    m_list = [datas[i].unsqueeze(0) for i in range(4, 7)] + [None]
    dt_list = [datas[i].unsqueeze(0) for i in range(7, 10)] + [None]
    dt_list1 = [datas[i].unsqueeze(0) for i in range(10, 13)] + [None]
    t_list = [datas[i].unsqueeze(0) for i in range(13, 16)] + [None]
    x_mean_list = [x.mean(dim=(0, 1)).unsqueeze(0) for x in x_list]
    y = datas[16]

    input_size_list = [76, 15, 12, 3]
    hidden_size_list = [128, 128, 128, 128]
    output_size_list = [76, 15, 12, 3]
    model_list = ["lstm","lstm","lstm","mlp"]
    pred_dim = 1

    model = PredModel(input_size_list, hidden_size_list, output_size_list, model_list, pred_dim)

    yhat = model(x_list, m_list, dt_list, dt_list1,t_list, x_mean_list)

    print("--")