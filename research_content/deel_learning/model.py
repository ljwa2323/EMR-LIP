import torch
import torch.nn as nn
import torch.nn.functional as F

from mlp import *
from lstm import *
from gru import *

MODEL_LIST = {"mlp":MLP, "gru": GRU,
                "bigru":  BiGRU,
                "biattgru" : BiGRUWithAttention,
                "lstm": LSTM,
                "bilstm" : BiLSTM, 
                "biattlstm" : BiLSTMWithAttention}

class PredModel(nn.Module):
    def __init__(self, input_size_list, hidden_size_list, model_list, \
                 output_size_list, type_list):
        super().__init__()
        self.models = nn.ModuleList()
        for i, model_name in enumerate(model_list):
            model_class = MODEL_LIST[model_name]
            self.models.append(model_class(input_size_list[i], hidden_size_list[i], input_size_list[i]))

        self.input_size_list = input_size_list
        self.hidden_size_list = hidden_size_list
        self.model_list = model_list
        self.output_size_list = output_size_list
        self.type_list = type_list
        
        self.pred_module_list = nn.ModuleList()
        for i, output_size in enumerate(output_size_list):
            if type_list[i] == "cat":
                if output_size == 1:
                    self.pred_module_list.append(nn.Sequential(MLP(sum(self.input_size_list), 200, output_size), nn.Sigmoid()))
                elif output_size >= 2:
                    self.pred_module_list.append(nn.Sequential(MLP(sum(self.input_size_list), 200, output_size), nn.Softmax(dim=-1)))
            elif type_list[i] == "num":
                self.pred_module_list.append(MLP(sum(self.input_size_list), 200, output_size))

    def forward(self, x_list):

        hidden_list = []
        for i, model_name in enumerate(self.model_list):
            if model_name == 'mlp':
                output = self.models[i](x_list[i]).squeeze(1)
            elif model_name in ['lstm',"gru","bilstm",'bigru','biattlstm','biattgru'] :
                output = self.models[i](x_list[i])
            hidden_list.append(output)

        hiddens = torch.cat(hidden_list, dim=-1)

        output_list = []
        for pred_module in self.pred_module_list:
            output = pred_module(hiddens)
            output_list.append(output)
        return output_list

    def count_parameters(self):
        return sum(p.numel() for p in self.parameters() if p.requires_grad)

if __name__ == "__main__":

    
    from model import *
    from tqdm import tqdm
    import numpy as np

    x_list = [torch.randn(1, 4, 20)]
    m_list = [torch.randint(0, 2, (1, 4, 20))]
    t_list = [torch.arange(0, 4).reshape(1, -1, 1)]

    input_size_list = [20]
    hidden_size_list = [128]
    output_size_list = [1, 1]
    type_list = ["cat","num"]
    model_list = ["biattlstm"]

    model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)

    yhat = model(x_list)

    print("--")