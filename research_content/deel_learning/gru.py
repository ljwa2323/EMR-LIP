import torch
import torch.nn as nn

class GRU(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(GRU, self).__init__()
        
        self.hidden_size = hidden_size
        
        # 使用整个 GRU 层而不是 GRUCell
        self.gru = nn.GRU(input_size, hidden_size, batch_first=True)
        
        # 输出层
        self.fc_out = nn.Linear(hidden_size, output_size)
        
        self.init_weights()
        
    def init_weights(self):
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
        
    def forward(self, x):
        # GRU 输出
        gru_out, _ = self.gru(x)  # gru_out: [batch_size, seq_len, hidden_size]
        
        # 取最后一个时间步的输出
        last_output = gru_out[:, -1, :]
        
        # 输出层
        output = self.fc_out(last_output)
        
        return output
    
class BiGRU(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(BiGRU, self).__init__()
        
        self.hidden_size = hidden_size
        
        # 双向 GRU 参数
        self.gru = nn.GRU(input_size, hidden_size, batch_first=True, bidirectional=True)
        
        # 输出层
        self.fc_out = nn.Linear(2 * hidden_size, output_size)
        
        self.init_weights()
        
    def init_weights(self):
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
        
    def forward(self, x):
        batch_size = x.size(0)
        
        # GRU 输出
        gru_out, _ = self.gru(x)  # gru_out: [batch_size, seq_len, 2 * hidden_size]
        
        # 取最后一个时间步的输出
        last_output = gru_out[:, -1, :]
        
        # 输出层
        output = self.fc_out(last_output)
        
        return output

class BiGRUWithAttention(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(BiGRUWithAttention, self).__init__()
        
        self.hidden_size = hidden_size
        
        # 双向 GRU 参数
        self.gru = nn.GRU(input_size, hidden_size, batch_first=True, bidirectional=True)
        
        # 注意力层
        self.attention = nn.Linear(2 * hidden_size, 1)
        
        # 输出层
        self.fc_out = nn.Linear(2 * hidden_size, output_size)
        
        self.init_weights()
        
    def init_weights(self):
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
        
    def forward(self, x):
        batch_size = x.size(0)
        
        # GRU 输出
        gru_out, _ = self.gru(x)  # gru_out: [batch_size, seq_len, 2 * hidden_size]
        
        # 注意力权重
        attention_weights = torch.softmax(self.attention(gru_out), dim=1)
        
        # 加权求和得到上下文向量
        context_vector = torch.sum(attention_weights * gru_out, dim=1)
        
        # 输出层
        output = self.fc_out(context_vector)
        
        return output