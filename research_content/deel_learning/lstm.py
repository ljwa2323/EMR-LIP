import torch
import torch.nn as nn

class LSTM(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(LSTM, self).__init__()
        
        self.hidden_size = hidden_size
        
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        
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
        # LSTM 输出
        lstm_out, _ = self.lstm(x)  # lstm_out: [batch_size, seq_len, hidden_size]
        
        # 取最后一个时间步的输出
        last_output = lstm_out[:, -1, :]
        
        # 输出层
        output = self.fc_out(last_output)
        
        return output

class BiLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(BiLSTM, self).__init__()
        
        self.hidden_size = hidden_size
        
        # 双向 LSTM 参数
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True, bidirectional=True)
        
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
        
        # LSTM 输出
        lstm_out, _ = self.lstm(x)  # lstm_out: [batch_size, seq_len, 2 * hidden_size]
        
        # 取最后一个时间步的输出
        last_output = lstm_out[:, -1, :]
        
        # 输出层
        output = self.fc_out(last_output)
        
        return output


class BiLSTMWithAttention(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(BiLSTMWithAttention, self).__init__()
        
        self.hidden_size = hidden_size
        
        # 双向 LSTM 参数
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True, bidirectional=True)
        
        # Attention 层
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
        
        # LSTM 输出
        lstm_out, _ = self.lstm(x)  # lstm_out: [batch_size, seq_len, 2 * hidden_size]
        
        # Attention 权重
        attention_weights = torch.softmax(self.attention(lstm_out), dim=1)
        
        # 加权求和得到上下文向量
        context_vector = torch.sum(attention_weights * lstm_out, dim=1)
        
        # 输出层
        output = self.fc_out(context_vector)
        
        return output