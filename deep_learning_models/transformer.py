import torch
import torch.nn as nn

class TimeEmbedding(nn.Module):
    def __init__(self, embed_size):
        super(TimeEmbedding, self).__init__()
        self.fc1 = nn.Linear(1, embed_size)
        self.fc2 = nn.Linear(embed_size, embed_size)
        self.layer_norm = nn.LayerNorm(embed_size)  # 添加 LayerNorm
        
    def forward(self, x):
        residual = x
        x = torch.tanh(self.fc1(x))
        x = self.layer_norm(self.fc2(x)) + residual  # 在这里应用 LayerNorm
        return x

class TransformerModel(nn.Module):
    def __init__(self, input_dim, d_model, output_dim, num_layers=6, nhead=8, dropout=0.1):
        super(TransformerModel, self).__init__()
        self.input_dim = input_dim
        self.d_model = d_model
        
        self.time_embedding = TimeEmbedding(embed_size=d_model)
        
        self.value_encoder = nn.Linear(input_dim * 2, d_model)
        
        self.transformer_encoder = nn.TransformerEncoder(
            nn.TransformerEncoderLayer(d_model, nhead=nhead, dropout=dropout), 
            num_layers=num_layers
        )
        
        self.fc_out = nn.Linear(d_model, output_dim)
        
        self.init_weights()
        
    def init_weights(self):
        initrange = 0.1
        self.value_encoder.bias.data.zero_()
        self.value_encoder.weight.data.uniform_(-initrange, initrange)
        self.fc_out.bias.data.zero_()
        self.fc_out.weight.data.uniform_(-initrange, initrange)
        
    def forward(self, src, src_mask, ti, src_key_padding_mask=None):
        # 分别处理时间信息和特征信息
        time_info = ti
        feature_info = src  # 缺失模式前的特征
        missing_pattern = src_mask # 缺失模式
        
        # 计算时间嵌入
        time_embedding = self.time_embedding(time_info)
        
        # 计算特征和缺失模式的连接后的向量，并进行编码
        concatenated_features = torch.cat((feature_info, missing_pattern), dim=-1)
        feature_encoding = self.value_encoder(concatenated_features)
        
        # 将时间嵌入和特征编码拼接起来
        # src = torch.cat((feature_encoding, time_embedding), dim=2)
        src = feature_encoding + time_embedding
        
        # 通过transformer编码器
        output = self.transformer_encoder(src, src_key_padding_mask=src_key_padding_mask)
        
        # 通过最终的全连接层
        output = self.fc_out(output[:,-1])
        
        return output

if __name__ == "__main__":

    # 创建模型实例
    # 假设我们有一个11维的输入特征空间（包括时间信息和缺失模式）和一个2维的输出空间
    model = TransformerModel(input_dim=11, d_model=64, output_dim=64, num_layers=3, nhead=4)
    x = torch.randn(1,10,12)
    mask = torch.randn(1,10,12)
    y_hat = model(x, mask)
    print("---")
