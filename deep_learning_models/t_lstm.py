import torch
import torch.nn as nn

class TLSTMCell(nn.Module):
    def __init__(self, input_size, hidden_size):
        super(TLSTMCell, self).__init__()
        
        self.input_size = input_size
        self.hidden_size = hidden_size
        
        self.lstm = nn.LSTMCell(input_size, hidden_size)
        
        self.Wd = nn.Parameter(torch.randn(hidden_size, hidden_size))
        self.bd = nn.Parameter(torch.randn(hidden_size))
        
        self.init_weights()
        
    def init_weights(self):
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
        
    def forward(self, x, h_prev, c_prev, delta_t):
        # Subspace decomposition and memory cell adjustment
        c_s = torch.tanh(torch.matmul(c_prev, self.Wd) + self.bd)
        c_s_hat = c_s * (1 / torch.log(torch.exp(torch.Tensor([1]).to(x.device)) + delta_t))
        c_t = c_prev - c_s
        c_star = c_t + c_s_hat
        
        # Standard LSTM update with adjusted memory cell
        h, c = self.lstm(x, (h_prev, c_star))
        
        return h, c

class TLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(TLSTM, self).__init__()
        
        self.hidden_size = hidden_size
        self.tlstm_cell = TLSTMCell(input_size, hidden_size)
        
        # Output layer
        self.fc_out = nn.Linear(hidden_size, output_size)
        
    def forward(self, x, delta_t):
        batch_size, seq_len, _ = x.size()
        
        h = torch.zeros(batch_size, self.hidden_size).to(x.device)
        c = torch.zeros(batch_size, self.hidden_size).to(x.device)
        
        outputs = []
        
        for t in range(seq_len):
            h, c = self.tlstm_cell(x[:, t, :], h, c, delta_t[:, t])
            outputs.append(h)
        
        # Output layer
        output = self.fc_out(outputs[-1])
        
        return output

if __name__ == "__main__":

    # Usage example:
    # Define the input size, hidden size, and output size
    input_size = 64
    hidden_size = 128
    output_size = 64

    # Instantiate the model
    model = TLSTM(input_size, hidden_size, output_size)

    x = torch.randn(1, 10, 64)
    delta = torch.rand(1, 10, 1)
    y_hat = model(x, delta)

    print("---")


