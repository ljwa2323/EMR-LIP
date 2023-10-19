import torch
import torch.nn as nn

class GRU(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(GRU, self).__init__()
        
        self.hidden_size = hidden_size
        
        # GRU parameters
        self.gru_cell = nn.GRUCell(input_size * 2 + 1, hidden_size)
        
        # Output layer
        self.fc_out = nn.Linear(hidden_size, output_size)
        
        self.init_weights()
        
    def init_weights(self):
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
        
    def forward(self, x, mask, ti):
        batch_size, seq_len, _ = x.size()
        
        h = torch.zeros(batch_size, self.hidden_size).to(x.device)
        
        outputs = []
        
        for t in range(seq_len):
            # Apply mask to the input
            x_t = torch.cat([x[:, t], mask[:, t], ti[:, t]], dim=-1)
            h = self.gru_cell(x_t, h)
            outputs.append(h)
        
        # Output layer
        output = self.fc_out(outputs[-1])
        
        return output