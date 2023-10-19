import torch
import torch.nn as nn

from mlp import *

class GRUD(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(GRUD, self).__init__()
        
        # Define the parameters for the GRU-D
        self.hidden_size = hidden_size
        
        # Decay mechanism parameters
        self.W_gamma = nn.Parameter(torch.randn(hidden_size))
        self.b_gamma = nn.Parameter(torch.randn(hidden_size))
        
        # GRU parameters
        self.gru_cell = nn.GRUCell(input_size, hidden_size)
        
        # Decay mechanism applied to input and hidden state
        self.fc_gamma_x = nn.Linear(input_size, input_size)
        self.fc_gamma_h = nn.Linear(1, hidden_size)
        
        # Output layer
        self.fc_out = nn.Linear(hidden_size, output_size)
        
        self.sigmoid = nn.Sigmoid()
        self.tanh = nn.Tanh()
        
        self.init_weights()
        
    def init_weights(self):
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.zeros_(param)
        
    def forward(self, x, masks, deltas_x, deltas_h, mean_observation):

        batch_size, seq_len, _ = x.size()
        
        h = torch.zeros(batch_size, self.hidden_size).to(x.device)
        
        outputs = []
        
        last_observation = torch.zeros_like(x[:, 0, :])
        for t in range(seq_len):
            # Decay mechanism
            gamma_x = torch.exp(-torch.max(torch.zeros_like(deltas_x[:, t]), self.fc_gamma_x(deltas_x[:, t])))
            gamma_h = torch.exp(-torch.max(torch.zeros_like(deltas_h[:, t]), self.fc_gamma_h(deltas_h[:, t])))
            
            # Imputation
            x_hat = masks[:, t, :] * x[:, t, :] + (1 - masks[:, t, :]) * (gamma_x * last_observation + (1 - gamma_x) * mean_observation)
            # Update last_observation
            last_observation = x_hat
            # GRU-D update
            h = self.gru_cell(x_hat, h * gamma_h)
            
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
    model = GRUD(input_size, hidden_size, output_size)
    x = torch.randn(1, 10, 64)
    x_mean = x.mean(dim=(0, 1)).unsqueeze(0)
    delta_h = torch.rand(1, 10, 1)
    mask = torch.randint(0, 2, (1, 10, 64))
    delta_x =  torch.rand(1, 10, 64) * 3.0
    y_hat = model.forward(x, mask, delta_x, delta_h, x_mean)
    print("-")