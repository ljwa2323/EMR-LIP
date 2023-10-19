import torch

def calculate_integrated_gradients(X, baseline, model, target, steps=50, device='cuda:0'):
    # 将所有的 Tensor 移到 GPU 上
    X = X.to(device)
    if baseline is None:
        baseline = torch.zeros_like(X, device=device)
    else:
        baseline = baseline.to(device)
    model = model.to(device)

    # 初始化一个全零的梯度向量
    integrated_gradients = torch.zeros_like(X)

    # 计算输入和基线之间的差值，并预先除以 steps
    diff_X = (X - baseline) / steps

    # 对于每个步骤，计算模型的预测和梯度
    for step in range(steps):
        # 计算当前步骤的输入
        current_input_X = baseline + step * diff_X
        current_input_X.requires_grad_()  # 确保输入数据需要梯度

        # 计算模型的预测
        # with torch.no_grad():
        preds = model(current_input_X)

        # 计算预测对输入的梯度
        grads_X = torch.autograd.grad(preds[0][target], current_input_X, retain_graph=True)[0]

        # 将梯度加到 integrated_gradients 上
        integrated_gradients += grads_X

    integrated_gradients = integrated_gradients.to('cpu').data
    return integrated_gradients
