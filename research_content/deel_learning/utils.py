import torch
import copy

def calculate_integrated_gradients(input_list, model, target_class_index, steps=50, device='cuda:0', baseline_list=None):
    # 将所有的 Tensor 移到 GPU 上并初始化基线和梯度向量
    for i in range(len(input_list)):
        input_list[i] = input_list[i].to(device)

    if baseline_list is None:
        baseline_list = [torch.zeros_like(input_tensor, device=device) for input_tensor in input_list]
    else:
        # 根据 input_list 的形状扩展 baseline_list
        baseline_list = [baseline.to(device).expand_as(input_tensor) for baseline, input_tensor in zip(baseline_list, input_list)]

    integrated_gradients_list = [torch.zeros_like(input_tensor) for input_tensor in input_list]
    diff_list = [(input_tensor - baseline_tensor) / steps for input_tensor, baseline_tensor in zip(input_list, baseline_list)]

    # 对于每个步骤，计算模型的预测和梯度
    for step in range(steps):
        for i in range(len(input_list)):
            # 创建当前步骤的输入列表，其中只有第i个元素是变化的
            current_step_input = input_list[:]  # 使用原始输入列表的副本
            current_step_input[i] = baseline_list[i] + step * diff_list[i]  # 只更新第i个元素
            current_step_input[i].requires_grad_()  # 为当前变量启用梯度跟踪

            # 执行模型前向传播
            preds = model(current_step_input)
            target_preds = preds[0][:, target_class_index]  # 选择目标类别的预测值

            # 计算梯度
            grads = torch.autograd.grad(target_preds, current_step_input[i], retain_graph=True)[0]

            # 检查当前步骤的输入是否与基线相同，如果相同，则梯度设置为零
            mask = (torch.abs(current_step_input[i] - baseline_list[i]) < 1e-6).float()  # 修改
            grads = grads * (1 - mask)

            integrated_gradients_list[i] += grads

    # 将计算的梯度除以步数并转移到 CPU
    for i in range(len(integrated_gradients_list)):
        integrated_gradients_list[i] = (integrated_gradients_list[i] / steps).to('cpu')

    return integrated_gradients_list