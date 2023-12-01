# EMR-LIP


### EMR-LIP 简介

EMR-LIP 是一个 light framework, 提供了一系列在不同EMR数据库之间通用的工具，用于处理 EMR 中纵向不规则数据. EMR-LIP框架专注于将 long table 或 wide table 转化为方便下游任务建模的规则的多元时间序列，包括固定点触发的预测模型或 continuous prediciton model 的开发，或者强化学习模型的开发。

需要说明的是，EMR-LIP 不包含一些与研究或数据库特异的 data cleaning 工作，包括异常值处理，变量合并以及 clinical concepts 的生成，因为这些工作难以用统一的程序在不同数据库之间进行迁移。相反，EMR-LIP将纵向不规则数据处理过程中的共性部分抽象了出来，并针对这些内容开发了灵活的工具，从而促进EMR纵向不规则数据预处理的标准化和下游任务的可比性。

EMR-LIP 其他framework的核心差异在于，EMR-LIP提供了更加细粒度的变量分类。在纵向不规则数据的预处理过程中，通常涉及到序列重采样，缺失值填补，变量转化等过程，不同类型的变量在这些过程中的处理细节是不同的。之前的框架大多只关注变量的 value type, 并将其分类为连续型和离散型，这种分类不足以正确处理EMR-LIP中所有的纵向不规则采样的变量。

The EMR-LIP framework classifies variables in EMRs from three distinct perspectives: temporal attributes, value types, and acquisition methods.  In terms of temporal attributes, variables are categorized into static, single-point, and interval-based types.  From the viewpoint of value types, they are classified as numerical, ordinal discrete, nominal discrete and binary.  Based on acquisition methods, variables are designated as either observational variables or operational variables. A comprehensive breakdown of the variable types is provided in **Table 1**. 

### **Table 1. Classification of variables in EMR databases.**

<table>
    <tr>
        <th>Classification</th>
        <th>Type</th>
        <th>Description</th>
        <th>Example Variable</th>
    </tr>
    <tr>
        <td rowspan="3">By time Attribute</td>
        <td>Static</td>
        <td>Without temporal attributes</td>
        <td>Gender</td>
    </tr>
    <tr>
        <td>Single-point</td>
        <td>Contains only a single time information</td>
        <td>Temperature</td>
    </tr>
    <tr>
        <td>Interval</td>
        <td>Contains a start and end time</td>
        <td>Ventilation status</td>
    </tr>
    <tr>
        <td rowspan="4">By value type</td>
        <td>Continuous</td>
        <td>Can take continuous values</td>
        <td>Temperature</td>
    </tr>
    <tr>
        <td>Ordinal Discrete</td>
        <td>Discrete values with an inherent order</td>
        <td>GCS (Glasgow Coma Scale) score</td>
    </tr>
    <tr>
        <td>Nominal Discrete</td>
        <td>Discrete values without any inherent order</td>
        <td>Ventilation status</td>
    </tr>
    <tr>
        <td>Binary</td>
        <td>Has two distinct outcomes, typically representing presence or absence</td>
        <td>Ventilation (yes/no)</td>
    </tr>
    <tr>
        <td rowspan="2">By acquisition type</td>
        <td>Observational Variable</td>
        <td>Data naturally recorded or collected without any human intervention</td>
        <td>Gender, Temperature</td>
    </tr>
    <tr>
        <td>Operational Variable</td>
        <td>Derived from a specific operation or action</td>
        <td>Ventilation status</td>
    </tr>
</table>


<img src="./assets/EMR-LIP.png" widht=500>

### EMR-LIP 框架的组件介绍

EMR-LIP 框架由input，variable dictionary, statistical information generation, sequence resampling, data imputation, variable transformation, feature engineering, output 等8部分构成，下面我们讲一一介绍.

- Input

EMR-LIP 接受两种形式的输入, 包括long table 和 wide table.  