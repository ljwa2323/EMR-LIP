setwd("./")
# getwd()
# rm(list=ls());gc()

library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)
library(readxl)
library(jsonlite)

source("/home/luojiawei/EMR_LIP/EMR_LIP.R")

root_path <- "/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share"
folders <- list.files(root_path)

k<-1
op_id <- folders[k]
y_mat <- fread(file.path(root_path, op_id, "y_mat.csv"))
y_static <- fread(file.path(root_path, op_id, "y_static.csv"))

names(y_static)
names(y_mat)

read_data <- function(k) {
    # k<-1
    op_id <- folders[k]
    res <- list()
    res[[1]] <- fread(file.path(root_path, op_id, "y_mat.csv"), header = T)
    res[[1]]$op_id <- op_id
    res[[2]] <- fread(file.path(root_path, op_id, "y_static.csv"), header = T)
    res[[2]]$op_id <- op_id
    if(k %% 1000 == 0) print(k)
    return(res)
}

res_list <- lapply(1:length(folders), read_data)# length(folders)
# res_list[[1]]

# res_list[[91102]] <- read_data(91102)
y_mat <- do.call(rbind, lapply(res_list, function(x) x[[1]])) %>% as.data.frame
y_static <- do.call(rbind, lapply(res_list, function(x) x[[2]]))  %>% as.data.frame

y_mat[1:3,]

normalize <- function(x) {
  return(x / sum(x))
}


output_size_list <- apply(y_mat[,2:(ncol(y_mat)-1)], 2, function(x) length(unique(na.omit(x)))) %>%
                        ifelse(. > 2, ., 1)

output_size_list <- c(output_size_list, 
                        apply(y_static[,1:(ncol(y_static)-1)], 2, function(x) length(unique(na.omit(x)))) %>%
  ifelse(. > 2, ., 1))

# length(output_size_list)
# length(type_list)
type_list <- rep("cat", length(output_size_list))

# table(y_mat$vaso_use)
# names(y_mat)
weight_list_dym <- list()
for(i in 2:(ncol(y_mat)-1)){
    # i<-2
    if(output_size_list[i-1] == 1) {
        m <- mean(y_mat[,i,drop=T],na.rm=T)
        m <- min(max(0.001, m),0.999)
        weight_list_dym[[names(y_mat)[i]]] <- normalize(c(1/(1-m), 1/m))
    } else{
        u <- factor(y_mat[,i,drop=T],levels=0:(output_size_list[i-1]-1))
        m <- prop.table(table(u))
        m <- pmin(pmax(0.001, m),0.999)
        m <- normalize(1/m) %>% as.vector
        weight_list_dym[[names(y_mat)[i]]] <- m
    }
}

weight_list_dym
write_json(weight_list_dym, "./task_weight_dym_mimic_iv.json")

weight_list_sta <- list()

for(i in 1:(ncol(y_static)-1)){
    # i<-1
    if(output_size_list[ncol(y_mat)-2+i] == 1) {
        m <- mean(y_static[,i,drop=T],na.rm=T)
        m <- min(max(0.001, m),0.999)
        weight_list_sta[[names(y_static)[i]]] <- normalize(c(1/(1-m), 1/m))
    } else{
        u <- factor(y_static[,i,drop=T],levels=0:(output_size_list[ncol(y_mat)-2+i]-1))
        m <- prop.table(table(u))
        m <- pmin(pmax(0.001, m),0.999)
        m <- normalize(1/m) %>% as.vector
        weight_list_sta[[names(y_static)[i]]] <- m
    }
}

weight_list_sta
write_json(weight_list_sta, "./task_weight_sta_mimic_iv.json")



root_path <- "/home/luojiawei/EMR_LIP_data/eicu_crd/all_stids_share"
folders <- list.files(root_path)

list.files(file.path(root_path, op_id))

read_data <- function(k) {
  # k <- 1
    op_id <- folders[k]
    res <- list()
    res[[1]] <- fread(file.path(root_path, op_id, "dynamic.csv"), header = T)    
    if(k %% 1000 == 0) print(k)
    return(res)
}

res_list <- lapply(1:length(folders), read_data)# length(folders)
# res_list[[1]]

# res_list[[91102]] <- read_data(91102)
X <- do.call(rbind, lapply(res_list, function(x) x[[1]])) %>% as.data.frame

X_means <- apply(X[,-1], 2, mean)
X_sds <- apply(X[,-1], 2, sd)
X_mins <- apply(X[,-1], 2, min)
X_sds[X_sds < 1e-6] <- 1e-6
tar_vars <- names(X_means)[c(13:20)] # eicu_crd
# tar_vars <- names(X_means)[c(13:20)] # mimic_iv
X_means[names(X_means) %in% tar_vars] <- 0
X_sds[names(X_sds)  %in% tar_vars] <- 1
X_stdmins <- (X_mins - X_means) / X_sds

X_df <- data.frame(name = names(X_means), mean = X_means, sd = X_sds, min = X_mins)
write.csv(X_df, "/home/luojiawei/EMR_LIP/stat_info_eicu_crd/X_stat_share.csv", row.names = FALSE)

# ================ get baseline values ==================================

fn1 <- read.csv("./fn1.csv")
fn2 <- read.csv("./fn2.csv")
fn3 <- read.csv("./fn3.csv")
fn4 <- read.csv("./fn4.csv")
fn5 <- read.csv("./fn5.csv")

blv1 <- data.frame(var = fn1[[1]], blv = c(lab_stdmins, rep(0, length(lab_stdmins)), x_s_stdmins))
blv2 <- data.frame(var = fn2[[1]], blv = c(vit_out_stdmins, rep(0, length(vit_out_stdmins)-1)))
blv3 <- data.frame(var = fn3[[1]], blv = c(vit_in_stdmins[-length(vit_in_stdmins)], rep(0, length(vit_in_stdmins)-1)))
blv4 <- data.frame(var = fn4[[1]], blv = c(vit_out_stdmins, rep(0, length(vit_out_stdmins)-1)))
blv5 <- data.frame(var = fn5[[1]][1:(nrow(fn5)-4)], blv = c(lab_stdmins, rep(0, length(lab_stdmins)), oper_info_stdmins))

# 将 blv1 到 blv5 的数据框写入 CSV 文件
write.csv(blv1, "./blv1.csv", row.names = FALSE)
write.csv(blv2, "./blv2.csv", row.names = FALSE)
write.csv(blv3, "./blv3.csv", row.names = FALSE)
write.csv(blv4, "./blv4.csv", row.names = FALSE)
write.csv(blv5, "./blv5.csv", row.names = FALSE)

