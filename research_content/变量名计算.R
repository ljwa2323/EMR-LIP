
library(data.table)
library(magrittr)

ds_id <- fread("/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv") %>% as.data.frame

ds_id[1:4,]

folder <- paste0("/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share/", ds_id$stay_id[1])

list.files(folder)

fn <- c()
ds <- read.csv(file.path(folder, "dynamic.csv"))
fn <- c(fn, names(ds)[-1], paste0("mask_",names(ds)[-c(1,ncol(ds))]))
fn
write.csv(fn, file="./变量名.csv", row.names=F)









