setwd("/home/luojiawei/EMR_LIP")

# rm(list = ls());gc()

source("./EMR_LIP.R")

library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)
library(readxl)
library(reshape2)


# ------- 样本筛选 ------------------

patients<-fread("/home/luojiawei/eicu/eicu-collaborative-research-database-2.0/patient.csv.gz",header=T,fill=T)

as.data.frame(patients[1:2,])

length(unique(patients$patientunitstayid))
length(unique(patients$patienthealthsystemstayid))
length(unique(patients$uniquepid))

range(patients$unitdischargeoffset) / 60 / 24
quantile(patients$unitdischargeoffset/60, probs=seq(0.1,0.9,0.1))

table(patients$unitstaytype)

# 计算 unitadmitoffset
patients$hospitaladmittime24 <- hms::as_hms(patients$hospitaladmittime24)
patients$unitadmittime24 <- hms::as_hms(patients$unitadmittime24)

# 计算时间差异，单位为分钟
time_difference <- as.numeric(difftime(patients$unitadmittime24, patients$hospitaladmittime24, units = "mins"))

# 修正超过24小时的情况
time_difference[time_difference < 0] <- time_difference[time_difference < 0] + 24 * 60

# 计算 unitadmitoffset
patients$unitadmitoffset <- patients$hospitaladmitoffset + time_difference

# 计算 ICU 住院时长，单位为分钟
patients$los_icu <- patients$unitdischargeoffset - patients$unitadmitoffset

# 如果需要将单位转换为小时或天，可以进行相应的转换
patients$los_icu_hours <- patients$los_icu / 60  # 转换为小时
patients$los_icu_days <- patients$los_icu / 1440  # 转换为天

# 筛选 ICU 住院时长在 12 小时以上且不超过 5 天的记录
patients <- patients %>% filter(los_icu_hours >= 12 & los_icu_days <= 30)

dim(patients)
# [1] 138508     33

# 筛选年龄在 18 岁以上且 80 岁以下的记录
patients <- patients %>%
  filter(age >= 18 & age <= 80)

dim(patients)
# [1] 114674     33

patients$hospitaldischargestatus <- ifelse(patients$hospitaldischargestatus == "Expired", 1, 0)

# 添加 death_offset 列
patients$death_offset <- ifelse(patients$hospitaldischargestatus == 1, patients$hospitaldischargeoffset, NA)

# 添加 los_icu_1d 列，判断 los_icu 是否大于等于 1 天
patients$los_icu_1d <- ifelse(patients$los_icu_days >= 1, 1, 0)

as.data.frame(patients[1:2,])
names(patients)
table(patients$los_icu_1d)



patients <- patients[which(patients$unitstaytype == "admit"), ]

range(patients$los_icu_hours)

# 添加 death_1d, death_2d, death_4d, death_7d 列
patients <- patients %>%
  mutate(
    death_1d = ifelse(!is.na(death_offset) & (death_offset - unitadmitoffset) <= 1440, 1, 0),
    death_2d = ifelse(!is.na(death_offset) & (death_offset - unitadmitoffset) <= 2880, 1, 0),
    death_4d = ifelse(!is.na(death_offset) & (death_offset - unitadmitoffset) <= 5760, 1, 0),
    death_7d = ifelse(!is.na(death_offset) & (death_offset - unitadmitoffset) <= 10080, 1, 0)
  )


# -------- 字典读取 -------------

d_s <- read_excel("./var_dict_eicu_crd.xlsx", sheet=1, col_names=T)
d_d <- read_excel("./var_dict_eicu_crd.xlsx", sheet=3, col_names=T)

# -------- eICU-CRD database connection -------------

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "eicu", 
                 host = "127.0.0.1", port = 5432, 
                 user = "ljw", 
                 password = "123456")
schema_name <- "eicu_crd"

# Blood Gas -------------------------

table_name <- "pivoted_bg"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
bg <- dbGetQuery(con, query)
names(bg)
d_bg <- d_d[d_d$item_id %in% names(bg)[c(3:10)],]
as.data.frame(d_bg)
stat_bg <- get_stat_wide(bg, d_bg$item_id, d_bg$value_type, d_bg$cont)

# Lab -------------------------

table_name <- "pivoted_lab"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
lab <- dbGetQuery(con, query)
lab[1:5,]
names(lab)
d_lab <- d_d[d_d$item_id %in% names(lab)[c(3:24)],]
as.data.frame(d_lab)
stat_lab <- get_stat_wide(lab, d_lab$item_id, d_lab$value_type, d_lab$cont)

# Vital -------------------------

table_name <- "pivoted_vital"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vit <- dbGetQuery(con, query)

vit[1:5,]
names(vit)
d_vit <- d_d[d_d$item_id %in% names(vit)[c(4:14)],]
as.data.frame(d_vit)
stat_vit <- get_stat_wide(vit, d_vit$item_id, d_vit$value_type, d_vit$cont)


# Medication -------------------------

table_name <- "pivoted_med"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
med <- dbGetQuery(con, query)

med[1:5,]
names(med)
d_med <- d_d[d_d$item_id %in% names(med)[c(5:13)],]
as.data.frame(d_med)
stat_med <- get_stat_wide(med, d_med$item_id, d_med$value_type, d_med$cont)

# Uring Output -------------------------

table_name <- "pivoted_uo"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
uo <- dbGetQuery(con, query)
names(uo)[3] <- "uo"
uo[1:5,]
names(uo)
d_uo <- d_d[d_d$item_id %in% names(uo)[c(3)],]
as.data.frame(d_uo)
uo <- remove_extreme_value_wide(uo, d_uo$item_id, d_uo$value_type, 1:2, d_uo)
stat_uo <- get_stat_wide(uo, d_uo$item_id, d_uo$value_type, d_uo$cont)

# GCS -------------------------

table_name <- "gcs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
gcs <- dbGetQuery(con, query)

gcs[1:3,]
names(gcs)
d_gcs <- d_d[d_d$item_id %in% names(gcs)[c(3)],]
as.data.frame(d_gcs)
stat_gcs <- get_stat_wide(gcs, d_gcs$item_id, d_gcs$value_type, d_gcs$cont)

# Ventilation -------------------------

table_name <- "pivoted_o2"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
o2 <- dbGetQuery(con, query)

o2[1:3,]
names(o2)[4] <- "o2"
d_o2 <- d_d[d_d$item_id %in% names(o2)[c(4)],]
o2$o2 <- ifelse(o2$o2 > 0, 1, 0)
as.data.frame(d_o2)
stat_o2 <- get_stat_wide(o2, d_o2$item_id, d_o2$value_type, d_o2$cont)

# 关闭连接
dbDisconnect(con)




get_data <- function(k) {

    # patients[k,,drop=F]
    # k <- 8068
    cur_sid <- patients$uniquepid[k]
    cur_adid <- patients$patienthealthsystemstayid[k]
    cur_stid <- patients$patientunitstayid[k]

    t0 <- 0
    t1 <- floor(patients$unitadmitoffset[k] / 60)
    t2 <- ceiling((patients$unitdischargeoffset[k] - t1) / 60)
    t_list <- seq(t0, t2 - t1,4)
    y_mat <- list()
    y_mat[[1]] <- cbind("time" = t_list)
    if(!is.na(patients$death_offset[k])){
        t_d <- ceiling((patients$death_offset[k] - t1) / 60)
        y_mat[[2]] <- cbind("24h_death" = ifelse(t_d - t_list < 24, 1, 0))
    } else{
        y_mat[[2]] <- cbind("24h_death" = rep(0, length(t_list)))
    }

    y_mat[[3]] <- cbind("icu_los"=ifelse(t2 - t_list <= 12, 1, 0))
    y_mat <- do.call(cbind, y_mat)
    y_mat <- y_mat %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
    
    # names(patients)
    y_static <- patients[k, c(35:39,17), drop=F]
    
    bg_k <- bg[which(bg$patientunitstayid == cur_stid), ,drop=F]
    bg_k$time1 <- (bg_k$chartoffset - t1) / 60
    bg_k1 <- resample_single_wide(bg_k,
                                d_bg$item_id,
                                d_bg$value_type,
                                d_bg$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_bg <- bg_k1[,2] == "1"
    bg_k1 <- bg_k1[,c(1,3:ncol(bg_k1))]
    m_bg <- get_mask(bg_k1,2:ncol(bg_k1), 1)
    bg_k1 <- fill(bg_k1, 2:ncol(bg_k1), 1, d_bg$value_type, d_bg$fill1, d_bg$fill2, stat_bg)
    bg_k1 <- fill_last_values(bg_k1, m_bg, 2:ncol(bg_k1), 1, d_bg)
    # bg_k1[1:2,]

    # uo[1:3,]
    uo_k <- uo[which(uo$patientunitstayid == cur_stid), ,drop=F]
    uo_k$time1 <- (uo_k$chartoffset - t1) / 60

    uo_k1 <- resample_single_wide(uo_k,
                                d_uo$item_id,
                                d_uo$value_type,
                                d_uo$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_uo <- uo_k1[,2] == "1"
    uo_k1 <- uo_k1[,c(1,3:ncol(uo_k1))]
    m_uo <- get_mask(uo_k1,2:ncol(uo_k1), 1)
    uo_k1 <- fill(uo_k1, 2:ncol(uo_k1), 1, d_uo$value_type, d_uo$fill1, d_uo$fill2, stat_uo)
    uo_k1 <- fill_last_values(uo_k1, m_uo, 2:ncol(uo_k1), 1, d_uo)
    # uo_k1[1:2,]

    # o2[1:2,]
    o2_k <- o2[which(o2$patientunitstayid == cur_stid), ,drop=F]
    o2_k$time1 <- (o2_k$chartoffset - t1) / 60

    o2_k1 <- resample_single_wide(o2_k,
                                d_o2$item_id,
                                d_o2$value_type,
                                d_o2$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_o2 <- o2_k1[,2] == "1"
    o2_k1 <- o2_k1[,c(1,3:ncol(o2_k1))]
    m_o2 <- get_mask(o2_k1,2:ncol(o2_k1), 1)
    o2_k1 <- fill(o2_k1, 2:ncol(o2_k1), 1, d_o2$value_type, d_o2$fill1, d_o2$fill2, stat_o2)
    o2_k1 <- fill_last_values(o2_k1, m_o2, 2:ncol(o2_k1), 1, d_o2)
    # o2_k1 <- to_onehot(o2_k1, 2:ncol(o2_k1), 1, d_o2$value_type, stat_o2)
    # o2_k1[1:2,]

    # gcs[1:4,]
    gcs_k <- gcs[which(gcs$patientunitstayid == cur_stid), ,drop=F]
    gcs_k$time1 <- (gcs_k$chartoffset - t1) / 60
    gcs_k1 <- resample_single_wide(gcs_k,
                                d_gcs$item_id,
                                d_gcs$value_type,
                                d_gcs$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_gcs <- gcs_k1[,2] == "1"
    gcs_k1 <- gcs_k1[,c(1,3:ncol(gcs_k1))]
    m_gcs <- get_mask(gcs_k1,2:ncol(gcs_k1), 1)
    gcs_k1 <- fill(gcs_k1, 2:ncol(gcs_k1), 1, d_gcs$value_type, d_gcs$fill1, d_gcs$fill2, stat_gcs)
    gcs_k1 <- fill_last_values(gcs_k1, m_gcs, 2:ncol(gcs_k1), 1, d_gcs)
    # gcs_k1[1:10,]

    vit_k <- vit[which(vit$patientunitstayid == cur_stid), ,drop=F]
    # vit_k[1:3,]
    vit_k$time1 <- (vit_k$chartoffset - t1) / 60
    vit_k1 <- resample_single_wide(vit_k,
                                d_vit$item_id,
                                d_vit$value_type,
                                d_vit$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_vit <- vit_k1[,2] == "1"
    vit_k1 <- vit_k1[,c(1,3:ncol(vit_k1))]
    m_vit <- get_mask(vit_k1,2:ncol(vit_k1), 1)
    vit_k1 <- fill(vit_k1, 2:ncol(vit_k1), 1, d_vit$value_type, d_vit$fill1, d_vit$fill2, stat_vit)
    vit_k1 <- fill_last_values(vit_k1, m_vit, 2:ncol(vit_k1), 1, d_vit)
    # vit_k1[1:10,]

    med_k <- med[which(med$patientunitstayid == cur_stid), ,drop=F]
    med_k$time1 <- (med_k$drugorderoffset - t1) / 60
    med_k$time2 <- (med_k$drugstopoffset - t1) / 60

    med_k1 <- resample_interval_wide(med_k,
                                d_med$item_id,
                                d_med$value_type,
                                d_med$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                "time2",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_med <- med_k1[,2] == "1"
    med_k1 <- med_k1[,c(1,3:ncol(med_k1))]
    m_med <- get_mask(med_k1,2:ncol(med_k1), 1)
    med_k1 <- fill(med_k1, 2:ncol(med_k1), 1, d_med$value_type, d_med$fill1, d_med$fill2, stat_med)
    med_k1 <- fill_last_values(med_k1, m_med, 2:ncol(med_k1), 1, d_med)
    # med_k1[1:2,]

    lab_k <- lab[which(lab$patientunitstayid == cur_stid), ,drop=F]
    lab_k$time1 <- (lab_k$chartoffset - t1) / 60
    lab_k1 <- resample_single_wide(lab_k,
                                d_lab$item_id,
                                d_lab$value_type,
                                d_lab$agg_fun, 
                                seq(t0, t2-t1, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_lab <- lab_k1[,2] == "1"
    lab_k1 <- lab_k1[,c(1,3:ncol(lab_k1))]
    m_lab <- get_mask(lab_k1,2:ncol(lab_k1), 1)
    lab_k1 <- fill(lab_k1, 2:ncol(lab_k1), 1, d_lab$value_type, d_lab$fill1, d_lab$fill2, stat_lab)
    lab_k1 <- fill_last_values(lab_k1, m_lab, 2:ncol(lab_k1), 1, d_lab)
    # lab_k1[1:4,]

    ind <- which(ind_bg | ind_vit | ind_gcs | ind_uo | ind_med | 
                    ind_o2 | ind_lab)
    if(length(ind) == 0) ind <- c(1)
    ds_k_ <- list(vit_k1, bg_k1[,2:ncol(bg_k1),drop=F],
                         gcs_k1[,2:ncol(gcs_k1),drop=F], 
                         uo_k1[,2:ncol(uo_k1),drop=F], 
                         med_k1[,2:ncol(med_k1),drop=F], 
                         o2_k1[,2:ncol(o2_k1),drop=F],
                         lab_k1[,2:ncol(lab_k1),drop=F]) %>% 
                            do.call(cbind, .) %>% 
                            convert_to_numeric_df
    m_k <- list(m_vit, m_bg[,2:ncol(m_bg),drop=F],
                         m_gcs[,2:ncol(m_gcs),drop=F], 
                         m_uo[,2:ncol(m_uo),drop=F], 
                         m_med[,2:ncol(m_med),drop=F], 
                         m_o2[,2:ncol(m_o2),drop=F],
                         m_lab[,2:ncol(m_lab),drop=F]) %>% 
                            do.call(cbind, .) %>% 
                            convert_to_numeric_df
    ds_k_ <- ds_k_[ind,]
    m_k <- m_k[ind,]

    if(nrow(ds_k_) == 1){
        ds_k_$dt <- c(0)
    } else{
        ds_k_$dt <- c(0, diff(ds_k_$time))
    }
    ds_k_$dt <- ds_k_$dt / 24
    ds_k_ <- ds_k_[,c("time",d_d$item_id,"dt")]
    m_k <- m_k[,c("time",d_d$item_id)]
    # ds_k_[1:2,]
    return(list(ds_k_, m_k, y_mat, y_static))
}

datas <- get_data(712)

datas[[1]][1:4,]
datas[[2]][1:4,]
datas[[3]]

which(patients$hospitaldischargestatus == 1)[1:10]
table(patients$hospitaldischargestatus)

process_data <- function(k, root_path) {

    # k<-441
    cur_stid <- patients$patientunitstayid[k]
    folder_path<-file.path(root_path, cur_stid)
    create_dir(folder_path, F)
    
    datas <- get_data(k)

    fwrite(datas[[1]], file=file.path(folder_path, "dynamic.csv"), row.names=F)
    fwrite(datas[[2]], file=file.path(folder_path, "mask.csv"), row.names=F)
    fwrite(datas[[3]], file=file.path(folder_path, "y_mat.csv"), row.names=F)
    fwrite(datas[[4]], file=file.path(folder_path, "y_static.csv"), row.names=F)
}

root_path <- "/home/luojiawei/EMR_LIP_data/eicu_crd/all_stids_share"
create_dir(root_path, T)


chunk_size <- 3000
num_rows <- nrow(patients)
num_chunks <- ceiling(num_rows / chunk_size)

results <- list()

for (i in 1:num_chunks) {
  start_index <- (i - 1) * chunk_size + 1
  end_index <- min(i * chunk_size, num_rows)
  
  results[[i]] <- mclapply(start_index:end_index, 
                    function(x) {
                      result <- tryCatch({
                        process_data(x, root_path = root_path)
                      }, error = function(e) {
                        print(e)
                        print(x)
                      })
                      if(x %% 1000 == 0) print(x)
                      
                      return(result)
                    }, mc.cores = detectCores())
  gc()
}

results <- do.call(c, results)

patients$patientunitstayid[100]
names(patients)

# 获取唯一的 uniquepid
unique_subjects <- patients %>% distinct(uniquepid)
set.seed(123)
# 随机分配训练集、测试集和验证集
unique_subjects <- unique_subjects %>%
  mutate(set = sample(c(1, 2, 3), size = n(), replace = TRUE, prob = c(0.7, 0.2, 0.1)))

# 将分配的集合标签加回到原始数据集
patients <- patients %>%
  left_join(unique_subjects, by = "uniquepid")

as.data.frame(patients[1:2,])
dim(patients)
table(patients$set)
write.csv(patients, file="/home/luojiawei/EMR_LIP_data/ds_id_eicu_crd.csv", row.names=F)
patients <- fread("/home/luojiawei/EMR_LIP_data/ds_id_eicu_crd.csv")
names(patients)

round(apply(patients[,c(36:39,17,35)], 2, mean) * 100,1)

# ======================================

get_data <- function(k) {

    # patients[k,,drop=F]
    # k <- 712
    cur_sid <- patients$uniquepid[k]
    cur_adid <- patients$patienthealthsystemstayid[k]
    cur_stid <- patients$patientunitstayid[k]

    # patients[1:2,]
    y_static <- list("los_icu_1d" = patients$los_icu_1d[k], "in_hospital_death" =  patients$hospitaldischargestatus[k]) %>% as.data.frame

    return(list(y_static))
}


process_data <- function(k, root_path) {

    # k<-441
    cur_stid <- patients$patientunitstayid[k]
    folder_path<-file.path(root_path, cur_stid)
    # create_dir(folder_path, F)
    
    datas <- get_data(k)

    fwrite(datas[[1]], file=file.path(folder_path, "y_static.csv"), row.names=F)
}


root_path <- "/home/luojiawei/EMR_LIP_data/eicu_crd/all_stids"
names(patients)
for(k in 1:nrow(patients)){
    # k <- 1
    cur_stid <- patients$patientunitstayid[k]
    folder_path<-file.path(root_path, cur_stid)
    fwrite(patients[k,c(35,37:40,17),drop=F], file=file.path(folder_path, "y_static.csv"), row.names=F)
    if(k %% 1000 == 0) print(i)
}


# patients[k,c(35,37:40,17),drop=F]