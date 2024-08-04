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

patients<-fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/patients.csv.gz",header=T,fill=T)
admissions<-fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/admissions.csv.gz",header=T,fill=T)
icustays<-fread("/home/luojiawei/mimiciv/mimic-iv-2.2/icu/icustays.csv.gz",header=T,fill=T)

icustays <- icustays %>% left_join(admissions[,c("subject_id","hadm_id","admittime","dischtime","deathtime",
                                             "admission_type","hospital_expire_flag","race")], by=c("subject_id","hadm_id"))
icustays <- icustays %>% left_join(patients[,c("subject_id","gender","anchor_age")], by = "subject_id")
dim(icustays)
# [1] 73181    16
names(icustays)
length(unique(icustays$subject_id))

as.data.frame(icustays[1:2,])
range(icustays$anchor_age)

icustays <- icustays %>%
  mutate(intime = ymd_hms(intime), outtime = ymd_hms(outtime),  # 转换时间格式
         icu_los = as.numeric(difftime(outtime, intime, units = "hours"))) %>%  # 计算 ICU 住院时间
  filter(icu_los >= 12 & icu_los <= 30 * 24)

dim(icustays)
# [1] 69700    17

icustays <- icustays %>%
    arrange(subject_id, hadm_id, intime) %>%  # 按照 subject_id 和 hadm_id 排序
    group_by(subject_id, hadm_id) %>%  # 按照 subject_id 和 hadm_id 分组
    mutate(reentry_within_30_days = ifelse(
        as.numeric(difftime(lead(intime), outtime, units = "days")) <= 30, 1, 0)) %>%  # 计算是否在30天内再次进入 ICU
    mutate(reentry_within_30_days = ifelse(is.na(lead(intime)), 0, reentry_within_30_days)) %>%  # 最后一次记为 0
    ungroup()  # 取消分组

as.data.frame(icustays[1:2,])
names(icustays)
# apply(icustays[,c(21:24,13)], 2, mean)
table(icustays$reentry_within_30_days)

icustays$icu_los_1d <- ifelse(icustays$icu_los > 24, 1, 0)

icustays <- icustays %>%
  mutate(
    death_1d = ifelse(!is.na(deathtime) & as.numeric(difftime(deathtime, intime, units = "days")) <= 1, 1, 0),
    death_2d = ifelse(!is.na(deathtime) & as.numeric(difftime(deathtime, intime, units = "days")) <= 2, 1, 0),
    death_4d = ifelse(!is.na(deathtime) & as.numeric(difftime(deathtime, intime, units = "days")) <= 4, 1, 0),
    death_7d = ifelse(!is.na(deathtime) & as.numeric(difftime(deathtime, intime, units = "days")) <= 7, 1, 0)
  )

icustays <- icustays %>% filter(anchor_age >= 18 & anchor_age <= 80 )
dim(icustays)

# -------- 字典读取 -------------

d_s <- read_excel("./var_dict_mimic.xlsx", sheet=1, col_names=T)
d_d <- read_excel("./var_dict_mimic.xlsx", sheet=3, col_names=T)

# -------- MIMIC-CODE database connection -------------

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "mimiciv", 
                 host = "127.0.0.1", port = 5432, 
                 user = "ljw", 
                 password = "123456")
schema_name <- "mimiciv_derived"

# Blood Gas -------------------------

table_name <- "bg"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
bg <- dbGetQuery(con, query)
names(bg)
bg[1:2,]
d_bg <- d_d[d_d$item_id %in% names(bg)[c(5:8,10:22,24,26)],]
as.data.frame(d_bg)
stat_bg <- get_stat_wide(bg, d_bg$item_id, d_bg$value_type, d_bg$cont)

# Uring Output -------------------------

table_name <- "urine_output"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
uo <- dbGetQuery(con, query)
names(uo)[3] <- "uo"
d_uo <- d_d[d_d$item_id %in% names(uo)[c(3)],,drop=F]
uo[1:3,]
uo <- remove_extreme_value_wide(uo, d_uo$item_id, d_uo$value_type, 1:2, d_uo)
stat_uo <- get_stat_wide(uo, d_uo$item_id, d_uo$value_type, d_uo$cont)

# ventilation -------------------------

table_name <- "ventilation"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vent <- dbGetQuery(con, query)
vent[1:2,]
names(vent)[c(4)] <- "vent"
vent$vent <- ifelse(vent$vent == "None", 0, 1)
d_vent <- d_d[d_d$item_id %in% names(vent)[c(4)],,drop=F]
stat_vent <- get_stat_wide(vent, d_vent$item_id, d_vent$value_type, d_vent$cont)

# GCS ---------------------------------

table_name <- "gcs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
gcs <- dbGetQuery(con, query)

d_gcs <- d_d[d_d$item_id %in% names(gcs)[c(4)],,drop=F]
stat_gcs <- get_stat_wide(gcs, d_gcs$item_id, d_gcs$value_type, d_gcs$cont)

# Vital signs ---------------------------------

table_name <- "vitalsign"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vit <- dbGetQuery(con, query)
names(vit)
vit[1:2,]
d_vit <- d_d[d_d$item_id %in% names(vit)[c(4:7,11,12,14,15)],,drop=F]
stat_vit <- get_stat_wide(vit, d_vit$item_id, d_vit$value_type, d_vit$cont)


# 用药 ------------------------------------
table_name <- "vasoactive_agent"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vaso <- dbGetQuery(con, query)

vaso[1:3,]
vaso[,c(4:10)] <- lapply(vaso[,c(4:10)], function(x) ifelse(x > 0, 1, 0))
names(vaso)
d_vaso <- d_d[d_d$item_id %in% names(vaso)[c(4:10)],,drop=F]
stat_vaso <- get_stat_wide(vaso, d_vaso$item_id, d_vaso$value_type, d_vaso$cont)

# 全血细胞计数 ------------------------------------
table_name <- "complete_blood_count"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
cbc <- dbGetQuery(con, query)

cbc[1:3,]
names(cbc)
d_cbc <- d_d[d_d$item_id %in% names(cbc)[c(7:14)],,drop=F]
stat_cbc <- get_stat_wide(cbc, d_cbc$item_id, d_cbc$value_type, d_cbc$cont)

# 生化检测 ------------------------------------
table_name <- "chemistry"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
chem <- dbGetQuery(con, query)

chem[1:3,]
names(chem)
d_chem <- d_d[d_d$item_id %in% names(chem)[c(5:8,10,13,15)],,drop=F]
stat_chem <- get_stat_wide(chem, d_chem$item_id, d_chem$value_type, d_chem$cont)

# 关闭连接
dbDisconnect(con)

get_data <- function(k) {

    # k <- 31
    cur_sid <- icustays$subject_id[k]
    cur_adid <- icustays$hadm_id[k]
    cur_stid <- icustays$stay_id[k]

    t1 <- 0
    t2 <- ceiling(as.numeric(difftime(icustays$outtime[k], icustays$intime[k], unit="hours")))
    t_list <- seq(t1,t2,4)
    y_mat <- list()
    y_mat[[1]] <- cbind("time" = t_list)
    if(!is.na(icustays$deathtime[k])){
        t_d <- ceiling(as.numeric(difftime(icustays$deathtime[k], icustays$intime[k], unit="hours")))
        y_mat[[2]] <- cbind("24h_death" = ifelse(t_d - seq(t1,t2,4) < 24, 1, 0))
    } else{
        y_mat[[2]] <- cbind("24h_death" = rep(0, length(seq(t1,t2,4))))
    }

    y_mat[[3]] <- cbind("icu_los"=ifelse(t2 - seq(t1,t2,4) <= 12, 1, 0))
    y_mat <- do.call(cbind, y_mat)
    y_mat <- y_mat %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

    # names(icustays)
    y_static <- icustays[k,c(19:23,13),drop=F]
    
    bg_k <- bg[which(bg$hadm_id == cur_adid), ,drop=F]
    bg_k$time1 <- as.numeric(difftime(bg_k$charttime, icustays$intime[k], unit="hours"))
    # as.data.frame(d_bg)
    bg_k1 <- resample_point_wide(bg_k,
                                d_bg$item_id,
                                d_bg$value_type,
                                d_bg$agg_fun, 
                                seq(t1, t2, 1),
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

    uo_k <- uo[which(uo$stay_id == cur_stid), ,drop=F]
    uo_k$time1 <- as.numeric(difftime(uo_k$charttime, icustays$intime[k], unit="hours"))

    uo_k1 <- resample_point_wide(uo_k,
                                d_uo$item_id,
                                d_uo$value_type,
                                d_uo$agg_fun, 
                                seq(t1, t2, 1),
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

    vent_k <- vent[which(vent$stay_id == cur_stid), ,drop=F]
    vent_k$time1 <- as.numeric(difftime(vent_k$starttime, icustays$intime[k], unit="hours"))
    vent_k$time2 <- as.numeric(difftime(vent_k$endtime, icustays$intime[k], unit="hours"))

    vent_k1 <- resample_interval_wide(vent_k,
                                d_vent$item_id,
                                d_vent$value_type,
                                d_vent$agg_fun, 
                                seq(t1, t2, 1),
                                "time1",
                                "time2",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_vent <- vent_k1[,2] == "1"
    vent_k1 <- vent_k1[,c(1,3:ncol(vent_k1))]
    m_vent <- get_mask(vent_k1,2:ncol(vent_k1), 1)
    vent_k1 <- fill(vent_k1, 2:ncol(vent_k1), 1, d_vent$value_type, d_vent$fill1, d_vent$fill2, stat_vent)
    vent_k1 <- fill_last_values(vent_k1, m_vent, 2:ncol(vent_k1), 1, d_vent)
    # vent_k1 <- to_onehot(vent_k1, 2:ncol(vent_k1), 1, d_vent$value_type, stat_vent)
    # vent_k1[1:2,]

    gcs_k <- gcs[which(gcs$stay_id == cur_stid), ,drop=F]
    gcs_k$time1 <- as.numeric(difftime(gcs_k$charttime, icustays$intime[k], unit="hours"))
    gcs_k1 <- resample_point_wide(gcs_k,
                                d_gcs$item_id,
                                d_gcs$value_type,
                                d_gcs$agg_fun, 
                                seq(t1, t2, 1),
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

    vit_k <- vit[which(vit$stay_id == cur_stid), ,drop=F]
    vit_k$time1 <- as.numeric(difftime(vit_k$charttime, icustays$intime[k], unit="hours"))
    vit_k1 <- resample_point_wide(vit_k,
                                d_vit$item_id,
                                d_vit$value_type,
                                d_vit$agg_fun, 
                                seq(t1, t2, 1),
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

    vaso_k <- vaso[which(vaso$stay_id == cur_stid), ,drop=F]
    vaso_k$time1 <- as.numeric(difftime(vaso_k$starttime, icustays$intime[k], unit="hours"))
    vaso_k$time2 <- as.numeric(difftime(vaso_k$endtime, icustays$intime[k], unit="hours"))

    vaso_k1 <- resample_interval_wide(vaso_k,
                                d_vaso$item_id,
                                d_vaso$value_type,
                                d_vaso$agg_fun, 
                                seq(t1, t2, 1),
                                "time1",
                                "time2",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_vaso <- vaso_k1[,2] == "1"
    vaso_k1 <- vaso_k1[,c(1,3:ncol(vaso_k1))]
    m_vaso <- get_mask(vaso_k1,2:ncol(vaso_k1), 1)
    vaso_k1 <- fill(vaso_k1, 2:ncol(vaso_k1), 1, d_vaso$value_type, d_vaso$fill1, d_vaso$fill2, stat_vaso)
    vaso_k1 <- fill_last_values(vaso_k1, m_vaso, 2:ncol(vaso_k1), 1, d_vaso)
    # vaso_k1[1:2,]

    cbc_k <- cbc[which(cbc$subject_id == cur_sid), ,drop=F]
    cbc_k$time1 <- as.numeric(difftime(cbc_k$charttime, icustays$intime[k], unit="hours"))
    cbc_k1 <- resample_point_wide(cbc_k,
                                d_cbc$item_id,
                                d_cbc$value_type,
                                d_cbc$agg_fun, 
                                seq(t1, t2, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_cbc <- cbc_k1[,2] == "1"
    cbc_k1 <- cbc_k1[,c(1,3:ncol(cbc_k1))]
    m_cbc <- get_mask(cbc_k1,2:ncol(cbc_k1), 1)
    cbc_k1 <- fill(cbc_k1, 2:ncol(cbc_k1), 1, d_cbc$value_type, d_cbc$fill1, d_cbc$fill2, stat_cbc)
    cbc_k1 <- fill_last_values(cbc_k1, m_cbc, 2:ncol(cbc_k1), 1, d_cbc)
    # cbc_k1[1:10,]

    chem_k <- chem[which(chem$subject_id == cur_sid), ,drop=F]
    chem_k$time1 <- as.numeric(difftime(chem_k$charttime, icustays$intime[k], unit="hours"))
    chem_k1 <- resample_point_wide(chem_k,
                                d_chem$item_id,
                                d_chem$value_type,
                                d_chem$agg_fun, 
                                seq(t1, t2, 1),
                                "time1",
                                1,
                                "both",
                                keepNArow = T,
                                keep_first = T)
    ind_chem <- chem_k1[,2] == "1"
    chem_k1 <- chem_k1[,c(1,3:ncol(chem_k1))]
    m_chem <- get_mask(chem_k1,2:ncol(chem_k1), 1)
    chem_k1 <- fill(chem_k1, 2:ncol(chem_k1), 1, d_chem$value_type, d_chem$fill1, d_chem$fill2, stat_chem)
    chem_k1 <- fill_last_values(chem_k1, m_chem, 2:ncol(chem_k1), 1, d_chem)
    # chem_k1[1:10,]

    ind <- which(ind_bg | ind_vit | ind_gcs | ind_uo | ind_vaso | 
                    ind_vent | ind_cbc | ind_chem)
    ds_k_ <- list(vit_k1, bg_k1[,2:ncol(bg_k1),drop=F],
                         gcs_k1[,2:ncol(gcs_k1),drop=F], 
                         uo_k1[,2:ncol(uo_k1),drop=F], 
                         vaso_k1[,2:ncol(vaso_k1),drop=F], 
                         vent_k1[,2:ncol(vent_k1),drop=F],
                         cbc_k1[,2:ncol(cbc_k1),drop=F],
                         chem_k1[,2:ncol(chem_k1),drop=F]) %>% 
                            do.call(cbind, .) %>% 
                            convert_to_numeric_df
    m_k <- list(m_vit, m_bg[,2:ncol(m_bg),drop=F],
                         m_gcs[,2:ncol(m_gcs),drop=F], 
                         m_uo[,2:ncol(m_uo),drop=F], 
                         m_vaso[,2:ncol(m_vaso),drop=F], 
                         m_vent[,2:ncol(m_vent),drop=F],
                         m_cbc[,2:ncol(m_cbc),drop=F],
                         m_chem[,2:ncol(m_chem),drop=F]) %>% 
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
    # m_k[1:2,]
    return(list(ds_k_, m_k, y_mat, y_static))
}

datas <- get_data(11)

datas[[1]][1:2,]
datas[[2]][1:2,]
datas[[3]]

process_data <- function(k, root_path) {

    # k<-441
    cur_stid <- icustays$stay_id[k]
    folder_path<-file.path(root_path, cur_stid)
    create_dir(folder_path, F)
    
    datas <- get_data(k)

    fwrite(datas[[1]], file=file.path(folder_path, "dynamic.csv"), row.names=F)
    fwrite(datas[[2]], file=file.path(folder_path, "mask.csv"), row.names=F)
    fwrite(datas[[3]], file=file.path(folder_path, "y_mat.csv"), row.names=F)
    fwrite(datas[[4]], file=file.path(folder_path, "y_static.csv"), row.names=F)
}

root_path <- "/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids_share"
create_dir(root_path, T)


chunk_size <- 3000
num_rows <- nrow(icustays)
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

icustays$stay_id[100]

# 假设 icustays 已经被加载和定义
set.seed(123)  # 设置随机种子以保证结果的可重复性


# names(icustays)
# icustays <- icustays[,-c(24)]

# 获取唯一的 subject_id
unique_subjects <- icustays %>% distinct(subject_id)

# 随机分配训练集、测试集和验证集
set.seed(123)  # 固定随机种子
unique_subjects <- unique_subjects %>%
  mutate(set = sample(c(1, 2, 3), size = n(), replace = TRUE, prob = c(0.7, 0.2, 0.1)))

# 将分配的集合标签加回到原始数据集
icustays <- icustays %>%
  left_join(unique_subjects, by = "subject_id")

as.data.frame(icustays[1:2,])

write.csv(icustays, file="/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv", row.names=F)

icustays <- fread("/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv")
round(apply(icustays[,c(20:23,13,19)], 2, mean) * 100,1)
names(icustays)
dim(icustays)
table(icustays$set)

# ===============================================

root_path <- "/home/luojiawei/EMR_LIP_data/mimic_iv/all_stids"
names(icustays)
dim(icustays)
for(k in 1:nrow(icustays)){
    # k <- 542
    # which(icustays$stay_id == 37258034)
    cur_stid <- icustays$stay_id[k]
    folder_path<-file.path(root_path, cur_stid)
    fwrite(icustays[k,c(18,20:24,13),drop=F], file=file.path(folder_path, "y_static.csv"), row.names=F)
    if(k %% 1000 == 0) print(i)
}


# icustays[k,c(18,20:24,13),drop=F]


