setwd("/home/luojiawei/Benchmark_project")

library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)

source("./EMR_APIs.R")

# ------- 样本筛选 ------------------

patients<-fread("/home/luojiawei/eicu/eicu-collaborative-research-database-2.0/patient.csv.gz",header=T,fill=T)

as.data.frame(patients[1:2,])

length(unique(patients$patientunitstayid))
length(unique(patients$patienthealthsystemstayid))
length(unique(patients$uniquepid))

range(patients$unitvisitnumber)

unique(patients$unittype)

# hospital <- fread("/home/luojiawei/eicu/eicu-collaborative-research-database-2.0/hospital.csv.gz",header=T, fill=T)
# hospital[1:2,]
# rm("hospital")

# 保留只有一次 patientunitstayid 记录的样本，并统计排除的人数
patients_grouped <- patients %>%
  group_by(patienthealthsystemstayid)

single_stay_patients <- patients_grouped %>%
  filter(n() == 1) %>%
  ungroup()

length(unique(single_stay_patients$patienthealthsystemstayid))

# 删除年龄在18岁以下的样本，并统计排除的人数
adult_patients <- single_stay_patients %>%
  filter(age >= 18)

length(unique(adult_patients$patienthealthsystemstayid))

# 更新 patients 数据集
patients <- adult_patients
length(unique(patients$patienthealthsystemstayid))

# 转换 hospitaladmitoffset 和 hospitaldischargeoffset 为小时
patients <- patients %>%
  mutate(hospitaladmitoffset_hours = hospitaladmitoffset / 60,
         hospitaldischargeoffset_hours = hospitaldischargeoffset / 60)

# 计算在院时间（以天为单位）
patients <- patients %>%
  mutate(stay_duration_days = (hospitaldischargeoffset_hours - hospitaladmitoffset_hours) / 24)

length(unique(patients$patienthealthsystemstayid))
# 保留在院时间在 [1,100] 天区间内的 patienthealthsystemstayid
patients <- patients %>%
  filter(stay_duration_days >= 1 & stay_duration_days <= 100)
length(unique(patients$patienthealthsystemstayid))

length(unique(patients$patienthealthsystemstayid))
patients <- patients[which(patients$unitdischargestatus %in% c("Alive","Expired")),]
patients$unitdischargestatus <- ifelse(patients$unitdischargestatus == "Alive", 0, 1)
table(patients$unitdischargestatus)
length(unique(patients$patienthealthsystemstayid))


patients <- patients[which(!(patients$gender %in% c("","Unknown","Other"))),]

length(unique(patients$patienthealthsystemstayid))

patients$hospitaldischargeoffset_r <- (patients$hospitaldischargeoffset - patients$hospitaladmitoffset) / 60

patients$los_day_7 <- ifelse(patients$stay_duration_days > 7, 1, 0)
# table(patients$los_day_7)
fwrite(patients, file="/home/luojiawei/Benchmark_project_data/eicu_data/patients.csv",row.names = F)

object_tosave <- c()


# ------  静态数据的处理和字典的生成 ---------------

as.data.frame(patients[1:2,])
dim(patients)

names(patients)

cols_static <- names(patients)[c(3,4)] # 这行要重新选择索引, 要确定数字索引的正确性
type_list_static <- c("cat","num")
stat_static <- get_stat_wide(patients, cols_static, type_list_static)

object_tosave <- c(object_tosave, "cols_static","type_list_static","stat_static")

names(patients)
length(unique(patients$uniquepid))
length(unique(patients$patienthealthsystemstayid))

# -------- 纵向数据读取 -------------

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "eicu", 
                 host = "127.0.0.1", port = 5432, 
                 user = "ljw", 
                 password = "123456")
schema_name <- "eicu_crd"

table_name <- "pivoted_bg"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
bg <- dbGetQuery(con, query)
bg <- merge(bg, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
bg <- bg %>% filter(patienthealthsystemstayid %in% unique(patients$patienthealthsystemstayid),
                    chartoffset >= hospitaladmitoffset) %>%
                    mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60)
bg[1:2,]
names(bg)
names(bg)[c(3:10)] <- paste0(names(bg)[c(3:10)],"_bg")
cols_bg<-names(bg)[c(3:10)]
fillf_bg <- rep("locf", length(cols_bg))
fillf1_bg <- rep("mean", length(cols_bg))
aggf_bg <- rep("median", length(cols_bg))
stat_bg <- get_stat_wide(bg, cols_bg, type_list = rep("num",length(cols_bg)))
get_type(stat_bg)
bg <- remove_extreme_value_wide(bg, cols_bg, get_type(stat_bg), names(bg)[c(11,2,13)])

bg[1:4,]

object_tosave <- c(object_tosave, "cols_bg","fillf_bg","fillf1_bg",
                  "stat_bg")


table_name <- "pivoted_lab"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
lab <- dbGetQuery(con, query)
lab <- merge(lab, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
lab <- lab %>% filter(patienthealthsystemstayid %in% patients$patienthealthsystemstayid,
                    chartoffset >= hospitaladmitoffset) %>% 
                    mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60)
lab[1:2,]
names(lab)
names(lab)[c(3:24)] <- paste0(names(lab)[c(3:24)], "_lab")
cols_lab<-names(lab)[c(3:24)]
fillf_lab <- rep("locf", length(cols_lab))
fillf1_lab <- rep("mean", length(cols_lab))
aggf_lab <- rep("median", length(cols_lab))
stat_lab <- get_stat_wide(lab,cols_lab, rep("num", length(cols_lab)))
get_type(stat_lab)
lab <- remove_extreme_value_wide(lab, cols_lab, get_type(stat_lab),
                                        names(lab)[c(25,2,27)])
lab[1:2,]

object_tosave <- c(object_tosave, "cols_lab","fillf_lab","fillf1_lab",
                  "stat_lab")


table_name <- "pivoted_vital"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vitalsign <- dbGetQuery(con, query)
vitalsign <- merge(vitalsign, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
vitalsign <- vitalsign %>% filter(patienthealthsystemstayid %in% patients$patienthealthsystemstayid,
                            chartoffset >= hospitaladmitoffset) %>% 
                            mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60)
vitalsign[1:4,]
names(vitalsign)
names(vitalsign)[c(4:10,12:14)] <- paste0(names(vitalsign)[c(4:10,12:14)], "_vit")

cols_vit <- names(vitalsign)[c(4:10,12:14)]
fillf_vit <- rep("locf", length(cols_vit))
fillf1_vit <- rep("mean", length(cols_vit))
aggf_vit <- rep("median", length(cols_vit))
stat_vital <- get_stat_wide(vitalsign, cols_vit, rep("num", length(cols_vit)))
vitalsign <- remove_extreme_value_wide(vitalsign, cols_vit, get_type(stat_vital), cols_keep = names(vitalsign)[c(15,2,17)])
get_type(stat_vital)

object_tosave <- c(object_tosave, "cols_vit","fillf_vit","fillf1_vit",
                  "stat_vital")


table_name <- "pivoted_med"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
med <- dbGetQuery(con, query)
med <- merge(med, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
med <- med %>% filter(patienthealthsystemstayid %in% patients$patienthealthsystemstayid,
                      chartoffset >= hospitaladmitoffset) %>% 
                      mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60,
                             drugstopoffset_r = (drugstopoffset - hospitaladmitoffset) / 60)
med[1:4,]
names(med)
names(med)[c(5:13)] <- paste0(names(med)[c(5:13)], "_med")

cols_med <- names(med)[c(5:13)]
fillf_med <- rep("zero", length(cols_med))
fillf1_med <- rep("zero", length(cols_med))
aggf_med <- rep("mean", length(cols_med))
stat_med <- get_stat_wide(med, cols_med, rep("bin", length(cols_med)))
med <- remove_extreme_value_wide(med, cols_med, get_type(stat_med), cols_keep = names(med)[c(14,3,4,16,17)])
get_type(stat_med)

object_tosave <- c(object_tosave, "cols_med","fillf_med","fillf1_med",
                  "stat_med")


table_name <- "pivoted_uo"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
uo <- dbGetQuery(con, query)
uo <- merge(uo, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
uo <- uo %>% filter(patienthealthsystemstayid %in% patients$patienthealthsystemstayid,
                      chartoffset >= hospitaladmitoffset) %>% 
                      mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60)
uo[1:4,]
uo$outputtotal[uo$outputtotal < 0] <- (-uo$outputtotal[uo$outputtotal < 0])
names(uo)
names(uo)[c(3)] <- paste0(names(uo)[c(3)], "_uo")

cols_uo <- names(uo)[c(3)]
fillf_uo <- rep("zero", length(cols_uo))
fillf1_uo <- rep("zero", length(cols_uo))
aggf_uo <- rep("sum", length(cols_uo))
stat_uo <- get_stat_wide(uo, cols_uo, rep("num", length(cols_uo)))
uo <- remove_extreme_value_wide(uo, cols_uo, get_type(stat_uo), cols_keep = names(uo)[c(5,2,7)])
get_type(stat_uo)

object_tosave <- c(object_tosave, "cols_uo","fillf_uo","fillf1_uo",
                  "stat_uo")


table_name <- "pivoted_o2"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
oxy <- dbGetQuery(con, query)
oxy <- merge(oxy, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
oxy <- oxy %>% filter(patienthealthsystemstayid %in% patients$patienthealthsystemstayid,
                      chartoffset >= hospitaladmitoffset) %>% 
                      mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60)
oxy[1:4,]
names(oxy)
names(oxy)[c(4)] <- paste0(names(oxy)[c(4)], "_oxy")

cols_oxy <- names(oxy)[c(4)]
fillf_oxy <- rep("zero", length(cols_oxy))
fillf1_oxy <- rep("zero", length(cols_oxy))
aggf_oxy <- rep("sum", length(cols_oxy))
stat_oxy <- get_stat_wide(oxy, cols_oxy, rep("num", length(cols_oxy)))
oxy <- remove_extreme_value_wide(oxy, cols_oxy, get_type(stat_oxy), cols_keep = names(oxy)[c(7,2,9)])
get_type(stat_oxy)


object_tosave <- c(object_tosave, "cols_oxy","fillf_oxy","fillf1_oxy",
                  "stat_oxy")


table_name <- "gcs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
gcs <- dbGetQuery(con, query)
gcs <- merge(gcs, patients[,c("patientunitstayid", "patienthealthsystemstayid",
                            "hospitaladmitoffset")], by="patientunitstayid")
gcs <- gcs %>% filter(patienthealthsystemstayid %in% patients$patienthealthsystemstayid,
                      chartoffset >= hospitaladmitoffset) %>% 
                      mutate(chartoffset_r = (chartoffset - hospitaladmitoffset)/60)
gcs[1:4,]
names(gcs)
names(gcs)[c(3)] <- paste0(names(gcs)[c(3)], "_gcs")

cols_gcs <- names(gcs)[c(3)]
fillf_gcs <- rep("locf", length(cols_gcs))
fillf1_gcs <- rep("mean", length(cols_gcs))
aggf_gcs <- rep("median", length(cols_gcs))
stat_gcs <- get_stat_wide(gcs, cols_gcs, rep("ord", length(cols_gcs)))
gcs <- remove_extreme_value_wide(gcs, cols_gcs, get_type(stat_gcs), cols_keep = names(gcs)[c(7,2,9)])
get_type(stat_gcs)

object_tosave <- c(object_tosave, "cols_gcs","fillf_gcs","fillf1_gcs",
                  "stat_gcs")


save(list=object_tosave, file="/home/luojiawei/Benchmark_project_data/eicu_data/meta_info.RData")

# 关闭连接
dbDisconnect(con)

length(unique(patients$patienthealthsystemstayid))
all_hadmid <- unique(patients$patienthealthsystemstayid)
length(all_hadmid)



get_1hadmid_allAlign <- function(k) {
  # k<-42
  id_k <- all_hadmid[k]
  patients_k <- patients[patients$patienthealthsystemstayid==id_k,,drop=F]
  static <- patients_k[, c(cols_static),drop=F]

  static <- norm_num(static, 1:ncol(static), NULL, get_type(stat_static), stat_static)
  static <- to_onehot(static, 1:ncol(static), NULL, get_type(stat_static), stat_static)
  static <- as.data.frame(static)
  static <- lapply(static, as.numeric) %>% as.data.frame

  twd<-1
  time_range <- seq(0, floor(patients_k$hospitaldischargeoffset_r[1]), twd)

  # ---------  实验室检查 --------------
  cols_1 <- c(cols_bg, cols_lab)
  stat_1 <- c(stat_bg, stat_lab)
  fillfun_list_1 <- c(fillf_bg, fillf_lab)
  allNAfillfun_list_1 <- c(fillf1_bg, fillf1_lab)

  bg_k <- bg[bg$patienthealthsystemstayid == id_k, ]
  bg_k$timecol2 <- rep(NA, nrow(bg_k))
  bg_k1 <- resample_data_wide(bg_k, 
                              cols_bg,
                              get_type(stat_bg),
							  aggf_bg,
                              time_range, 
                              time_col1 = "chartoffset_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)
  
  lab_k <- lab[lab$hadm_id == id_k, ]
  lab_k$timecol2 <- rep(NA, nrow(lab_k))
  lab_k1 <- resample_data_wide(lab_k, 
                                cols_lab,
                                get_type(stat_lab),
								aggf_lab,
                                time_range, 
                                time_col1 = "chartoffset_r", 
                                time_col2 = "timecol2",
                                time_window = 1,
                                keepNArow=T)

  # ---------  生命体征 --------------
  stat_2 <- c(stat_vital, stat_gcs, stat_uo) 
  cols_2 <- c(cols_vit, cols_gcs, cols_uo)
  fillfun_list_2 <- c(fillf_vit, fillf_gcs, fillf_uo)
  allNAfillfun_list_2 <- c(fillf1_vit, fillf1_gcs, fillf1_uo)

  vitalsign_k <- vitalsign[vitalsign$patienthealthsystemstayid == id_k,]
  vitalsign_k$timecol2 <- rep(NA, nrow(vitalsign_k))
  vitalsign_k1 <- resample_data_wide(vitalsign_k, 
                                     cols_vit,
                                     get_type(stat_vital),
                                      aggf_vit, 
                                     time_range, 
                                     time_col1 = "chartoffset_r", 
                                     time_col2 = "timecol2",
                                     time_window = 1,
                                     keepNArow = T)

  gcs_k <- gcs[gcs$patienthealthsystemstayid == id_k,]
  gcs_k$timecol2 <- rep(NA, nrow(gcs_k))
  gcs_k1 <- resample_data_wide(gcs_k, 
                              cols_gcs,
                              get_type(stat_gcs),
							  aggf_gcs,
                              time_range, 
                              time_col1 = "chartoffset_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow = T)

  uo_k <- uo[uo$patienthealthsystemstayid == id_k,]
  uo_k$timecol2 <- rep(NA, nrow(uo_k))
  uo_k1 <- resample_data_wide(uo_k, 
                              cols_uo,
                              get_type(stat_uo),
							  aggf_uo,
                              time_range, 
                              time_col1 = "chartoffset_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow = T)

  # -------- 药物治疗, 机械通气 ------------
  
  cols_3 <- c(cols_med, cols_oxy)
  stat_3 <- c(stat_med, stat_oxy)
  fillfun_list_3 <- c(fillf_med, fillf_oxy)
  allNAfillfun_list_3 <- c(fillf1_med, fillf1_oxy)

  med_k <- med[med$patienthealthsystemstayid == id_k,]
  med_k1 <- resample_process_wide(med_k, 
                              cols_med,
                              get_type(stat_med),
							  aggf_med,
                              time_range, 
                              time_col1 = "chartoffset_r", 
                              time_col2 = "drugstopoffset_r",
                              time_window = 1,
                              keepNArow=T)

  oxy_k <- oxy[oxy$patienthealthsystemstayid == id_k,]
  oxy_k$timecol2 <- rep(NA, nrow(oxy_k))
  oxy_k1 <- resample_data_wide(oxy_k,
                              cols_oxy,
                              get_type(stat_oxy),
							  aggf_oxy,
                              time_range, 
                              time_col1 = "chartoffset_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  ind_all <- which(bg_k1[,2]=="1" | lab_k1[,2]=="1" | 
            vitalsign_k1[,2] == "1" | gcs_k1[,2] == "1" | uo_k1[,1] == "1" |
            med_k1[,2] == "1" | oxy_k1[,2] == "1")

  X_1 <- cbind(bg_k1[,c(1,3:ncol(bg_k1)),drop=F], 
                lab_k1[,c(3:ncol(lab_k1)),drop=F])
  X_1 <- X_1[ind_all,,drop=F]
  mask_1 <- get_mask(X_1, 2:ncol(X_1), 1)
  delta_1 <- get_deltamat(mask_1, 2:ncol(X_1), 1)
  X_1 <- fill(X_1, 2:ncol(X_1), 1, get_type(stat_1), fillfun_list_1, allNAfillfun_list_1, stat_1)
  X_1 <- norm_num(X_1, 2:ncol(X_1), 1, get_type(stat_1), stat_1)
  X_1 <- to_onehot(X_1, 2:ncol(X_1), 1, get_type(stat_1), stat_1)
  mask_1 <- shape_as_onehot(mask_1, 2:ncol(mask_1), 1, get_type(stat_1), stat_1)
  delta_1 <- shape_as_onehot(delta_1, 2:ncol(delta_1), 1, get_type(stat_1), stat_1)

  X_1 <- as.data.frame(X_1)
  X_1 <- lapply(X_1, as.numeric) %>% as.data.frame
  mask_1 <- as.data.frame(mask_1)
  mask_1 <- lapply(mask_1, as.numeric) %>% as.data.frame
  delta_1 <- as.data.frame(delta_1)
  delta_1 <- lapply(delta_1, as.numeric) %>% as.data.frame

  X_2 <- cbind(vitalsign_k1[,c(1, 3:ncol(vitalsign_k1)),drop=F], 
                gcs_k1[,3:ncol(gcs_k1),drop=F], 
                uo_k1[,3:ncol(uo_k1),drop=F])
  X_2 <- X_2[ind_all,,drop=F]
  mask_2 <- get_mask(X_2, 2:ncol(X_2), 1)
  delta_2 <- get_deltamat(mask_2, 2:ncol(X_2), 1)
  X_2 <- fill(X_2, 2:ncol(X_2), 1, get_type(stat_2), fillfun_list_2, allNAfillfun_list_2, stat_2)
  X_2 <- norm_num(X_2, 2:ncol(X_2), 1, get_type(stat_2), stat_2)
  X_2 <- to_onehot(X_2, 2:ncol(X_2), 1, get_type(stat_2), stat_2)
  mask_2 <- shape_as_onehot(mask_2, 2:ncol(mask_2), 1, get_type(stat_2), stat_2)
  delta_2 <- shape_as_onehot(delta_2, 2:ncol(delta_2), 1, get_type(stat_2), stat_2)

  X_2 <- as.data.frame(X_2)
  X_2 <- lapply(X_2, as.numeric) %>% as.data.frame
  mask_2 <- as.data.frame(mask_2)
  mask_2 <- lapply(mask_2, as.numeric) %>% as.data.frame
  delta_2 <- as.data.frame(delta_2)
  delta_2 <- lapply(delta_2, as.numeric) %>% as.data.frame
  
  X_3 <- cbind(med_k1[,c(1, 3:ncol(med_k1)),drop=F], 
              oxy_k1[,3:ncol(oxy_k1),drop=F])
  X_3 <- X_3[ind_all,,drop=F]
  mask_3 <- get_mask(X_3, 2:ncol(X_3), 1)
  delta_3 <- get_deltamat(mask_3, 2:ncol(X_3), 1)
  X_3 <- fill(X_3, 2:ncol(X_3), 1, get_type(stat_3), fillfun_list_3, allNAfillfun_list_3, stat_3)
  X_3 <- norm_num(X_3, 2:ncol(X_3), 1, get_type(stat_3), stat_3)
  X_3 <- to_onehot(X_3, 2:ncol(X_3), 1, get_type(stat_3), stat_3)
  mask_3 <- shape_as_onehot(mask_3, 2:ncol(mask_3), 1, get_type(stat_3), stat_3)
  delta_3 <- shape_as_onehot(delta_3, 2:ncol(delta_3), 1, get_type(stat_3), stat_3)

  X_3 <- as.data.frame(X_3)
  X_3 <- lapply(X_3, as.numeric) %>% as.data.frame
  mask_3 <- as.data.frame(mask_3)
  mask_3 <- lapply(mask_3, as.numeric) %>% as.data.frame
  delta_3 <- as.data.frame(delta_3)
  delta_3 <- lapply(delta_3, as.numeric) %>% as.data.frame

  return(list(static, X_1, mask_1, delta_1, X_2, mask_2, delta_2, X_3, mask_3, delta_3, patients_k))
  # return(list(patients_k))
}

process_data <- function(k, root_path) {

    # k<-8
    id_k<-all_hadmid[k]
    folder_path<-file.path(root_path, id_k)
    # if (!dir.exists(folder_path)) {
    #   dir.create(folder_path)
    # }
    create_dir(folder_path, F)
    
    datas <- get_1hadmid_allAlign(k)
    
    # datas[[2]][1:2,]
    # datas[[5]][1:2,]
    # datas[[8]][1:2,]

    fwrite(datas[[1]], file=file.path(folder_path, "static.csv"), row.names=F)

    fwrite(datas[[2]], file=file.path(folder_path, "lab_x.csv"), row.names=F)
    fwrite(datas[[3]], file=file.path(folder_path, "lab_m.csv"), row.names=F)
    fwrite(datas[[4]], file=file.path(folder_path, "lab_dt.csv"), row.names=F)

    fwrite(datas[[5]], file=file.path(folder_path, "vital_x.csv"), row.names=F)
    fwrite(datas[[6]], file=file.path(folder_path, "vital_m.csv"), row.names=F)
    fwrite(datas[[7]], file=file.path(folder_path, "vital_dt.csv"), row.names=F)

    fwrite(datas[[8]], file=file.path(folder_path, "trt_x.csv"), row.names=F)
    fwrite(datas[[9]], file=file.path(folder_path, "trt_m.csv"), row.names=F)
    fwrite(datas[[10]], file=file.path(folder_path, "trt_dt.csv"), row.names=F)

    fwrite(datas[[11]][,c("hospitaldischargeoffset_r","unitdischargestatus","los_day_7","stay_duration_days"),drop=F], 
           file=file.path(folder_path, "y.csv"), row.names=F)
}

names(patients)

root_path <- "/home/luojiawei/Benchmark_project_data/eicu_data/patient_folders_1/"
create_dir(root_path, T)
twd<-1 # 时间分辨率(hour)

# 定义每个块的大小
chunk_size <- 1000

# 计算块的数量
num_chunks <- ceiling(length(all_hadmid) / chunk_size)

# 对每个块进行处理
for (i in seq_len(num_chunks)) {
  # 计算当前块的索引
  # i<-2
  idx <- ((i - 1) * chunk_size + 1):min(i * chunk_size, length(all_hadmid))
  
  # 对当前块的数据进行处理
  results <- mclapply(idx, 
                      function(x) {
                        result <- tryCatch({
                          process_data(x, root_path = root_path)
                        }, error = function(e) {
                          print(e)
                          print(x)
                        })
                        if(x %% 200 == 0) print(x)
                        
                        return(result)
                      }, mc.cores = detectCores())
  
  # 保存结果（如果需要）
  
  # 触发垃圾回收
  gc()
}


#  -------- 数据集划分 -------------

as.data.frame(patients[1:2,])

library(caret)

# 设置种子以确保结果可重复
set.seed(123)

# 创建数据分区
partition <- createDataPartition(patients$patienthealthsystemstayid, p = 0.7, list = FALSE)

# 创建训练集
train_set <- patients[partition, ]

# 创建测试和验证集
remaining_set <- patients[-partition, ]

# 再次进行数据分区以在测试集和验证集之间进行划分
partition <- createDataPartition(remaining_set$patienthealthsystemstayid, p = 0.67, list = FALSE)

# 创建测试集
test_set <- remaining_set[partition, ]

# 创建验证集
valid_set <- remaining_set[-partition, ]

fwrite(train_set, file="/home/luojiawei/Benchmark_project_data/eicu_data/train_set.csv", row.names=F)
fwrite(test_set, file="/home/luojiawei/Benchmark_project_data/eicu_data/test_set.csv", row.names=F)
fwrite(valid_set, file="/home/luojiawei/Benchmark_project_data/eicu_data/valid_set.csv", row.names=F)


patients <- fread("/home/luojiawei/Benchmark_project_data/eicu_data/patients.csv", header=T)
patients[1:2,]
names(patients)

