setwd("/home/luojiawei/Benchmark_project")

library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)

normalize_type <- function(x) {
  return(ifelse(x %in% c("num","Numeric","Solution","Ingredient","Processes","Numeric with tag"), 
              "num", ifelse(x %in% c("cat","Text"), 
                  "cat", ifelse(x %in% c("ord"), "ord", ifelse(x %in% c("Checkbox", "Date and time"), "bin", NA)))))
}

source("./EMR_APIs.R")

# rm(list = ls())
# gc()


# ------- 样本筛选 ------------------

patients<-fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/patients.csv.gz",header=T,fill=T)
admissions<-fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/admissions.csv.gz",header=T,fill=T)
icustays<-fread("/home/luojiawei/mimiciv/mimic-iv-2.2/icu/icustays.csv.gz",header=T,fill=T)

icustays <- icustays %>% left_join(admissions[,c("subject_id","hadm_id","admittime","dischtime","deathtime",
                                             "admission_type","hospital_expire_flag","race")], by=c("subject_id","hadm_id"))
icustays <- icustays %>% left_join(patients[,c("subject_id","gender","anchor_age")], by = "subject_id")

names(icustays)

icustays[1:2,]
range(icustays$anchor_age)

length(unique(icustays$hadm_id))

# 识别 ICU transfers
icu_transfers <- icustays[icustays$first_careunit != icustays$last_careunit,]
print(paste("ICU transfers: ", length(unique(icu_transfers$hadm_id))))
icustays <- icustays[!(icustays$hadm_id %in% icu_transfers$hadm_id),]
length(unique(icustays$hadm_id))


# 识别 2+ ICU stays per admission
icu_stays_counts <- icustays[, .(count = .N), by = hadm_id]
multiple_icu_stays <- icu_stays_counts[count > 1]
print(paste("2+ ICU stays per admission: ", length(unique(multiple_icu_stays$hadm_id))))

icustays <- icustays[!(hadm_id %in% multiple_icu_stays$hadm_id),]
length(unique(icustays$hadm_id))

names(icustays)[names(icustays)=='los'] <- "los_icu"

admissions <- icustays

length(unique(admissions$hadm_id))

# 计算 los_day, 计算是否30天重新入院
admissions <- admissions %>%
  mutate(
    admittime = ymd_hms(admittime),
    dischtime = ymd_hms(dischtime),
    los_day = as.numeric(dischtime - admittime, units = "days")
  ) %>%
  arrange(subject_id, admittime) %>%
  group_by(subject_id) %>%
  mutate(
    next_admittime = lead(admittime),
    re_admission = if_else(!is.na(next_admittime) & as.numeric(next_admittime - dischtime, units = "days") <= 30, 1, 0)
  ) %>%
  mutate(re_admission = ifelse(is.na(re_admission), 0, re_admission))

length(unique(admissions$hadm_id))
# 选择在院时间在 [1,100] 天区间内的 hadm_id
admissions <- admissions %>%
  filter(los_day >= 1 & los_day <=100)
length(unique(admissions$hadm_id))

dim(admissions)
as.data.frame(admissions[1:2,])
table(admissions$re_admission)
table(admissions$hospital_expire_flag)
quantile(admissions$los_day)
names(admissions)

summary(admissions[,c("subject_id","hadm_id","admittime","dischtime")])

# 只保留第一次 admission 的记录
# admissions <- admissions %>%
#   arrange(subject_id, admittime) %>%
#   distinct(subject_id, .keep_all = TRUE)

dim(admissions)

admissions <- admissions[complete.cases(admissions[,c("subject_id","hadm_id","admittime","dischtime")]),]

object_tosave <- c()


# ------  静态数据的处理和字典的生成 --------------- 

# admissions <- admissions %>%
#   mutate(admission_type = case_when(
#     admission_type %in% c("EW EMER.", "DIRECT EMER.") ~ "EMERGENCY",
#     admission_type %in% c("OBSERVATION ADMIT", "EU OBSERVATION", "DIRECT OBSERVATION", "AMBULATORY OBSERVATION") ~ "OBSERVATION",
#     admission_type == "SURGICAL SAME DAY ADMISSION" ~ "ELECTIVE", 
#     TRUE ~ admission_type
#   ))

# admissions <- admissions %>%
#   mutate(marital_status = replace(marital_status, marital_status == "", "None"))

# admissions <- admissions %>%
#   mutate(race = case_when(
#     race %in% c("AMERICAN INDIAN/ALASKA NATIVE") ~ "OTHER",
#     str_detect(race, "ASIAN") ~ "ASIAN",
#     str_detect(race, "BLACK") ~ "BLACK",
#     str_detect(race, "HISPANIC") ~ "HISPANIC",
#     race %in% c("MULTIPLE RACE/ETHNICITY", "OTHER", "PATIENT DECLINED TO ANSWER", "UNABLE TO OBTAIN", "UNKNOWN") ~ "OTHER",
#     race %in% c("NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER") ~ "OTHER",
#     str_detect(race, "WHITE") ~ "WHITE",
#     TRUE ~ "OTHER"
#   ))

admissions <- admissions %>%
  mutate(
    deathtime_r = as.numeric(difftime(deathtime, admittime, units = "hours")),
    dischtime_r = as.numeric(difftime(dischtime, admittime, units = "hours"))
  )

admissions$los_day_7 <- ifelse(admissions$los_day > 7 , 1, 0)

fwrite(admissions, file="/home/luojiawei/Benchmark_project_data/mimiciv_data/admissions.csv",row.names = F)


dim(admissions)
as.data.frame(admissions[1:2,])

names(admissions)
cols_static <- names(admissions)[c(15,16)] # 这行要重新选择索引, 要确定数字索引的正确性
type_list_static <- c("cat","num")
stat_static <- get_stat_wide(admissions, cols_static, type_list_static)

object_tosave <- c(object_tosave, "cols_static","type_list_static","stat_static")

# -------- 纵向数据读取 -------------

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "mimiciv", 
                 host = "127.0.0.1", port = 5432, 
                 user = "ljw", 
                 password = "123456")
schema_name <- "mimiciv_derived"

table_name <- "sofa"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
sofa <- dbGetQuery(con, query)
sofa <- merge(sofa, icustays[,c("subject_id","hadm_id","stay_id","intime")], by=c("stay_id"), all.x=T)
sofa$charttime <- sofa$intime + hours(sofa$hr)
sofa <- sofa[sofa$hadm_id %in% unique(admissions$hadm_id),]
sofa <- merge(sofa, admissions[,c("hadm_id", "admittime")], by="hadm_id")
sofa <- sofa %>% filter(hadm_id %in% admissions$hadm_id,
                    charttime >= admittime) %>%
                    mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
sofa[1:2,]
names(sofa)
# names(sofa)[c(30)] <- paste0(names(sofa)[c(30)],"_sofa")
cols_sofa<-names(sofa)[c(30)]
fillf_sofa <- rep("locf", length(cols_sofa))
fillf1_sofa <- rep("mean", length(cols_sofa))
aggf_sofa <- rep("median", length(cols_sofa))
stat_sofa <- get_stat_wide(sofa, cols_sofa, type_list = rep("num",length(cols_sofa)))
get_type(stat_sofa)

object_tosave <- c(object_tosave, "cols_sofa","fillf_sofa","fillf1_sofa",
                  "stat_sofa")

table_name <- "bg"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
bg <- dbGetQuery(con, query)
bg <- merge(bg, admissions[,c("hadm_id", "admittime")], by="hadm_id")
bg <- bg %>% filter(hadm_id %in% admissions$hadm_id,
                    charttime >= admittime) %>%
                    mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
bg[1:2,]
names(bg)
names(bg)[c(5:8,11:27)] <- paste0(names(bg)[c(5:8,11:27)],"_bg")
cols_bg<-names(bg)[c(5:8,11:27)]
fillf_bg <- rep("locf", length(cols_bg))
fillf1_bg <- rep("mean", length(cols_bg))
aggf_bg <- rep("median", length(cols_bg))
stat_bg <- get_stat_wide(bg, cols_bg, type_list = rep("num",length(cols_bg)))
get_type(stat_bg)
bg <- remove_extreme_value_wide(bg, cols_bg, get_type(stat_bg), names(bg)[c(2,1,29)])

object_tosave <- c(object_tosave, "cols_bg","fillf_bg","fillf1_bg",
                  "stat_bg")


table_name <- "chemistry"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
chemistry <- dbGetQuery(con, query)
chemistry <- merge(chemistry, admissions[,c("hadm_id","admittime")], by="hadm_id")
chemistry <- chemistry[chemistry$hadm_id %in% unique(admissions$hadm_id),]
chemistry <- chemistry %>% filter(hadm_id %in% admissions$hadm_id,
                                  charttime >= admittime) %>% 
                                  mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
chemistry[1:4,]
names(chemistry)
names(chemistry)[c(5:16)] <- paste0(names(chemistry)[c(5:16)], "_chemistry")
cols_chem<-names(chemistry)[c(5:16)]
fillf_chem <- rep("locf", length(cols_chem))
fillf1_chem <- rep("mean", length(cols_chem))
aggf_chem <- rep("median", length(cols_chem))
stat_chemistry <- get_stat_wide(chemistry,cols_chem, rep("num", length(cols_chem)))
get_type(stat_chemistry)
chemistry <- remove_extreme_value_wide(chemistry, cols_chem, get_type(stat_chemistry),
                                        names(chemistry)[c(2,1,18)])
chemistry[1:2,]

object_tosave <- c(object_tosave, "cols_chem","fillf_chem","fillf1_chem",
                  "stat_chemistry")


table_name <- "vitalsign"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vitalsign <- dbGetQuery(con, query)
vitalsign <- merge(vitalsign, icustays[,c("subject_id","hadm_id","stay_id")], by=c("subject_id","stay_id"), all.x=T)
vitalsign <- vitalsign[vitalsign$hadm_id %in% unique(admissions$hadm_id),]
vitalsign <- merge(vitalsign, admissions[,c("hadm_id","admittime")], by="hadm_id")
vitalsign <- vitalsign %>% filter(hadm_id %in% admissions$hadm_id,
                                  charttime >= admittime) %>% 
                          mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
vitalsign[1:4,]
names(vitalsign)
names(vitalsign)[c(5:13,15:16)] <- paste0(names(vitalsign)[c(5:13,15:16)], "_vit")

cols_vit <- names(vitalsign)[c(5:13,15:16)]
fillf_vit <- rep("locf", length(cols_vit))
fillf1_vit <- rep("mean", length(cols_vit))
aggf_vit <- rep("median", length(cols_vit))
stat_vital <- get_stat_wide(vitalsign, cols_vit, rep("num", length(cols_vit)))
vitalsign <- remove_extreme_value_wide(vitalsign, cols_vit, get_type(stat_vital), cols_keep = names(vitalsign)[c(2,1,18)])
get_type(stat_vital)

object_tosave <- c(object_tosave, "cols_vit","fillf_vit","fillf1_vit",
                  "stat_vital")


table_name <- "gcs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
gcs <- dbGetQuery(con, query)
gcs <- merge(gcs, icustays[,c("subject_id","hadm_id","stay_id")], by=c("subject_id","stay_id"), all.x=T)
gcs <- gcs[gcs$hadm_id %in% unique(admissions$hadm_id),]
gcs <- merge(gcs, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)
gcs <- gcs %>% filter(hadm_id %in% admissions$hadm_id,
                      charttime >= admittime) %>% 
                      mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
gcs[1:4,]
gcs <- gcs %>% mutate(
  gcs_motor = if_else(gcs_motor == 0, NA_real_, gcs_motor),
  gcs_verbal = if_else(gcs_verbal == 0, NA_real_, gcs_verbal),
  gcs_eyes = if_else(gcs_eyes == 0, NA_real_, gcs_eyes)
)
cols_gcs <- names(gcs)[c(6:8)]
fillf_gcs <- rep("locf", length(cols_gcs))
fillf1_gcs <- rep("mean", length(cols_gcs))
aggf_gcs <- rep("median", length(cols_gcs))
stat_gcs <- get_stat_wide(gcs, cols_gcs, rep("ord", length(cols_gcs)))
get_type(stat_gcs)

object_tosave <- c(object_tosave, "cols_gcs","fillf_gcs","fillf1_gcs",
                  "stat_gcs")


table_name <- "ventilation"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
ventilation <- dbGetQuery(con, query)
ventilation <- merge(ventilation, icustays[,c("subject_id","hadm_id","stay_id")], by=c("stay_id"), all.x=T)
ventilation <- ventilation[ventilation$hadm_id %in% unique(admissions$hadm_id),]
ventilation <- merge(ventilation, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)

ventilation <- ventilation %>% filter(hadm_id %in% admissions$hadm_id,
                                      starttime >= admittime) %>% 
                                mutate(starttime_r = as.numeric(difftime(starttime, admittime, units = "hours")),
                                       endtime_r = as.numeric(difftime(endtime, admittime, units = "hours")))
ventilation[1:4,]
ventilation$ventilation_status[ventilation$ventilation_status == "None"] <- NA
names(ventilation)
cols_vent<- names(ventilation)[c(5)]
fillf_vent <- rep("zero", length(cols_vent))
fillf1_vent <- rep("zero", length(cols_vent))
aggf_vent <- rep("mode", length(cols_vent))
stat_vent <- get_stat_wide(ventilation, cols_vent, rep("cat", length(cols_vent)))
get_type(stat_vent)

object_tosave <- c(object_tosave, "cols_vent","fillf_vent","fillf1_vent",
                  "stat_vent")


table_name <- "vasoactive_agent"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
vasoactive_agent <- dbGetQuery(con, query)
vasoactive_agent <- merge(vasoactive_agent, icustays[,c("subject_id","hadm_id","stay_id")], by=c("stay_id"), all.x=T)
vasoactive_agent <- vasoactive_agent[vasoactive_agent$hadm_id %in% unique(admissions$hadm_id),]
vasoactive_agent <- merge(vasoactive_agent, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)

vasoactive_agent <- vasoactive_agent %>% filter(hadm_id %in% admissions$hadm_id,
                                                starttime >= admittime) %>% 
                                          mutate(starttime_r = as.numeric(difftime(starttime, admittime, units = "hours")),
                                                 endtime_r = as.numeric(difftime(endtime, admittime, units = "hours")))
vasoactive_agent[1:4,]
names(vasoactive_agent)
cols_vaso <- names(vasoactive_agent)[c(5:11)]
fillf_vaso <- rep("zero", length(cols_vaso))
fillf1_vaso <- rep("zero", length(cols_vaso))
aggf_vaso <- rep("sum", length(cols_vaso))
stat_vaso <- get_stat_wide(vasoactive_agent, cols_vaso, rep("num", length(cols_vaso)))
vasoactive_agent <- remove_extreme_value_wide(vasoactive_agent, cols_vaso, get_type(stat_vaso), names(vasoactive_agent)[c(1,2,3,4,13,14,15)], neg_valid = F)
get_type(stat_vaso)
object_tosave <- c(object_tosave, "cols_vaso","fillf_vaso","fillf1_vaso",
                  "stat_vaso")


table_name <- "complete_blood_count"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
complete_blood_count <- dbGetQuery(con, query)

complete_blood_count <- merge(complete_blood_count, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)
complete_blood_count <- complete_blood_count[complete_blood_count$hadm_id %in% unique(admissions$hadm_id),]
complete_blood_count <- complete_blood_count %>% filter(hadm_id %in% admissions$hadm_id,
                                                charttime >= admittime) %>% 
                                                mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
complete_blood_count[1:4,]
names(complete_blood_count)
cols_cbc <- names(complete_blood_count)[c(5:14)]
fillf_cbc <- rep("locf", length(cols_cbc))
fillf1_cbc <- rep("mean", length(cols_cbc))
aggf_cbc <- rep("median", length(cols_cbc))
stat_cbc <- get_stat_wide(complete_blood_count, cols_cbc, rep("num", length(cols_cbc)))
complete_blood_count <- remove_extreme_value_wide(complete_blood_count, cols_cbc, get_type(stat_cbc), names(complete_blood_count)[c(2,1,16)])
get_type(stat_cbc)

object_tosave <- c(object_tosave, "cols_cbc","fillf_cbc","fillf1_cbc",
                  "stat_cbc")


table_name <- "coagulation"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
coagulation <- dbGetQuery(con, query)

coagulation <- merge(coagulation, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)
coagulation <- coagulation[coagulation$hadm_id %in% unique(admissions$hadm_id),]

coagulation <- coagulation %>% filter(hadm_id %in% admissions$hadm_id,
                                                charttime >= admittime) %>% 
                                                mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
coagulation[1:4,]
names(coagulation)
names(coagulation)[c(5:10)] <- paste0(names(coagulation)[c(5:10)], "_coag")
cols_coag <- names(coagulation)[c(5:10)]
fillf_coag <- rep("locf", length(cols_coag))
fillf1_coag <- rep("mean", length(cols_coag))
aggf_coag <- rep("median", length(cols_coag))
stat_coag <- get_stat_wide(coagulation, cols_coag, rep("num", length(cols_coag)))
coagulation <- remove_extreme_value_wide(coagulation, cols_coag, get_type(stat_coag), names(coagulation)[c(2,1,12)])
get_type(stat_coag)

object_tosave <- c(object_tosave, "cols_coag","fillf_coag","fillf1_coag",
                  "stat_coag")

table_name <- "urine_output"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
urine_output <- dbGetQuery(con, query)

urine_output <- merge(urine_output, icustays[,c("subject_id","hadm_id","stay_id")], by=c("stay_id"), all.x=T)
urine_output <- urine_output[urine_output$hadm_id %in% unique(admissions$hadm_id),]
urine_output <- merge(urine_output, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)

urine_output <- urine_output %>% filter(hadm_id %in% admissions$hadm_id,
                                                charttime >= admittime) %>% 
                                                mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
urine_output[1:4,]
names(urine_output)
cols_uo <- names(urine_output)[c(4)]
fillf_uo <- rep("zero", length(cols_uo))
fillf1_uo <- rep("zero", length(cols_uo))
aggf_uo <- rep("sum", length(cols_uo))
stat_uo <- get_stat_wide(urine_output, cols_uo, rep("num", length(cols_uo)))
urine_output <- remove_extreme_value_wide(urine_output, cols_uo, get_type(stat_uo), names(urine_output)[c(2,1,7)])
get_type(stat_uo)

object_tosave <- c(object_tosave, "cols_uo","fillf_uo","fillf1_uo",
                  "stat_uo")


table_name <- "blood_differential"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
blood_differential <- dbGetQuery(con, query)

blood_differential <- merge(blood_differential, icustays[,c("subject_id","hadm_id","stay_id")], by=c("subject_id","hadm_id"), all.x=T)
blood_differential <- blood_differential[blood_differential$hadm_id %in% unique(admissions$hadm_id),]
blood_differential <- merge(blood_differential, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)

blood_differential <- blood_differential %>% filter(hadm_id %in% admissions$hadm_id,
                                                charttime >= admittime) %>% 
                                                mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
blood_differential[1:4,]
names(blood_differential)
names(blood_differential)[c(5:20)] <- paste0(names(blood_differential)[c(5:20)], "_bd")  
cols_bd <- names(blood_differential)[c(5:20)]
fillf_bd <- rep("locf", length(cols_bd))
fillf1_bd <- rep("mean", length(cols_bd))
aggf_bd <- rep("median", length(cols_bd))
stat_bd <- get_stat_wide(blood_differential, cols_bd, rep("num", length(cols_bd)))
blood_differential <- remove_extreme_value_wide(blood_differential, cols_bd, get_type(stat_bd), names(blood_differential)[c(2,1,23)])
get_type(stat_bd)

object_tosave <- c(object_tosave, "cols_bd","fillf_bd","fillf1_bd",
                  "stat_bd")

# table_name <- "rrt"
# query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
# rrt <- dbGetQuery(con, query)

# rrt <- merge(rrt, icustays[,c("subject_id","hadm_id","stay_id")], by=c("stay_id"), all.x=T)
# rrt <- merge(rrt, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)

# rrt <- rrt %>% filter(hadm_id %in% admissions$hadm_id,
#                                                 charttime >= admittime) %>% 
#                                                 mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
# rrt[1:4,]
# names(rrt)
# names(rrt)[c(3:5)] <- paste0(names(rrt)[c(3:5)], "_rrt")  
# rrt$dialysis_type_rrt[is.na(rrt$dialysis_type_rrt)] <- "None"
# cols_rrt <- names(rrt)[c(3:5)]
# stat_rrt <- get_stat_wide(rrt, cols_rrt, c("cat","cat","cat"))
# get_type(stat_rrt)


table_name <- "enzyme"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
enzyme <- dbGetQuery(con, query)

enzyme <- merge(enzyme, icustays[,c("subject_id","hadm_id","stay_id")], by=c("subject_id","hadm_id"), all.x=T)
enzyme <- enzyme[enzyme$hadm_id %in% unique(admissions$hadm_id),]
enzyme <- merge(enzyme, admissions[,c("hadm_id","admittime")], by="hadm_id", all.x=T)

enzyme <- enzyme %>% filter(hadm_id %in% admissions$hadm_id,
                                                charttime >= admittime) %>% 
                                                mutate(charttime_r = as.numeric(difftime(charttime, admittime, units = "hours")))
enzyme[1:4,]
names(enzyme)
names(enzyme)[c(5:15)] <- paste0(names(enzyme)[c(5:15)], "_enzyme")  
cols_enzyme <- names(enzyme)[c(5:15)]
fillf_enzyme <- rep("locf", length(cols_enzyme))
fillf1_enzyme <- rep("mean", length(cols_enzyme))
aggf_enzyme <- rep("median", length(cols_enzyme))
stat_enzyme <- get_stat_wide(enzyme, cols_enzyme, c(rep("num",length(cols_enzyme))))
enzyme <- remove_extreme_value_wide(enzyme, cols_enzyme, get_type(stat_enzyme), names(enzyme)[c(2,1,18)])
get_type(stat_enzyme)

object_tosave <- c(object_tosave, "cols_enzyme","fillf_enzyme","fillf1_enzyme",
                  "stat_enzyme")

save(list=object_tosave, file="/home/luojiawei/Benchmark_project_data/mimiciv_data/meta_info.RData")

# 关闭连接
dbDisconnect(con)

# all_hadmid <- do.call(intersect, list(unique(admissions$hadm_id),
#                                       unique(vitalsign$hadm_id)))
length(unique(admissions$subject_id))
all_hadmid <- unique(admissions$hadm_id)
length(all_hadmid)

get_1hadmid <- function(k) {
  # k<-532
  id_k <- all_hadmid[k]
  admissions_k <- admissions[admissions$hadm_id==id_k,,drop=F]
  static <- admissions_k[, c(cols_static),drop=F]

  static <- norm_num(static, 1:ncol(static), NULL, get_type(stat_static), stat_static)
  static <- to_onehot(static, 1:ncol(static), NULL, get_type(stat_static), stat_static)
  static <- as.data.frame(static)
  static <- lapply(static, as.numeric) %>% as.data.frame

  twd<-1
  time_range <- seq(0, floor(admissions_k$dischtime_r[1]), twd)

  # 实验室检查
  cols_1 <- c(cols_bg, cols_chem, cols_cbc,cols_coag, cols_bd, cols_enzyme)
  stat_1 <- c(stat_bg, stat_chemistry, stat_cbc, stat_coag, stat_bd, stat_enzyme)
  fillfun_list_1 <- c(fillf_bg, fillf_chem, fillf_cbc, fillf_coag, fillf_bd, fillf_enzyme)
  allNAfillfun_list_1 <- c(fillf1_bg, fillf1_chem, fillf1_cbc, fillf1_coag, fillf1_bd, fillf1_enzyme)

  bg_k <- bg[bg$stay_id == id_k, ]
  bg_k$timecol2 <- rep(NA, nrow(bg_k))
  bg_k1 <- resample_data_wide(bg_k, 
                              cols_bg,
                              get_type(stat_bg),
                              aggf_bg,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)
  
  chemistry_k <- chemistry[chemistry$hadm_id == id_k, ]
  chemistry_k$timecol2 <- rep(NA, nrow(chemistry_k))
  chemistry_k1 <- resample_data_wide(chemistry_k, 
                                     cols_chem,
                                     get_type(stat_chemistry),
									                   aggf_chem,
                                     time_range, 
                                     time_col1 = "charttime_r", 
                                     time_col2 = "timecol2",
                                     time_window = 1,
                                     keepNArow=T)
  
  complete_blood_count_k <- complete_blood_count[complete_blood_count$hadm_id == id_k,,drop=F]
  complete_blood_count_k$timecol2 <- rep(NA, nrow(complete_blood_count_k))
  complete_blood_count_k1 <- resample_data_wide(complete_blood_count_k, 
                              cols_cbc,
                              get_type(stat_cbc),
                              aggf_cbc,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  coagulation_k <- coagulation[coagulation$hadm_id == id_k,,drop=F]
  coagulation_k$timecol2 <- rep(NA, nrow(coagulation_k))
  coagulation_k1 <- resample_data_wide(coagulation_k, 
                              cols_coag,
                              get_type(stat_coag),
                              aggf_coag,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  blood_differential_k <- blood_differential[blood_differential$hadm_id == id_k,,drop=F]
  blood_differential_k$timecol2 <- rep(NA, nrow(blood_differential_k))
  blood_differential_k1 <- resample_data_wide(blood_differential_k, 
                              cols_bd,
                              get_type(stat_bd),
                              aggf_bd,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  enzyme_k <- enzyme[enzyme$hadm_id == id_k,,drop=F]
  enzyme_k$timecol2 <- rep(NA, nrow(enzyme_k))
  enzyme_k1 <- resample_data_wide(enzyme_k, 
                              cols_enzyme,
                              get_type(stat_enzyme),
                              aggf_enzyme,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  ind_1 <- which(bg_k1[,2]=="1" | chemistry_k1[,2]=="1" | complete_blood_count_k1[,2] == "1" | 
            coagulation_k1[,2]=="1" | blood_differential_k1[,2]=="1" | 
            enzyme_k1[,2]=="1")
  X_1 <- cbind(bg_k1[,c(1,3:ncol(bg_k1)),drop=F], chemistry_k1[,c(3:ncol(chemistry_k1)),drop=F],complete_blood_count_k1[,3:ncol(complete_blood_count_k1), drop=F],
               coagulation_k1[,c(3:ncol(coagulation_k1)),drop=F],
               blood_differential_k1[,c(3:ncol(blood_differential_k1)),drop=F],
               enzyme_k1[,3:ncol(enzyme_k1),drop=F])
  X_1 <- X_1[ind_1,,drop=F]
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

  # ---------  生命体征 --------------
  stat_2 <- c(stat_vital, stat_gcs, stat_uo) 
  cols_2 <- c(cols_vit, cols_gcs, cols_uo)
  fillfun_list_2 <- c(fillf_vit, fillf_gcs, fillf_uo)
  allNAfillfun_list_2 <- c(fillf1_vit, fillf1_gcs, fillf1_uo)

  vitalsign_k <- vitalsign[vitalsign$hadm_id == id_k,]
  vitalsign_k$timecol2 <- rep(NA, nrow(vitalsign_k))
  vitalsign_k1 <- resample_data_wide(vitalsign_k, 
                                     cols_vit,
                                     get_type(stat_vital),
                                     aggf_vit,
                                     time_range, 
                                     time_col1 = "charttime_r", 
                                     time_col2 = "timecol2",
                                     time_window = 1,
                                     keepNArow = T)

  gcs_k <- gcs[gcs$hadm_id == id_k,]
  gcs_k$timecol2 <- rep(NA, nrow(gcs_k))
  gcs_k1 <- resample_data_wide(gcs_k, 
                              cols_gcs,
                              get_type(stat_gcs),
                              aggf_gcs,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow = T)
  urine_output_k <- urine_output[urine_output$hadm_id == id_k,]
  urine_output_k$timecol2 <- rep(NA, nrow(urine_output_k))
  urine_output_k1 <- resample_data_wide(urine_output_k, 
                              cols_uo,
                              get_type(stat_uo),
                              aggf_uo,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow = T)

  ind_2 <- which(vitalsign_k1[,2] == "1" | gcs_k1[,2] == "1" | urine_output_k1[,1] == "1")
  X_2 <- cbind(vitalsign_k1[,c(1, 3:ncol(vitalsign_k1)),drop=F], gcs_k1[,3:ncol(gcs_k1),drop=F], urine_output_k1[,3:ncol(urine_output_k1),drop=F])
  X_2 <- X_2[ind_2,,drop=F]
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

  # -------- 药物治疗, 机械通气 ------------
  
  cols_3 <- c(cols_vent, cols_vaso)
  stat_3 <- c(stat_vent, stat_vaso)
  fillfun_list_3 <- c(fillf_vent, fillf_vaso)
  allNAfillfun_list_3 <- c(fillf1_vent, fillf1_vaso)

  ventilation_k <- ventilation[ventilation$hadm_id == id_k,]
  ventilation_k1 <- resample_process_wide(ventilation_k, 
                              cols_vent,
                              get_type(stat_vent),
                              aggf_vent,
                              time_range, 
                              time_col1 = "starttime_r", 
                              time_col2 = "endtime_r",
                              time_window = 1,
                              keepNArow=T)

  vasoactive_agent_k <- vasoactive_agent[vasoactive_agent$hadm_id == id_k,]
  vasoactive_agent_k1 <- resample_process_wide(vasoactive_agent_k, 
                              cols_vaso,
                              get_type(stat_vaso),
                              aggf_vaso,
                              time_range, 
                              time_col1 = "starttime_r", 
                              time_col2 = "endtime_r",
                              time_window = 1,
                              keepNArow=T)

  ind_3 <- which(ventilation_k1[,2] == "1" | vasoactive_agent_k1[,2] == "1")
  X_3 <- cbind(ventilation_k1[,c(1, 3:ncol(ventilation_k1)),drop=F], 
              vasoactive_agent_k1[,3:ncol(vasoactive_agent_k1),drop=F])
  X_3 <- X_3[ind_3,,drop=F]
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

  return(list(static, X_1, mask_1, delta_1, X_2, mask_2, delta_2, X_3, mask_3, delta_3, admissions_k))
}

get_1hadmid_allAlign <- function(k) {
  # k<-8
  id_k <- all_hadmid[k]
  admissions_k <- admissions[admissions$hadm_id==id_k,,drop=F]
  static <- admissions_k[, c(cols_static),drop=F]

  static <- norm_num(static, 1:ncol(static), NULL, get_type(stat_static), stat_static)
  static <- to_onehot(static, 1:ncol(static), NULL, get_type(stat_static), stat_static)
  static <- as.data.frame(static)
  static <- lapply(static, as.numeric) %>% as.data.frame

  twd<-1
  time_range <- seq(0, floor(admissions_k$dischtime_r[1]), twd)

  # 实验室检查
  cols_1 <- c(cols_bg, cols_chem, cols_cbc,cols_coag, cols_bd, cols_enzyme)
  stat_1 <- c(stat_bg, stat_chemistry, stat_cbc, stat_coag, stat_bd, stat_enzyme)
  fillfun_list_1 <- c(fillf_bg, fillf_chem, fillf_cbc, fillf_coag, fillf_bd, fillf_enzyme)
  allNAfillfun_list_1 <- c(fillf1_bg, fillf1_chem, fillf1_cbc, fillf1_coag, fillf1_bd, fillf1_enzyme)

  bg_k <- bg[bg$stay_id == id_k, ]
  bg_k$timecol2 <- rep(NA, nrow(bg_k))
  bg_k1 <- resample_data_wide(bg_k, 
                              cols_bg,
                              get_type(stat_bg),
                              aggf_bg,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)
  
  chemistry_k <- chemistry[chemistry$hadm_id == id_k, ]
  chemistry_k$timecol2 <- rep(NA, nrow(chemistry_k))
  chemistry_k1 <- resample_data_wide(chemistry_k, 
                                     cols_chem,
                                     get_type(stat_chemistry),
                                     aggf_chem,
                                     time_range, 
                                     time_col1 = "charttime_r", 
                                     time_col2 = "timecol2",
                                     time_window = 1,
                                     keepNArow=T)
  
  complete_blood_count_k <- complete_blood_count[complete_blood_count$hadm_id == id_k,,drop=F]
  complete_blood_count_k$timecol2 <- rep(NA, nrow(complete_blood_count_k))
  complete_blood_count_k1 <- resample_data_wide(complete_blood_count_k, 
                              cols_cbc,
                              get_type(stat_cbc),
                              aggf_cbc,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  coagulation_k <- coagulation[coagulation$hadm_id == id_k,,drop=F]
  coagulation_k$timecol2 <- rep(NA, nrow(coagulation_k))
  coagulation_k1 <- resample_data_wide(coagulation_k, 
                              cols_coag,
                              get_type(stat_coag),
                              aggf_coag,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  blood_differential_k <- blood_differential[blood_differential$hadm_id == id_k,,drop=F]
  blood_differential_k$timecol2 <- rep(NA, nrow(blood_differential_k))
  blood_differential_k1 <- resample_data_wide(blood_differential_k, 
                              cols_bd,
                              get_type(stat_bd),
                              aggf_bd,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  enzyme_k <- enzyme[enzyme$hadm_id == id_k,,drop=F]
  enzyme_k$timecol2 <- rep(NA, nrow(enzyme_k))
  enzyme_k1 <- resample_data_wide(enzyme_k, 
                              cols_enzyme,
                              get_type(stat_enzyme),
                              aggf_enzyme,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow=T)

  

  # ---------  生命体征 --------------
  stat_2 <- c(stat_vital, stat_gcs, stat_uo) 
  cols_2 <- c(cols_vit, cols_gcs, cols_uo)
  fillfun_list_2 <- c(fillf_vit, fillf_gcs, fillf_uo)
  allNAfillfun_list_2 <- c(fillf1_vit, fillf1_gcs, fillf1_uo)

  vitalsign_k <- vitalsign[vitalsign$hadm_id == id_k,]
  vitalsign_k$timecol2 <- rep(NA, nrow(vitalsign_k))
  vitalsign_k1 <- resample_data_wide(vitalsign_k, 
                                     cols_vit,
                                     get_type(stat_vital),
                                     aggf_vit,
                                     time_range, 
                                     time_col1 = "charttime_r", 
                                     time_col2 = "timecol2",
                                     time_window = 1,
                                     keepNArow = T)

  gcs_k <- gcs[gcs$hadm_id == id_k,]
  gcs_k$timecol2 <- rep(NA, nrow(gcs_k))
  gcs_k1 <- resample_data_wide(gcs_k, 
                              cols_gcs,
                              get_type(stat_gcs),
                              aggf_gcs,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow = T)

  urine_output_k <- urine_output[urine_output$hadm_id == id_k,]
  urine_output_k$timecol2 <- rep(NA, nrow(urine_output_k))
  urine_output_k1 <- resample_data_wide(urine_output_k, 
                              cols_uo,
                              get_type(stat_uo),
                              aggf_uo,
                              time_range, 
                              time_col1 = "charttime_r", 
                              time_col2 = "timecol2",
                              time_window = 1,
                              keepNArow = T)

  # -------- 药物治疗, 机械通气 ------------
  
  cols_3 <- c(cols_vent, cols_vaso)
  stat_3 <- c(stat_vent, stat_vaso)
  fillfun_list_3 <- c(fillf_vent, fillf_vaso)
  allNAfillfun_list_3 <- c(fillf1_vent, fillf1_vaso)

  ventilation_k <- ventilation[ventilation$hadm_id == id_k,]
  ventilation_k1 <- resample_process_wide(ventilation_k, 
                              cols_vent,
                              get_type(stat_vent),
                              aggf_vent,
                              time_range, 
                              time_col1 = "starttime_r", 
                              time_col2 = "endtime_r",
                              time_window = 1,
                              keepNArow=T)

  vasoactive_agent_k <- vasoactive_agent[vasoactive_agent$hadm_id == id_k,]
  vasoactive_agent_k1 <- resample_process_wide(vasoactive_agent_k, 
                              cols_vaso,
                              get_type(stat_vaso),
                              aggf_vaso,
                              time_range, 
                              time_col1 = "starttime_r", 
                              time_col2 = "endtime_r",
                              time_window = 1,
                              keepNArow=T)

  ind_all <- which(bg_k1[,2]=="1" | chemistry_k1[,2]=="1" | complete_blood_count_k1[,2] == "1" | 
            coagulation_k1[,2]=="1" | blood_differential_k1[,2]=="1" | 
            enzyme_k1[,2]=="1" |
            vitalsign_k1[,2] == "1" | gcs_k1[,2] == "1" | urine_output_k1[,1] == "1" |
            ventilation_k1[,2] == "1" | vasoactive_agent_k1[,2] == "1")

  X_1 <- cbind(bg_k1[,c(1,3:ncol(bg_k1)),drop=F], chemistry_k1[,c(3:ncol(chemistry_k1)),drop=F],complete_blood_count_k1[,3:ncol(complete_blood_count_k1), drop=F],
               coagulation_k1[,c(3:ncol(coagulation_k1)),drop=F],
               blood_differential_k1[,c(3:ncol(blood_differential_k1)),drop=F],
               enzyme_k1[,3:ncol(enzyme_k1),drop=F])
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

  X_2 <- cbind(vitalsign_k1[,c(1, 3:ncol(vitalsign_k1)),drop=F], gcs_k1[,3:ncol(gcs_k1),drop=F], urine_output_k1[,3:ncol(urine_output_k1),drop=F])
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
  
  X_3 <- cbind(ventilation_k1[,c(1, 3:ncol(ventilation_k1)),drop=F], 
              vasoactive_agent_k1[,3:ncol(vasoactive_agent_k1),drop=F])
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

  return(list(static, X_1, mask_1, delta_1, X_2, mask_2, delta_2, X_3, mask_3, delta_3, admissions_k))
}


process_data <- function(k, root_path) {

    # k<-8
    id_k<-all_hadmid[k]
    folder_path<-file.path(root_path, id_k)
    # create_dir(folder_path, F)
    
    datas <- get_1hadmid_allAlign(k)
    # datas <- get_1hadmid(k)
    
    # datas[[1]]
    # datas[[2]][1:2,]
    # datas[[5]][1:2,]
    # datas[[8]][1:2,]

    # fwrite(datas[[1]], file=file.path(folder_path, "static.csv"), row.names=F)

    # fwrite(datas[[2]], file=file.path(folder_path, "lab_x.csv"), row.names=F)
    # fwrite(datas[[3]], file=file.path(folder_path, "lab_m.csv"), row.names=F)
    # fwrite(datas[[4]], file=file.path(folder_path, "lab_dt.csv"), row.names=F)

    # fwrite(datas[[5]], file=file.path(folder_path, "vital_x.csv"), row.names=F)
    # fwrite(datas[[6]], file=file.path(folder_path, "vital_m.csv"), row.names=F)
    # fwrite(datas[[7]], file=file.path(folder_path, "vital_dt.csv"), row.names=F)

    # fwrite(datas[[8]], file=file.path(folder_path, "trt_x.csv"), row.names=F)
    # fwrite(datas[[9]], file=file.path(folder_path, "trt_m.csv"), row.names=F)
    # fwrite(datas[[10]], file=file.path(folder_path, "trt_dt.csv"), row.names=F)

    fwrite(datas[[11]][,c("dischtime_r","hospital_expire_flag","re_admission","los_day_7"),drop=F], 
           file=file.path(folder_path, "y.csv"), row.names=F)
}

names(admissions)

root_path <- "/home/luojiawei/Benchmark_project_data/mimiciv_data/patient_folders_1/"
create_dir(root_path, T)
twd<-1 # 时间分辨率(hour)

results <- mclapply(seq_along(all_hadmid), 
                function(x) {
                  result <- tryCatch({
                    process_data(x, root_path = root_path)
                  }, error = function(e) {
                    print(e)
                    print(x)
                  })
                  if(x %% 3000 == 0) print(x)
                  
                  return(result)
                }, mc.cores = detectCores())

#  -------- 数据集划分 -------------

as.data.frame(admissions[1:2,])

library(caret)

# 设置种子以确保结果可重复
set.seed(123)

# 创建数据分区
partition <- createDataPartition(admissions$hadm_id, p = 0.7, list = FALSE)

# 创建训练集
train_set <- admissions[partition, ]

# 创建测试和验证集
remaining_set <- admissions[-partition, ]

# 再次进行数据分区以在测试集和验证集之间进行划分
partition <- createDataPartition(remaining_set$hadm_id, p = 0.67, list = FALSE)

# 创建测试集
test_set <- remaining_set[partition, ]

# 创建验证集
valid_set <- remaining_set[-partition, ]

fwrite(train_set, file="/home/luojiawei/Benchmark_project_data/mimiciv_data/train_set.csv", row.names=F)
fwrite(test_set, file="/home/luojiawei/Benchmark_project_data/mimiciv_data/test_set.csv", row.names=F)
fwrite(valid_set, file="/home/luojiawei/Benchmark_project_data/mimiciv_data/valid_set.csv", row.names=F)


admissions <- fread("/home/luojiawei/Benchmark_project_data/mimiciv_data/admissions.csv", header=T)
admissions[1:2,]
names(admissions)
