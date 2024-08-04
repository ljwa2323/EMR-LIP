
library(tableone)
library(tibble)
library(data.table)
library(dplyr)

# ---------------  描述性统计分析-------------------------------

icustays <- fread("/home/luojiawei/EMR_LIP_data/ds_id_mimic_iv.csv")
patients <- fread("/home/luojiawei/EMR_LIP_data/ds_id_eicu_crd.csv")

names(patients)[c(4,3,5,31,17)]

names(icustays)[c(16,15,14,17,13)]

icustays[1:5,c(16,15,14,17,13)]
patients[1:5, c(4,3,5,31,17)]

unique(patients$ethnicity)
unique(icustays$race)

# 更新映射函数
map_ethnicity <- function(ethnicity) {
  if (grepl("WHITE|CAUCASIAN|EUROPEAN|RUSSIAN|BRAZILIAN", ethnicity, ignore.case = TRUE)) {
    return("White")
  } else if (grepl("BLACK|AFRICAN", ethnicity, ignore.case = TRUE)) {
    return("Black/African American")
  } else if (grepl("HISPANIC|LATINO", ethnicity, ignore.case = TRUE)) {
    return("Hispanic/Latino")
  } else if (grepl("ASIAN", ethnicity, ignore.case = TRUE)) {
    return("Asian")
  } else {
    return("Other/Unknown")
  }
}

# 应用更新后的映射函数
patients$ethnicity <- sapply(patients$ethnicity, map_ethnicity)
icustays$race <- sapply(icustays$race, map_ethnicity)

# 查看更新映射后的结果
unique(patients$ethnicity)
unique(icustays$race)

# ------------------------

patients$los_icu <- patients$los_icu / 60


ds1 <- icustays[,c(16,15,14,17,13)]
ds2 <- patients[, c(4,3,5,31,17)]

names(ds1) <- c("age","gender","race","los_icu","in_hospital_death")
names(ds2) <- c("age","gender","race","los_icu","in_hospital_death")

ds <- rbind(ds1, ds2)
ds$db <- rep(c("mimic","eicu"), times=c(nrow(ds1), nrow(ds2)))
unique(ds$gender)

# 映射函数
map_gender <- function(gender) {
  if (grepl("^F$|^Female$", gender, ignore.case = TRUE)) {
    return("Female")
  } else if (grepl("^M$|^Male$", gender, ignore.case = TRUE)) {
    return("Male")
  } else {
    return("Other/Unknown")
  }
}

# 应用映射函数
ds$gender <- sapply(ds$gender, map_gender)

# 查看映射后的结果
unique(ds$gender)



ds <- as.data.frame(ds)
names(ds)
vars <- names(ds)[c(1:5)]
str_vars <- names(ds)[c(2,3,5)]
num_vars <- vars[!(vars %in% str_vars)]

ds[,str_vars] <- lapply(ds[,str_vars], function(x) {x <- as.character(x); x<-factor(x); x})
ds[,num_vars] <- lapply(ds[,num_vars], as.numeric)

# # 对每一列进行正态性检验
normality_test <- lapply(num_vars, function(var) {
  # var <- num_vars[1]
  x <- ds[[var]]
  result <- tryCatch(ad.test(x), error = function(e) {
    list(p.value = 0.01)
  })
  return(result)
})

# # 将列表转换为一个命名列表
normality_test <- setNames(normality_test, num_vars)

non_norm <- c()
for (i in seq_along(normality_test)) {
  if (normality_test[[i]]$p.value < 0.05) {
    non_norm <- c(non_norm, names(normality_test)[i])
  }
}
non_norm <- c()
# non_norm <- non_norm[-c(1,2,3,4)]

# Grouped summary table
tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = ds, strata = "db")

# Print table
tab1<-print(tab, nonnormal = non_norm, showAllLevels = T)
rn<-row.names(tab1)
tab1<-as.data.frame(tab1)
tab1 <- rownames_to_column(tab1, var = "Variable")
tab1$Variable<-rn

tab1

write.csv(tab1, file="./结果文件夹/基线.csv", row.names=F, fileEncoding='gb2312')
