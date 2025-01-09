source("./EMR_LIP_tmp.R")

library(readxl)
library(dplyr)

ds <- read_excel("data.xlsx", col_names=T, sheet=1)
var_dict <- read_excel("var_dict.xlsx", col_names=T, sheet=1)
ds_map <- read_excel("var_dict.xlsx", col_names=T, sheet=2)

ds <- rename_long_table(ds, ds_map$old_name, ds_map$new_name, "item_id")
ds <- rename_wide_table(ds, ds_map$old_name, ds_map$new_name)

ds <- remove_extreme_value_long(ds, var_dict$itemid, var_dict$value_type,
                                 "item_id", "value", var_dict)

ds <- remove_extreme_value_wide(ds, var_dict$itemid, var_dict$value_type, 1:3
                                 var_dict)

stat_ds <- get_stat_long(ds, 
                          var_dict$itemid, 
                          var_dict$value_type,
                          "item_id",
                          "value",
                          var_dict$cont,
                          var_dict)

stat_ds <- get_stat_wide(ds, 
                          var_dict$itemid, 
                          var_dict$value_type,
                          var_dict$cont,
                          var_dict)


# undebug(resample_long)

ds_k <- ds[ds$sid == 1,]
ds_k1 <- resample_long(df = ds_k,
                       itemid_list = var_dict$itemid,
                       type_list = var_dict$value_type,
                       agg_f_list = var_dict$agg_fun,
                       time_list = 1:15,
                       time_col1 = "time",
                       time_col2 = "time2",
                       itemid_col = "item_id",
                       value_col = "value",
                       time_window = 1,
                       direction = "both",
                       keepNArow = T,
                       keep_first = T)

ds_k1 <- resample_wide(df = ds_k,
                       itemid_list = var_dict$itemid,
                       type_list = var_dict$value_type,
                       agg_f_list = var_dict$agg_fun,
                       time_list = 1:4,
                       time_col1 = "time",
                       time_col2 = "time2",
                       time_window = 1,
                       direction = "both",
                       keepNArow = T,
                       keep_first = T)


ind1 <- (ds_k1[,2] == "1")
ds_k1 <- ds_k1[,c(1,3:ncol(ds_k1))]
mask_k1 <- get_mask(ds_k1, names(ds_k1)[2:ncol(ds_k1)], "time")
ds_k1 <- fill(ds_k1, 2:ncol(ds_k1), get_type(stat_ds), var_dict$fill1, var_dict$fill2, stat_ds)
str(ds_k1)
ds_k1 <- fill_last_values(ds_k1, mask_k1, 2:ncol(ds_k1), 1, var_dict)
ds_k1 <- to_onehot(ds_k1, 2:ncol(ds_k1), 1, get_type(stat_ds), stat_ds)





ds <- read_excel("data.xlsx", col_names=T, sheet=1)
var_dict <- read_excel("var_dict.xlsx", col_names=T, sheet=1)
var_dict1 <- var_dict[var_dict$time_type == "point", ,drop=F]
var_dict2 <- var_dict[var_dict$time_type == "interval", ,drop=F]
ds_map <- read_excel("var_dict.xlsx", col_names=T, sheet=2)

ds <- rename_long_table(ds, ds_map$old_name, ds_map$new_name, "item_id")

ds <- remove_extreme_value_long(ds, var_dict$itemid, var_dict$value_type,
                                 "item_id", "value", var_dict)

stat_ds1 <- get_stat_long(ds, 
                          var_dict1$itemid, 
                          var_dict1$value_type,
                          "item_id",
                          "value",
                          var_dict1$cont)
stat_ds2 <- get_stat_long(ds, 
                          var_dict2$itemid, 
                          var_dict2$value_type,
                          "item_id",
                          "value",
                          var_dict2$cont)

ds_k <- ds[ds$sid == 1,]
ds_k1 <- resample_point_long(ds_k,
                                var_dict1$itemid,
                                var_dict1$value_type,
                                var_dict1$agg_fun,
                                1:4,
                                "item_id",
                                "value",
                                "time",
                                1,
                                direction = "both",
                                keepNArow = T,
                                keep_first = F)
# undebug(resample_single_long)
ind1 <- (ds_k1[,2] == "1")
ds_k1 <- ds_k1[,c(1,3:ncol(ds_k1))]
mask_k1 <- get_mask(ds_k1, 2:ncol(ds_k1), 1)
ds_k1 <- fill(ds_k1, 2:ncol(ds_k1), 1, get_type(stat_ds1), var_dict1$fill1, var_dict1$fill2, stat_ds1)
ds_k1 <- fill_last_values(ds_k1, mask_k1, 2:ncol(ds_k1), 1, var_dict1)
ds_k1 <- to_onehot(ds_k1, 2:ncol(ds_k1), 1, get_type(stat_ds1), stat_ds1)


ds_k2 <- resample_interval_long(ds_k,
                            var_dict2$itemid,
                            var_dict2$value_type,
                            var_dict2$agg_fun,
                            1:4,
                            "item_id",
                            "value",
                            "time",
                            "time2",
                            1,
                            direction = "both",
                            keepNArow = T,
                            keep_first = F)
ind2 <- (ds_k2[,2] == "1")
ds_k2 <- ds_k2[,c(1,3:ncol(ds_k2))]
mask_k2 <- get_mask(ds_k2, 2:ncol(ds_k2), 1)
ds_k2 <- fill(ds_k2, 2:ncol(ds_k2), 1, get_type(stat_ds2), var_dict2$fill1, var_dict2$fill2, stat_ds2)
ds_k2 <- fill_last_values(ds_k2, mask_k2, 2:ncol(ds_k2), 1, var_dict2)
ds_k2 <- to_onehot(ds_k2, 2:ncol(ds_k2), 1, get_type(stat_ds2), stat_ds2)

ds_k_ <- cbind(ds_k1, ds_k2[,2:ncol(ds_k2),drop=F]) %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
ds_k_ <- ds_k_[which(ind1 | ind2), ]
as.data.frame(ds_k_)
# as.data.frame(ds_k)

cbind(ds_k1, ds_k2[,2:ncol(ds_k2),drop=F])