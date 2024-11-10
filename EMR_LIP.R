library(data.table)
library(lubridate)
library(magrittr)

####################################################################################
#
#                            EMR-LIP APIs
# 
####################################################################################


#######################################
# remove and create directory
#######################################
create_dir <- function(root_path,verbo = T) {
  if (dir.exists(root_path)) {
    unlink(root_path, recursive = TRUE)
    if(verbo) print(paste0(root_path," removed"))
  }
  if (!dir.exists(root_path)) {
    dir.create(root_path)
    if(verbo) print(paste0(root_path," created"))
  }
}


#######################################
# convert to numeric data frame
#######################################

# 定义一个函数来转换输入为数值型数据框
convert_to_numeric_df <- function(df) {
  if (is.matrix(df)) {
    # 如果输入是矩阵，先转换为数据框
    df <- as.data.frame(df)
  }
  
  if (is.list(df) || is.data.frame(df)) {
    # 如果输入是列表或数据框，转换所有元素为数值型
    df <- lapply(df, as.numeric) %>% as.data.frame()
  }
  
  return(df)
}


#######################################
# get variable statistic
#######################################

get_stat_wide <- function(df, itemid_list, type_list, C_list) {

    stats <- mapply(function(i, itemid, type, cont){
        if (type == "num") {
            x <- df[[itemid]]
            x <- as.numeric(x)
            
            # Calculated quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)
            #If quantile_obj is all NA, the value is assigned to all zeros
            if (all(is.na(quantile_obj)) || length(quantile_obj) == 0) {
                quantile_obj <- rep(0, length(quantile_obj))
            }

            # Calculated standard deviation
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]

            mean_obj <- mean(x_in_range, na.rm = TRUE)

            # If mean_obj is NA, the value is assigned to 0
            if (is.na(mean_obj) || length(mean_obj) == 0) {
                mean_obj <- 0
            }

            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # If sd_obj ==0 or NA, the value is 0.001
            if (is.na(sd_obj) || sd_obj == 0 || length(sd_obj) == 0) {
                sd_obj <- 1
            }

            return(list(type=type, mean=mean_obj, sd=sd_obj, quantiles=quantile_obj, cont=as.numeric(cont)))

        } else if (type == "cat") {
            # Computational mode
            x <- df[[itemid]]
            x <- as.character(x)
            mode_obj <- names(which.max(table(x, useNA = "no")))

            uniq_value <- sort(unique(na.omit(x)), decreasing = F)

            # Count the number of all possible values
            unique_count <- length(uniq_value)

            # Returns a list of 2 objects
            return(list(type=type, mode=mode_obj, unique_count=unique_count, unique_values=uniq_value, cont=cont))
        } else if (type == "ord") {

            x <- df[[itemid]]
            x <- as.numeric(x)

            mode_obj <- names(which.max(table(x, useNA = "no")))

            # Calculate quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # Calculate standard deviation
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]

            mean_obj <- mean(x_in_range, na.rm = TRUE)

            # If mean_obj is NA, the value is assigned to 0
            if (is.na(mean_obj) || length(mean_obj) == 0) {
                mean_obj <- 0
            }

            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # If sd_obj ==0 or NA, the value is 0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type=type, mean=mean_obj, sd=sd_obj, quantiles=quantile_obj, cont=as.numeric(cont), mode=mode_obj))
        } else if (type == "bin") {
            return(list(type=type, cont=as.numeric(cont)))
        }
        }, 1:length(itemid_list), itemid_list, type_list, C_list, SIMPLIFY = F)

  names(stats)<-as.character(itemid_list)

  return(stats)
}

get_stat_long <- function(df, itemid_list, type_list, itemid_col, value_col, C_list) {

    stats<-mapply(function(i, itemid, type, cont){

        ind <- which(df[[itemid_col]] == itemid)

        if(type == "num"){
            x <- df[[value_col]][ind]
            x <- as.numeric(x)
            
            # Calculate quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # Calculate standard deviation
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]

            mean_obj <- mean(x_in_range, na.rm = TRUE)

            # If mean_obj is NA, the value is assigned to 0
            if (is.na(mean_obj) || length(mean_obj) == 0) {
                mean_obj <- 0
            }

            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # If sd_obj ==0 or NA, the value is 0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type=type, mean=mean_obj, sd=sd_obj, quantiles=quantile_obj, cont=as.numeric(cont)))

        } else if(type == "cat"){
            # Calculate mode
            x <- df[[value_col]][ind]
            x <- as.character(x)
            mode_obj <- names(which.max(table(x, useNA = "no")))

            uniq_value <- sort(unique(na.omit(x)), decreasing = F)

            # Count the number of all possible values
            unique_count <- length(uniq_value)

            # Returns a list of 2 objects
            return(list(type=type, mode=mode_obj, unique_count=unique_count, unique_values=uniq_value, cont=cont)) 
            
        } else if(type == "ord"){

            x <- df[[value_col]][ind]
            x <- as.numeric(x)
            
            mode_obj <- names(which.max(table(x, useNA = "no")))

            # Calculate quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]

            mean_obj <- mean(x_in_range, na.rm = TRUE)

            # If mean_obj is NA, the value is assigned to 0
            if (is.na(mean_obj) || length(mean_obj) == 0) {
                mean_obj <- 0
            }

            # Calculate standard deviation
            if (abs(mean_obj - quantile_obj[6]) > 0.05 * min(mean_obj, quantile_obj[6])) {
                sd_obj <- sd(x_in_range, na.rm = TRUE)
            } else {
                sd_obj <- sd(x, na.rm = TRUE)
            }

            # If sd_obj ==0 or NA, the value is 0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type=type, mean=mean_obj, sd=sd_obj, quantiles=quantile_obj, cont=as.numeric(cont), mode=mode_obj))

        } else if (type == "bin") {
            return(list(type=type, cont=as.numeric(cont)))
        }
        }, 1:length(itemid_list), itemid_list, type_list, C_list, SIMPLIFY = F)

  names(stats)<-as.character(itemid_list)

  return(stats)
}

get_type <- function(stat) {
    return(lapply(stat, function(x) x[[1]]) %>% unlist)
}

#######################################
# resampling
#######################################

get_first <- function(x, na.rm=T){
    if(na.rm) {
        x <- na.omit(x)
    }
    if(length(x) > 0) {
        return(head(x, 1))
    } else {
        return(NA)
    }
}

get_last <- function(x, na.rm=T){
    if(na.rm) {
        x <- na.omit(x)
    }
    if(length(x) > 0) {
        return(tail(x, 1))
    } else {
        return(NA)
    }
}

Min <- function(x, na.rm = FALSE) {
  x <- as.numeric(x)
  if (length(x) == 0 || (na.rm && all(is.na(x)))) {
    return(NA)
  } else {
    return(min(x, na.rm = na.rm))
  }
}

Max <- function(x, na.rm = FALSE) {
  x <- as.numeric(x)
  if (length(x) == 0 || (na.rm && all(is.na(x)))) {
    return(NA)
  } else {
    return(max(x, na.rm = na.rm))
  }
}

Mean <- function(x, na.rm = FALSE) {
  x <- as.numeric(x)
  if (length(x) == 0 || (na.rm && all(is.na(x)))) {
    return(NA)
  } else {
    return(mean(x, na.rm = na.rm))
  }
}

Median <- function(x, na.rm = FALSE) {
  x <- as.numeric(x)
  if (length(x) == 0 || (na.rm && all(is.na(x)))) {
    return(NA)
  } else {
    return(median(x, na.rm = na.rm))
  }
}

Any <- function(x, ...){
    x <- as.numeric(x)
    if(length(x) == 0) {
        return(NA)
    } else {
        return(ifelse(any(x == 1, ...), 1, 0))
    }
}

All <- function(x, ...){
    x <- as.numeric(x)
    if(length(x) == 0) {
        return(NA)
    } else {
        return(ifelse(all(x == 1, ...), 1, 0))
    }
}

Mode <- function(x, na.rm=T){
    # this function is used to calculate mode
    # Input:
    #   - x: A column of vectors or data frames used to calculate the mode
    # Output:
    #   - The value of the mode, which is of type character
    if(na.rm) x <- na.omit(x)
    if(length(x) == 0) {
        return(NA)
    }
    x <- as.character(x)
    tab <- table(x, useNA = "no")
    max_freq <- max(tab)
    r <- names(tab)[tab == max_freq]
    if (length(r) == 0) {
        return(NA)
    } else if (length(r) == 1) {
        return(r)
    } else {
        # Find the last occurrence of each mode in the original vector
        last_occurrences <- sapply(r, function(mode) max(which(x == mode)))
        # Return the mode that occurs last
        return(r[which.max(last_occurrences)])
    }
}

Mode_w <- function(x, w, na.rm=T){
    w <- as.numeric(w)
    if(na.rm) {
        na_index <- is.na(x) | is.na(w)
        x <- x[!na_index]
        w <- w[!na_index]
    }
    # 如果 x 或 w 全是 NA，或者长度为0，则返回 NA
    if(length(x) == 0 || length(w) == 0) {
        return(NA)
    }
    # Create a weighted histogram
    weighted_hist <- tapply(w, x, sum)
    # Find the x value corresponding to the maximum value in the weighted histogram
    modes <- names(weighted_hist)[weighted_hist == max(weighted_hist)]
    if (length(modes) == 0) return(NA) else return(modes)
}

mean_w <- function(x, w, na.rm=T){
    if(na.rm) {
        na_index <- is.na(x) | is.na(w)
        x <- x[!na_index]
        w <- w[!na_index]
    }
    # 如果 x 或 w 全是 NA，或者长度为0，则返回 NA
    if(length(x) == 0 || length(w) == 0) {
        return(NA)
    }
    # Calculate the weighted average
    weighted_mean <- sum(x * w) / sum(w)
    return(weighted_mean)
}

median_w <- function(x, w, na.rm=T){
    if(na.rm) {
        na_index <- is.na(x) | is.na(w)
        x <- x[!na_index]
        w <- w[!na_index]
    }
    # 如果 x 或 w 全是 NA，或者长度为0，则返回 NA
    if(length(x) == 0 || length(w) == 0) {
        return(NA)
    }
    # Calculate the weighted median
    ord <- order(x)
    cw <- cumsum(w[ord])
    p <- cw / sum(w)
    weighted_median <- x[ord][max(which(p <= 0.5))]
    return(weighted_median)
}

agg_f_dict <- list("mean" = Mean, "sum" = sum, "sum_w" = sum, 
                    "mode" = Mode, "mode_w" = Mode_w, 
                   "mean_w" = mean_w, "median_w" = median_w, 
                   "min" = Min, "max" = Max, "median" = Median, 
                   "first" = get_first, "last" = get_last,
                   "any" = Any, "all" = All)

resample_point_wide <- function(df, itemid_list, type_list, agg_f_list, time_list, time_col1, time_window, direction="both", keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        # 根据 direction 参数调整时间过滤条件
        if (direction == "both") {
            ind_time <- which(df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2))
        } else if (direction == "left") {
            ind_time <- which(df[[time_col1]] >= (cur_t - time_window) & df[[time_col1]] <= cur_t)
        } else if (direction == "right") {
            ind_time <- which(df[[time_col1]] >= cur_t & df[[time_col1]] <= (cur_t + time_window))
        }

        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid, type, agg_f) {
                            x <- ds_cur[[itemid]]
                            if (type == "num") {
                                x <- as.numeric(x)
                                return(agg_f_dict[[agg_f]](x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                return(agg_f_dict[[agg_f]](x, na.rm=T))
                            } else if (type == "bin"){
                                x <- as.numeric(x)
                                return(agg_f_dict[[agg_f]](x, na.rm=T))
                            }
                            }, itemid_list, type_list, agg_f_list, SIMPLIFY = T) %>% unlist
        # theta
        # if(sum(is.na(cur_x)) > theta, "0", "1")
        return(c("1", cur_x))

        }) %>% do.call(rbind, .)

    # Check whether mat is a matrix, and if not, convert it to a matrix
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat <- which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,,drop=F])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
        if(keep_first){
            mat[1,2] <- "1"
        } else{
            mat <- mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames

    return(mat)
}

resample_point_long <- function(df, itemid_list, type_list, agg_f_list, time_list, itemid_col, value_col, time_col1, time_window, direction="both", keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        # 根据 direction 参数调整时间过滤条件
        if (direction == "both") {
            ind_time <- which(df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2))
        } else if (direction == "left") {
            ind_time <- which(df[[time_col1]] >= (cur_t - time_window) & df[[time_col1]] <= cur_t)
        } else if (direction == "right") {
            ind_time <- which(df[[time_col1]] >= cur_t & df[[time_col1]] <= (cur_t + time_window))
        }

        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid, type, agg_f) {
                            ind <- which(ds_cur[[itemid_col]] == itemid)
                            x <- ds_cur[[value_col]][ind]
                            if (type == "num") {
                                x <- as.numeric(x)
                                return(agg_f_dict[[agg_f]](x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                return(agg_f_dict[[agg_f]](x, na.rm=T))
                            } else if (type == "bin"){
                                x <- as.numeric(x)
                                return(agg_f_dict[[agg_f]](x, na.rm=T))
                            }
                            }, itemid_list, type_list, agg_f_list, SIMPLIFY = T) %>% unlist
        # theta
        # if(sum(is.na(cur_x)) > theta, "0", "1")
        return(c("1", cur_x))

        }) %>% do.call(rbind, .)

    # Check whether mat is a matrix, and if not, convert it to a matrix
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat <- which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,,drop=F])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
        if(keep_first){
            mat[1,2] <- "1"
        } else{
            mat <- mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames

    return(mat)
}

resample_interval_wide <- function(df, itemid_list, type_list, agg_f_list, time_list, time_col1, time_col2, time_window, direction="both", keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        # 根据 direction 参数调整时间过滤条件
        if (direction == "both") {
            ind_time <- which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                               (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        } else if (direction == "left") {
            ind_time <- which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window) & df[[time_col1]] <= cur_t) |
                               (!is.na(df[[time_col2]]) & (df[[time_col1]] <= cur_t & df[[time_col2]] >= (cur_t - time_window)))))
        } else if (direction == "right") {
            ind_time <- which(((is.na(df[[time_col2]]) & df[[time_col1]] >= cur_t & df[[time_col1]] <= (cur_t + time_window)) |
                               (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window) & df[[time_col2]] >= cur_t))))
        }

        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
        ds_cur <- df[ind_time, ]

        # Calculate the length of the intersection
        if (direction == "both") {
            overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window/2) - pmax(ds_cur[[time_col1]], cur_t - time_window/2)
        } else if (direction == "left") {
            overlap <- pmin(ds_cur[[time_col2]], cur_t) - pmax(ds_cur[[time_col1]], cur_t - time_window)
        } else if (direction == "right") {
            overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window) - pmax(ds_cur[[time_col1]], cur_t)
        }
        overlap <- pmax(overlap, 0)

        # Calculates the length of [start_time, end_time]
        total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]

        # Calculate the ratios
        ds_cur$proportion <- overlap / total

        cur_x <- mapply(function(itemid, type, agg_f) {
            x <- ds_cur[[itemid]]
            if (type == "num") {
                x <- as.numeric(x)
                if(agg_f %in% c("mean_w", "median_w")) {
                    return(agg_f_dict[[agg_f]](x, overlap, na.rm=T))
                } else if(agg_f == "sum_w"){
                    x <- x * ds_cur$proportion
                    return(agg_f_dict[[agg_f]](x, na.rm=T))
                } else{
                    return(agg_f_dict[[agg_f]](x, na.rm=T))
                }
            } else if (type %in% c("cat","ord")){
                x <- as.character(x)
                if(agg_f == "mode_w") {
                    return(agg_f_dict[[agg_f]](x, overlap, na.rm=T))
                } else {
                    return(agg_f_dict[[agg_f]](x, na.rm=T))
                }
            } else if (type == "bin") {
                x <- as.numeric(x)
                return(agg_f_dict[[agg_f]](x))
            }
            }, itemid_list, type_list, agg_f_list, SIMPLIFY = T) %>% unlist
        # theta
        # if(sum(is.na(cur_x)) > theta, "0", "1")
        return(c("1", cur_x))
    }) %>% do.call(rbind, .)
    
    # Check whether mat is a matrix, and if not, convert it to a matrix
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1, 1] <- "1"

    if(!keepNArow) {
        ind_mat <- which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,,drop=F])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
        if(keep_first){
            mat[1,2] <- "1"
        } else{
            mat <- mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames
    return(mat)

}

resample_interval_long <- function(df, itemid_list, type_list, agg_f_list, time_list, itemid_col, value_col, time_col1, time_col2, time_window, direction="both", keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        # 根据 direction 参数调整时间过滤条件
        if (direction == "both") {
            ind_time <- which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                               (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        } else if (direction == "left") {
            ind_time <- which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window) & df[[time_col1]] <= cur_t) |
                               (!is.na(df[[time_col2]]) & (df[[time_col1]] <= cur_t & df[[time_col2]] >= (cur_t - time_window)))))
        } else if (direction == "right") {
            ind_time <- which(((is.na(df[[time_col2]]) & df[[time_col1]] >= cur_t & df[[time_col1]] <= (cur_t + time_window)) |
                               (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window) & df[[time_col2]] >= cur_t))))
        }

        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
        ds_cur <- df[ind_time, ]

        # Calculate the length of the intersection
        if (direction == "both") {
            overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window/2) - pmax(ds_cur[[time_col1]], cur_t - time_window/2)
        } else if (direction == "left") {
            overlap <- pmin(ds_cur[[time_col2]], cur_t) - pmax(ds_cur[[time_col1]], cur_t - time_window)
        } else if (direction == "right") {
            overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window) - pmax(ds_cur[[time_col1]], cur_t)
        }
        overlap <- pmax(overlap, 0)

        # Calculates the length of [start_time, end_time]
        total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]

        # Calculate the ratios
        ds_cur$proportion <- overlap / total

        cur_x <- mapply(function(itemid, type, agg_f) {
                            ind <- which(ds_cur[[itemid_col]] == itemid)
                            x <- ds_cur[[value_col]][ind]
                            if (type == "num") {
                                x <- as.numeric(x)
                                
                                if(agg_f %in% c("mean_w", "median_w")) {
                                    return(agg_f_dict[[agg_f]](x, overlap[ind], na.rm=T))
                                } else if(agg_f == "sum_w") {
                                    x <- x * ds_cur$proportion[ind]
                                    return(agg_f_dict[[agg_f]](x, na.rm=T))
                                } else{
                                    return(agg_f_dict[[agg_f]](x, na.rm=T))
                                }
                            } else if (type %in% c("cat", "ord")){
                                x <- as.character(x)
                                
                                if(agg_f == "mode_w") {
                                    return(agg_f_dict[[agg_f]](x, overlap[ind], na.rm=T))
                                } else{
                                    return(agg_f_dict[[agg_f]](x, na.rm=T))
                                }
                            } else if (type == "bin"){
                                x <- as.numeric(x)
                                
                                return(agg_f_dict[[agg_f]](x))
                            }
                        }, itemid_list, type_list, agg_f_list, SIMPLIFY = T) %>% unlist
        # theta
        # if(sum(is.na(cur_x)) > theta, "0", "1")
        return(c("1", cur_x))
    }) %>% do.call(rbind, .)
    
    # Check whether mat is a matrix, and if not, convert it to a matrix
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat <- which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,,drop=F])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
        if(keep_first){
            mat[1,2] <- "1"
        } else{
            mat <- mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames

    return(mat)

}


#######################################
# One hot
#######################################
to_onehot <- function(mat, col_list, time_col, type_list, stats){

    itemid_list <- colnames(mat)[col_list]
    mat1 <- mapply(function(col, name, type){
        if(type %in% c("num","ord","bin")){
            return(mat[,col,drop=F])
        }  else if(type=="cat") {
            N <- as.integer(stats[[name]][["unique_count"]])
            vec <- stats[[name]][["unique_values"]]
            X <- mapply(function(u){
                    x<-rep(0, N)
                    if(is.na(u)){
                        return(rep(NA, N))
                    } else{
                        x[which(u==vec)]<-1
                        return(x)
                    }
                }, mat[,col], SIMPLIFY=F) %>% do.call(rbind, .)
            colnames(X)<-paste0(name,"___",1:N)
            return(X)
        }
    }, col_list, itemid_list, type_list, SIMPLIFY = F) %>% do.call(cbind, .)
    
    row.names(mat1) <- NULL
    if (!is.null(time_col)) {
        mat1 <- cbind(mat[,time_col,drop=F], mat1)
    }
    return(mat1)
}

rev_onehot <- function(mat1, col_list, time_col, type_list, stats){
    # This function is used to restore the unique thermal code to the original data
    # input:
    #   - mat1: matrix, which contains the data after unique thermal coding
    #   - col_list: An integer vector representing the index of the variable column that is uniquely thermally encoded
    #   - time_col: An integer that represents the index of the time column
    #   - type_list: A string vector that represents a list of types for variables
    #   - stats: List, containing statistics for each variable
    # output:
    #   - matrix of the original data after restoration
    itemid_list <- names(stats)

    num_list <-mapply(function(type, stat){
        if (type %in% c("num","ord","bin")) return(1) else return(stat[[3]])
    }, type_list, stats, SIMPLIFY = T) %>% unlist

    mat <- lapply(1:nrow(mat1), function(r){
        # r<-1
        x <- mat1[r, col_list]
        cur <- 1
        x1 <- list()
        for(i in 1:length(num_list)){
            if(type_list[i] %in% c("num","ord","bin")){
                y <- x[cur]
                cur <- cur+1
                x1[[i]] <- y
            } else {
                vec <- stats[[i]][["unique_values"]]
                enc <- as.integer(x[cur:(cur+num_list[i]-1)])
                ind <- which(enc==1)
                if(length(ind) == 0){
                    y <- NA
                } else {
                    y <- vec[ind]
                }
                x1[[i]] <- y
                cur <- cur +num_list[i]
            }   
        }
        x1 <- unlist(x1)
        x1
    }) %>% do.call(rbind, .)

    mat <- cbind(mat1[,time_col,drop=F], mat)
    colnames(mat) <- c(colnames(mat1)[time_col], itemid_list)
    mat
}


#######################################
# Normalize
#######################################
norm_num <- function(mat, col_list, time_col, type_list, stats){

    itemid_list <- as.character(colnames(mat)[col_list])
    mat1 <- mapply(function(col, name, type){
        if(type %in% c("num","ord")) {
            m <- stats[[name]][["mean"]]
            s <- stats[[name]][["sd"]]
            x <- as.numeric(mat[, col])
            x1 <- round((x - m) / (s + 1e-6),10)
            return(x1)
        } else if(type %in% c("cat", "bin")) {
            return(mat[,col,drop=F])
        }
    }, col_list, itemid_list, type_list, SIMPLIFY = F) %>% do.call(cbind, .)

    if (!is.null(time_col)) {
        mat1 <- cbind(mat[,time_col,drop=F], mat1)
        row.names(mat1) <- NULL
        colnames(mat1) <- colnames(mat)[c(time_col, col_list)]
    } else {
        row.names(mat1) <- NULL
        colnames(mat1) <- colnames(mat)[col_list]
    }
    mat1
}


rev_normnum <- function(mat, col_list, time_col, type_list, stats){
    # The rev_normnum function is used to reverse normalize the normalized numerical variables
    # inputs:
    #   - mat: A matrix containing data that needs to be reverse-normalized
    #   - col_list: An integer vector representing the index of the variable column that needs to be reverse-normalized
    #   - time_col: An integer that represents the index of the time column
    #   - type_list: Character vector, representing the value type of each variable
    #   - stats: List，contains statistics (mean and standard deviation) for each numerical variable
    # output:
    #   - The matrix after inverse normalization
    itemid_list <- as.character(colnames(mat)[col_list])
    mat1 <- mapply(function(col, itemid, type){
        if(type %in% c("num","ord")) {
            m <- stats[[itemid]][["mean"]]
            s <- stats[[itemid]][["sd"]]
            x1 <- as.numeric(mat[,col,drop=F])
            x <- round(x1 * (s + 1e-6) + m, 10)
            return(x)
        } else if(type %in% c("cat","bin")) {
            return(mat[,col,drop=F])
        }
    }, col_list, itemid_list, type_list, SIMPLIFY = F) %>% do.call(cbind, .)
    mat1 <- cbind(mat[,time_col,drop=F], mat1)
    colnames(mat1) <- colnames(mat)[c(time_col, col_list)]
    mat1
}


#######################################
# fill missing value 
#######################################

fill <- function(mat, col_list, time_col, type_list, fill1_list, fill2_list, stats){
    
    itemid_list <- as.character(colnames(mat)[col_list])
    
    mat1 <- mapply(function(col, name, type, fill1, fill2){

        # name = "E";type="num";fill1="mean_k",fill2="lin";col=3

        if(type == "num") {
            m <- stats[[name]][["mean"]]
            m1 <- stats[[name]][["quantiles"]][6]
            cont <- stats[[name]][["cont"]]
            if (is.na(cont)) {
                cont <- m
            }

            x <- mat[,col,drop=T]
            ti <- mat[, time_col,drop=T]
            x <- as.numeric(x)
            ti <- as.numeric(ti)

            if(is.na(x[1])) {
                if(fill1 == "mean"){
                    x[1] <- m
                } else if(fill1 == "median"){
                    x[1] <- m1
                } else if(fill1 == "cont"){
                    x[1] <- cont
                } else if(fill1 == "locb"){
                    if (all(is.na(x))) {
                        x_locb <- cont
                    } else{
                        x_locb <- x[which(!is.na(x))[1]]
                    }
                    x[1] <- x_locb
                } else if(fill1 == "zero"){
                    x[1] <- 0
                } else if(fill1 == "mean_k"){
                    if (all(is.na(x))) {
                        x[1] <- cont
                    } else {
                        x[1] <- mean(x, na.rm = TRUE)
                    }
                } else if(fill1 == "median_k"){
                    if (all(is.na(x))) {
                        x[1] <- cont
                    } else {
                        x[1] <- median(x, na.rm = TRUE)
                    }
                } else {
                    stop("wrong fill1 method")
                }
            }

            if (fill2 == "lin") {
                x1 <- fill_lin(x, ti)
            } else if (fill2 == "zero"){
                x1 <- fill_cont(x, 0)
            } else if (fill2 == "locf"){
                x1 <- fill_locf(x)
            } else if (fill2 == "cont"){
                x1 <- fill_cont(x, cont)
            } else if (fill2 == "mean"){
                x1 <- fill_cont(x, m)
            } else if (fill2 == "median"){
                x1 <- fill_cont(x, m1)
            } else if (fill2 == "locb"){
                x1 <- fill_locb(x)
            } else if (fill2 == "mean_k"){
                if (all(is.na(x))) {
                    x1 <- fill_cont(x, cont)
                } else {
                    x1 <- fill_cont(x, mean(x, na.rm = TRUE))
                }
            } else if (fill2 == "median_k"){
                if (all(is.na(x))) {
                    x1 <- fill_cont(x, cont)
                } else {
                    x1 <- fill_cont(x, median(x, na.rm = TRUE))
                }
            } else{
                stop("wrong fill2 method")
            }
            return(cbind(x1))

        } else if(type=="cat") {
            mo <- stats[[name]][["mode"]]
            x <- mat[,col,drop=T]
            x <- as.character(x)
            cont <- stats[[name]][["cont"]]
            if (is.na(cont)) {
                cont <- mo
            }
            if(is.na(x[1])) {
                if(fill1 == "mode"){
                    x[1] <- mo
                } else if(fill1 == "cont"){
                    x[1] <- cont
                } else if(fill1 == "locb"){
                    if (all(is.na(x))) {
                        x_locb <- cont
                    } else{
                        x_locb <- x[which(!is.na(x))[1]]
                    }
                    x[1] <- x_locb
                } else if(fill1 == "zero"){
                    x[1] <- 0
                } else if(fill1 == "mode_k"){
                    if (all(is.na(x))) {
                        x[1] <- cont
                    } else {
                        x[1] <- Mode(x)
                    }
                } else {
                    stop("wrong fill1 method")
                }
            }

            if (fill2 == "zero"){
                x1 <- fill_cont(x, 0)
            } else if (fill2 == "locf"){
                x1 <- fill_locf(x)
            } else if (fill2 == "cont"){
                x1 <- fill_cont(x, cont)
            } else if (fill2 == "locb"){
                x1 <- fill_locb(x)
            } else if (fill2 == "mode_k"){
                if (all(is.na(x))) {
                    x1 <- fill_cont(x, cont)
                } else {
                    x1 <- fill_cont(x, Mode(x))
                }
            } else{
                stop("wrong fill2 method")
            }
            return(cbind(x1))

        } else if(type == "ord"){
            m <- stats[[name]][["mean"]]
            m1 <- stats[[name]][["quantiles"]][6]
            mo <- stats[[name]][["mode"]]
            cont <- stats[[name]][["cont"]]
            if (is.na(cont)) {
                cont <- m1
            }

            x <- mat[,col,drop=T]
            ti <- mat[, time_col,drop=T]
            ti <- as.numeric(ti)
            x <- as.numeric(x)

            if(is.na(x[1])) {
                if(fill1 == "mean"){
                    x[1] <- m
                } else if(fill1 == "median"){
                    x[1] <- m1
                } else if(fill1 == "mode"){
                    x[1] <- mo
                } else if(fill1 == "cont"){
                    x[1] <- cont
                } else if(fill1 == "locb"){
                    if (all(is.na(x))) {
                        x_locb <- cont
                    } else{
                        x_locb <- x[which(!is.na(x))[1]]
                    }
                    x[1] <- x_locb
                } else if(fill1 == "zero"){
                    x[1] <- 0
                } else if(fill1 == "mode_k"){
                    if (all(is.na(x))) {
                        x[1] <- cont
                    } else {
                        x[1] <- Mode(x)
                    }
                } else {
                    stop("wrong fill1 method")
                }
            }
            

            if (fill2 == "lin") {
                x1 <- fill_lin(x, ti)
            } else if (fill2 == "zero"){
                x1 <- fill_cont(x, 0)
            } else if (fill2 == "locf"){
                x1 <- fill_locf(x)
            } else if (fill2 == "cont"){
                x1 <- fill_cont(x, cont)
            } else if (fill2 == "mean"){
                x1 <- fill_cont(x, m)
            } else if (fill2 == "median"){
                x1 <- fill_cont(x, m1)
            } else if (fill2 == "locb"){
                x1 <- fill_locb(x)
            } else if (fill2 == "mode"){
                x1 <- fill_cont(x, mo)
            } else if (fill2 == "mean_k"){
                if (all(is.na(x))) {
                    x1 <- fill_cont(x, cont)
                } else {
                    x1 <- fill_cont(x, mean(x, na.rm = TRUE))
                }
            } else if (fill2 == "median_k"){
                if (all(is.na(x))) {
                    x1 <- fill_cont(x, cont)
                } else {
                    x1 <- fill_cont(x, median(x, na.rm = TRUE))
                }
            } else if (fill2 == "mode_k"){
                if (all(is.na(x))) {
                    x1 <- fill_cont(x, cont)
                } else {
                    x1 <- fill_cont(x, Mode(x))
                }
            } else{
                stop("wrong fill2 method")
            }
            return(cbind(x1))

        } else if(type == "bin"){
            x <- mat[,col,drop=T]
            x <- as.numeric(x)
            cont <- stats[[name]][["cont"]]
            if (is.na(cont)) {
                cont <- 0
            }
            if(is.na(x[1])) {
                if(fill1 == "cont"){
                    x[1] <- cont
                } else if(fill1 == "locb"){
                    if (all(is.na(x))) {
                        x_locb <- cont
                    } else{
                        x_locb <- x[which(!is.na(x))[1]]
                    }
                    x[1] <- x_locb
                } else if(fill1 == "zero"){
                    x[1] <- 0
                } else {
                    stop("wrong fill1 method")
                }
            }

            if (fill2 == "zero"){
                x1 <- fill_cont(x, 0)
            } else if (fill2 == "cont"){
                x1 <- fill_cont(x, cont)
            } else if (fill2 == "locf"){
                x1 <- fill_locf(x)
            } else if (fill2 == "locb"){
                x1 <- fill_locb(x)
            } else{
                stop("wrong fill value")
            }
            return(cbind(x1))
        }
    }, col_list, itemid_list, type_list, fill1_list, fill2_list, SIMPLIFY = F) %>% do.call(cbind, .)
    mat1 <- cbind(mat[,time_col,drop=F], mat1)
    row.names(mat1) <- NULL
    colnames(mat1)[2:ncol(mat1)] <- itemid_list
    mat1
}


fill_lin <- function(x, time) {
    # The fill_lin function is used to fill the linear interpolation
    # inputs:
    #   - x: Numerical vector representing the data for which linear interpolation is required
    #   - time: Numerical vector, representing time
    # output:
    #   - a value vector after linear interpolation.
    x <- as.numeric(x)
    time <- as.numeric(time)
    
    non_na_indices <- which(!is.na(x))  # Indices of non-NA values
    na_indices <- which(is.na(x))       # Indices of NA values
    
    if (length(na_indices) == 0) {
        return(x)
    }
    
    if (length(non_na_indices) == 0) {
        return(rep(NA, length(x)))
    }
    
    # Pre-allocated memory
    x_filled <- x
    
    x_filled[na_indices] <- sapply(na_indices, function(i) {
        left_indices <- non_na_indices[non_na_indices < i]  # Indices left of the NA
        right_indices <- non_na_indices[non_na_indices > i] # Indices right of the NA
        
        if (length(left_indices) > 0) {
            last_left_index <- max(left_indices)  # Most recent non-NA on the left
            if (length(right_indices) > 0) {
                first_right_index <- min(right_indices)  # Nearest non-NA on the right
                # Linear interpolation formula
                return((x[first_right_index] - x[last_left_index]) / (time[first_right_index] - time[last_left_index]) * (time[i] - time[last_left_index]) + x[last_left_index])
            } else {
                return(x[last_left_index])
            }
        } else {
            if (length(right_indices) > 0) {
                first_right_index <- min(right_indices)  # Nearest non-NA on the right
                return(x[first_right_index])
            } else {
                return(NA)
            }
        }
    })
    
    return(x_filled)
}

fill_locf <- function(x) {
    # The fill_locf function is used to fill missing values with the most recent observations
    # inputs:
    #   - x: Vector representing data that needs to be filled with missing values
    # output:
    #   - Fill the data after the missing value. The type is vector
    
    non_na_indices <- which(!is.na(x))  # Indices of non-NA values
    na_indices <- which(is.na(x))       # Indices of NA values
    
    if(length(na_indices) == 0) {
        return(x)
    }
    
    if(length(non_na_indices) == 0) {
        return(rep(NA, length(x)))
    }
    
    x_filled <- x
    
    x_filled[na_indices] <- sapply(na_indices, function(i) {
        left_indices <- non_na_indices[non_na_indices < i]  # Indices left of the NA
        right_indices <- non_na_indices[non_na_indices > i] # Indices right of the NA
        
        if(length(left_indices) > 0) {
            last_left_index <- max(left_indices)  # Location of the most recent observation on the left
            return(x[last_left_index])
        } else if(length(right_indices) > 0) {
            first_right_index <- min(right_indices)  # Location of the most recent observations on the right
            return(x[first_right_index])
        } else {
            return(NA)
        }
    })
    
    return(x_filled)
}

fill_locb <- function(x) {
    # fill_locb 函数用于用最近的后续观察值填充缺失值
    # 输入:
    #   - x: 需要填充缺失值的数据向量
    # 输出:
    #   - 填充后的数据向量

    non_na_indices <- which(!is.na(x))  # 非缺失值的索引
    na_indices <- which(is.na(x))       # 缺失值的索引
    
    if(length(na_indices) == 0) {
        return(x)
    }
    
    if(length(non_na_indices) == 0) {
        return(rep(NA, length(x)))
    }
    
    x_filled <- x
    
    x_filled[na_indices] <- sapply(na_indices, function(i) {
        left_indices <- non_na_indices[non_na_indices < i]  # 左侧非缺失值的索引
        right_indices <- non_na_indices[non_na_indices > i] # 右侧非缺失值的索引
        
        if(length(right_indices) > 0) {
            first_right_index <- min(right_indices)  # 最近的后续观察位置
            return(x[first_right_index])
        } else if(length(left_indices) > 0) {
            last_left_index <- max(left_indices)  # 最近的前序观察位置
            return(x[last_left_index])
        } else {
            return(NA)
        }
    })
    
    return(x_filled)
}

fill_cont <- function(x, cont) {
    # The fill_cont function is used to fill the missing value with a specified value
    # inputs:
    #   - x: Vector representing data that needs to be filled with missing values
    #   - cont: The value used to fill the missing values
    # output:
    #   - Fill the data after the missing value. The type is vector
    x_filled <- x
    x_filled[is.na(x_filled)] <- cont
    return(x_filled)
}

fill_last_values <- function(mat, mask, col_list, time_col, var_dict) {
    # 生成一个新的数据框架，其中每列的缺失值从最后一个非缺失值开始被填充
    mat1 <- lapply(1:length(col_list), function(i) {
        j1 <- col_list[i]
        ind <- which(mask[, j1] == 1)
        if (length(ind) < 1) {
            return(mat[, j1, drop = F])
        }
        first_ind <- ind[length(ind)] + 1
        # 如果 first_ind 本身就是最后一个观测，则不做任何处理
        if (first_ind > nrow(mat)) {
            return(mat[, j1, drop = F])  # 返回原始列
        }
        if (!is.na(var_dict$last_value[i])) {
            mat[first_ind:nrow(mat), j1] <- var_dict$last_value[i]
        }
        return(mat[, j1, drop = F])
    }) %>% do.call(cbind, .)

    # 合并时间列和处理后的数据列
    mat1 <- cbind(mat[, time_col, drop = F], mat1)
    
    # 获取列名并设置列名，确保时间列和数据列的名称正确
    itemid_list <- as.character(colnames(mat)[col_list])
    colnames(mat1) <- c(colnames(mat)[time_col], itemid_list)
    
    return(mat1)
}

#######################################
# mask and delta_time
#######################################
get_mask <- function(mat, col_list, time_col){
    # The get_mask function is used to generate a missing value mask
    # Inputs:
    #   - mat: A matrix containing the data needed to generate the missing value mask
    #   - col_list: An integer vector representing the index of the variable column that needs to generate the missing value mask
    #   - time_col: An integer that represents the index of the time column
    # Output:
    #   - Generated missing value mask matrix
    itemid_list <- as.character(colnames(mat)[col_list])
    mat1 <- rbind(apply(mat[,col_list,drop=F], 2, function(x) ifelse(is.na(x), 0, 1)))
    mat1 <- cbind(mat[,time_col,drop=F], mat1)
    colnames(mat1)[2:ncol(mat1)] <- itemid_list
    mat1
}

get_deltamat <- function(mat, col_list, time_col) {

  itemid_list <- as.character(colnames(mat)[col_list])

  # Suppose s is a matrix, and m is the second to last column of mat
  s <- as.numeric(mat[,time_col])
  m <- mat[,col_list,drop=F]

  # Initialize delta
  delta <- matrix(0, nrow = nrow(m), ncol = ncol(m))

  if (nrow(m) > 1) {
    # Calculate the delta for each point in time
    for (t in 2:nrow(delta)) {
        for (d in 1:ncol(delta)) {
        if (m[t-1, d] == "0") {
            delta[t, d] <- s[t] - s[t-1] + delta[t-1, d]
        } else {
            delta[t, d] <- s[t] - s[t-1]
        }
        }
    }
  }

  # output delta
  delta<- cbind(mat[, time_col, drop = FALSE], delta)
  colnames(delta)[2:ncol(delta)] <- itemid_list
  return(delta)
}

shape_as_onehot <- function(mat, col_list, time_col, type_list, stats) {
    itemid_list <- mapply(function(stat, name){
        if(stat[[1]] %in% c("num", "ord","bin")){
            return(name)
        } else {
            return(paste0(name, "___", 1:stat[[3]]))
        }
    }, stats, names(stats)) %>% unlist

    num_list <-mapply(function(type, stat){
        if (type %in% c("num", "ord","bin")) return(1) else return(stat[[3]])
    }, type_list, stats, SIMPLIFY = T) %>% unlist

    mat1 <- lapply(1:length(col_list), function(i) {
        if (type_list[i] %in% c("num", "ord", "bin")) {
            return(mat[, col_list[i], drop = F])  # 直接返回列
        } else if (type_list[i] == "cat") {
            return(matrix(rep(mat[, col_list[i],drop=T], num_list[i]), ncol = num_list[i]))  # 重复列 num_list[i] 次
        }
    }) %>% do.call(cbind, .)  # 将所有处理后的列合并

    # 将时间列与处理后的数据合并
    mat1 <- cbind(mat[, time_col, drop = F], mat1)
    # 设置列名，包括时间列和所有变量列
    colnames(mat1) <- c(colnames(mat)[time_col], itemid_list)
    mat1  # 返回处理后的矩阵
}


#######################################
# rename itemid
#######################################

# 重命名宽表格式的数据框
rename_wide_table <- function(ds, old_name_list, new_name_list) {
  if (length(old_name_list) != length(new_name_list)) {
    stop("old_name_list and new_name_list must have the same length")
  }
  
  # 检查并替换列名，仅当新旧名称不同时
  for (i in seq_along(old_name_list)) {
    if (old_name_list[i] != new_name_list[i] && old_name_list[i] %in% names(ds)) {
      names(ds)[names(ds) == old_name_list[i]] <- new_name_list[i]
    }
  }
  
  return(ds)
}

# 重命名长表格式的数据框
rename_long_table <- function(ds, old_name_list, new_name_list, name_col) {
  if (length(old_name_list) != length(new_name_list)) {
    stop("old_name_list and new_name_list must have the same length")
  }
  
  # 检查并替换 name_col 中的值，仅当新旧名称不同时
  for (i in seq_along(old_name_list)) {
    if (old_name_list[i] != new_name_list[i]) {
      ds[[name_col]][which(ds[[name_col]] == old_name_list[i])] <- new_name_list[i]
    }
  }
  
  return(ds)
}

#######################################
# remove extreme value
#######################################
remove_extreme_value_long <- function(df, itemid_list, type_list, itemid_col, value_col, var_dict, sep = "\\|") {
    df_new <- mapply(function(i, itemid, type) {
        ind <- which(df[[itemid_col]] == itemid)
        df_cur <- df[ind, ]
        if (type == "num") {
            x <- as.numeric(df_cur[[value_col]])
            low <- var_dict$low[i]
            high <- var_dict$high[i]
            if (!is.na(low)) {
                x[x < low] <- NA
            }
            if (!is.na(high)) {
                x[x > high] <- NA
            }
            df_cur[[value_col]] <- x
            return(df_cur)
        } else if (type %in% c("ord", "cat")) {
            valid_values <- var_dict$valid_value[i]
            if (!is.na(valid_values)) {
                valid_values <- unlist(strsplit(valid_values, sep))
                x <- as.character(df_cur[[value_col]])
                x[!x %in% valid_values] <- NA
                df_cur[[value_col]] <- x
            }
            return(df_cur)
        } else if (type == "bin") {
            return(df_cur)
        }
    }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F) %>% do.call(rbind, .)
    return(df_new)
}


remove_extreme_value_wide <- function(df, itemid_list, type_list, cols_keep, var_dict, sep = "\\|") {
    mat <- mapply(function(i, itemid, type) {
        if (type == "num") {
            x <- as.numeric(df[[itemid]])
            low <- var_dict$low[i]
            high <- var_dict$high[i]
            if (!is.na(low)) {
                x[x < low] <- NA
            }
            if (!is.na(high)) {
                x[x > high] <- NA
            }
            return(x)
        } else if (type %in% c("ord", "cat")) {
            valid_values <- var_dict$valid_value[i]
            if (!is.na(valid_values)) {
                valid_values <- unlist(strsplit(valid_values, sep))
                x <- as.character(df[[itemid]])
                x[!x %in% valid_values] <- NA
                return(x)
            } else {
                return(df[[itemid]])
            }
        } else if (type == "bin") {
            return(df[[itemid]])
        }
    }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F) %>% do.call(cbind, .)
    mat1 <- cbind(df[, cols_keep, drop = F], mat)
    row.names(mat1) <- NULL
    colnames(mat1)[(length(cols_keep) + 1):ncol(mat1)] <- itemid_list
    return(as.data.frame(mat1))
}


#######################################
# data aggregate
#######################################
calculate_stats <- function(mat, col_list, type_list) {
    name_list <- colnames(mat)
    stats <- c()
    
    for (i in seq_along(col_list)) {
        col <- col_list[i]
        type <- type_list[i]
        if (type == "num") {
            numeric_col <- as.numeric(mat[, col])
            if (all(is.na(numeric_col))) {
                stats <- c(stats, setNames(c(NA, NA, NA), paste(name_list[col], c("_min", "_median", "_max"), sep = "")))
            } else {
                stats <- c(stats, setNames(c(min(numeric_col, na.rm = TRUE), median(numeric_col, na.rm = TRUE), max(numeric_col, na.rm = TRUE)), paste(name_list[col], c("_min", "_median", "_max"), sep = "")))
            }
        } else if (type %in% c("cat", "ord")) {
            cat_col <- mat[, col]
            mode_val <- Mode(cat_col)
            stats <- c(stats, setNames(mode_val, paste(name_list[col], "_mode", sep = "")))
        } else if (type  == "bin") {
            bin_col <- as.numeric(mat[, col])
            if (all(is.na(bin_col))) {
                stats <- c(stats, setNames(NA, paste(name_list[col], "_mean", sep = "")))
            } else {
                stats <- c(stats, setNames(mean(bin_col, na.rm = TRUE), paste(name_list[col], "_mean", sep = "")))
            }
        }
    }
    
    mat1 <- rbind(stats)
    row.names(mat1) <- NULL
    return(mat1)
}
