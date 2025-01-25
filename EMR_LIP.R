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

get_stat_wide <- function(df, itemid_list, type_list, C_list, var_dict, sep = "\\|") {
    stats <- mapply(function(i, itemid, type, cont) {
        # 获取数据
        values <- df[[itemid]]
        
        if (type == "num") {
            values <- as.numeric(values)
            mean_obj <- mean(values, na.rm = TRUE)
            median_obj <- median(values, na.rm = TRUE)
            sd_obj <- sd(values, na.rm = TRUE)
            if (is.na(mean_obj) || length(mean_obj) == 0) {
                mean_obj <- 0
            }
            if (is.na(median_obj) || length(median_obj) == 0) {
                median_obj <- 0
            }
            if (is.na(sd_obj) || length(sd_obj) == 0) {
                sd_obj <- 1
            }
            
            # cont 值优先使用传入的值，如果为 NA 则使用 mean
            cont_value <- if (!is.na(cont)) as.numeric(cont) else mean_obj
            
            return(list(
                type = type,
                mean = mean_obj,
                median = median_obj,
                sd = sd_obj,
                cont = cont_value
            ))
            
        } else if (type %in% c("cat", "ord")) {  # 合并处理 cat 和 ord
            values <- as.character(values)
            mode_obj <- Mode(values)
            
            # 从 var_dict 获取 valid_value
            valid_values <- var_dict$valid_value[i]
            if (!is.na(valid_values)) {
                # 使用自定义分隔符分割并去除空白字符
                uniq_value <- sort(trimws(unlist(strsplit(valid_values, sep))))
            } else {
                # 如果 valid_value 为空则使用数据中的唯一值
                uniq_value <- sort(unique(na.omit(values)))
            }
            
            # cont 值优先使用传入的值，如果为 NA 则使用 mode
            cont_value <- if (!is.na(cont)) cont else mode_obj
            
            return(list(
                type = type,
                mode = mode_obj,
                unique_values = uniq_value,
                cont = cont_value
            ))
            
        } else if (type == "bin") {
            values <- as.numeric(values)
            # 对于二值型变量，cont 默认为 0
            cont_value <- if (!is.na(cont)) as.numeric(cont) else 0
            
            return(list(
                type = type,
                cont = cont_value
            ))
        }
    }, 1:length(itemid_list), itemid_list, type_list, C_list, SIMPLIFY = F)
    
    names(stats) <- as.character(itemid_list)
    return(stats)
}

get_stat_long <- function(df, itemid_list, type_list, itemid_col, value_col, C_list, var_dict, sep = "\\|") {
    stats <- mapply(function(i, itemid, type, cont) {
        # 获取数据
        ind <- which(df[[itemid_col]] == itemid)
        values <- df[[value_col]][ind]
        
        if (type == "num") {
            values <- as.numeric(values)
            mean_obj <- mean(values, na.rm = TRUE)
            median_obj <- median(values, na.rm = TRUE)
            sd_obj <- sd(values, na.rm = TRUE)
            if (is.na(mean_obj) || length(mean_obj) == 0) {
                mean_obj <- 0
            }
            if (is.na(median_obj) || length(median_obj) == 0) {
                median_obj <- 0
            }
            if (is.na(sd_obj) || length(sd_obj) == 0) {
                sd_obj <- 1
            }
            
            # cont 值优先使用传入的值，如果为 NA 则使用 mean
            cont_value <- if (!is.na(cont)) as.numeric(cont) else mean_obj
            
            return(list(
                type = type,
                mean = mean_obj,
                median = median_obj,
                sd = sd_obj,
                cont = cont_value
            ))
            
        } else if (type %in% c("cat", "ord")) {  # 合并处理 cat 和 ord
            values <- as.character(values)
            mode_obj <- Mode(values)
            
            # 从 var_dict 获取 valid_value
            valid_values <- var_dict$valid_value[i]
            if (!is.na(valid_values)) {
                # 使用自定义分隔符分割并去除空白字符
                uniq_value <- sort(trimws(unlist(strsplit(valid_values, sep))))
            } else {
                # 如果 valid_value 为空则使用数据中的唯一值
                uniq_value <- sort(unique(na.omit(values)))
            }
            
            # cont 值优先使用传入的值，如果为 NA 则使用 mode
            cont_value <- if (!is.na(cont)) cont else mode_obj
            
            return(list(
                type = type,
                mode = mode_obj,
                unique_values = uniq_value,
                cont = cont_value
            ))
            
        } else if (type == "bin") {
            values <- as.numeric(values)
            # 对于二值型变量，cont 默认为 0
            cont_value <- if (!is.na(cont)) as.numeric(cont) else 0
            
            return(list(
                type = type,
                cont = cont_value
            ))
        }
    }, 1:length(itemid_list), itemid_list, type_list, C_list, SIMPLIFY = F)
    
    names(stats) <- as.character(itemid_list)
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


resample_wide <- function(df, itemid_list, type_list, agg_f_list, time_list, 
                         time_col1, time_col2=NULL, time_window, 
                         direction="both", keepNArow=F, keep_first=T) {
    
    # 创建初始数据框
    result_df <- data.frame(
        time = time_list,
        keep = "0",
        stringsAsFactors = FALSE
    )
    
    # 使用 sapply 初始化变量列
    result_df[itemid_list] <- sapply(seq_along(itemid_list), function(i) {
        if(type_list[i] == "num") rep(NA_real_, length(time_list))
        else if(type_list[i] %in% c("cat", "ord")) rep(NA_character_, length(time_list))
        else if(type_list[i] == "bin") rep(NA_integer_, length(time_list))
    })
    
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
        
        # 如果过滤后没有观测值,返回第一个时间点的全 NA 行
        if(nrow(mat) == 0) {
            result_df <- data.frame(
                time = time_list[1],
                keep = "0",
                stringsAsFactors = FALSE
            )
            result_df[itemid_list] <- NA
            # 确保正确的数据类型
            for(i in seq_along(itemid_list)) {
                if(type_list[i] == "num") {
                    result_df[[itemid_list[i]]] <- as.numeric(NA)
                } else if(type_list[i] == "bin") {
                    result_df[[itemid_list[i]]] <- as.integer(NA)
                } else {
                    result_df[[itemid_list[i]]] <- as.character(NA)
                }
            }
            return(result_df)
        }
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

resample_long <- function(df, itemid_list, type_list, agg_f_list, time_list,
                         time_col1, time_col2=NULL, itemid_col, value_col,
                         time_window, direction="both", keepNArow=F, keep_first=T) {
    
    # 创建初始数据框
    result_df <- data.frame(
        time = time_list,
        keep = "0",
        stringsAsFactors = FALSE
    )
    
    # 使用 sapply 初始化变量列
    result_df[itemid_list] <- sapply(seq_along(itemid_list), function(i) {
        if(type_list[i] == "num") rep(NA_real_, length(time_list))
        else if(type_list[i] %in% c("cat", "ord")) rep(NA_character_, length(time_list))
        else if(type_list[i] == "bin") rep(NA_integer_, length(time_list))
    })
    
    # 使用 vapply 处理每个时间点
    results <- vapply(seq_along(time_list), function(t) {
        cur_t <- time_list[t]
        
        # 时间窗口过滤
        ind_time <- if (is.null(time_col2)) {
            switch(direction,
                "both" = which(df[[time_col1]] >= (cur_t - time_window/2) & 
                          df[[time_col1]] <= (cur_t + time_window/2)),
                "left" = which(df[[time_col1]] >= (cur_t - time_window) & 
                             df[[time_col1]] <= cur_t),
                "right" = which(df[[time_col1]] >= cur_t & 
                              df[[time_col1]] <= (cur_t + time_window))
            )
        } else {
            switch(direction,
                "both" = which((is.na(df[[time_col2]]) & 
                               df[[time_col1]] >= (cur_t - time_window/2) & 
                               df[[time_col1]] <= (cur_t + time_window/2)) |
                              (!is.na(df[[time_col2]]) & 
                               df[[time_col1]] <= (cur_t + time_window/2) & 
                               df[[time_col2]] >= (cur_t - time_window/2))),
                "left" = which((is.na(df[[time_col2]]) & 
                              df[[time_col1]] >= (cur_t - time_window) & 
                              df[[time_col1]] <= cur_t) |
                             (!is.na(df[[time_col2]]) & 
                              df[[time_col1]] <= cur_t & 
                              df[[time_col2]] >= (cur_t - time_window))),
                "right" = which((is.na(df[[time_col2]]) & 
                               df[[time_col1]] >= cur_t & 
                               df[[time_col1]] <= (cur_t + time_window)) |
                              (!is.na(df[[time_col2]]) & 
                               df[[time_col1]] <= (cur_t + time_window) & 
                               df[[time_col2]] >= cur_t))
            )
        }
        
        if(length(ind_time) == 0) {
            return(c("0", rep(NA, length(itemid_list))))
        }
        
        ds_cur <- df[ind_time, ]
        
        # 计算重叠部分
        if (!is.null(time_col2)) {
            overlap <- switch(direction,
                "both" = pmin(ds_cur[[time_col2]], cur_t + time_window/2) - 
                         pmax(ds_cur[[time_col1]], cur_t - time_window/2),
                "left" = pmin(ds_cur[[time_col2]], cur_t) - 
                         pmax(ds_cur[[time_col1]], cur_t - time_window),
                "right" = pmin(ds_cur[[time_col2]], cur_t + time_window) - 
                          pmax(ds_cur[[time_col1]], cur_t)
            )
            overlap <- pmax(overlap, 0)
            total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]
            ds_cur$proportion <- overlap / total
        }
        
        # 使用 sapply 处理每个变量
        values <- sapply(seq_along(itemid_list), function(i) {
            itemid <- itemid_list[i]
            type <- type_list[i]
            agg_f <- agg_f_list[i]
            
            ind_var <- which(ds_cur[[itemid_col]] == itemid)
            if(length(ind_var) == 0) return(NA)
            
            x <- ds_cur[[value_col]][ind_var]
            
            if(type == "num") {
                x <- as.numeric(x)
                if(!is.null(time_col2) && agg_f %in% c("mean_w", "median_w", "mode_w")) {
                    agg_f_dict[[agg_f]](x, overlap[ind_var], na.rm=T)
                } else if(!is.null(time_col2) && agg_f == "sum_w") {
                    agg_f_dict[[agg_f]](x * ds_cur$proportion[ind_var], na.rm=T)
                } else {
                    agg_f_dict[[agg_f]](x, na.rm=T)
                }
            } else if(type %in% c("cat", "ord")) {
                if(!is.null(time_col2) && agg_f == "mode_w") {
                    agg_f_dict[[agg_f]](x, overlap[ind_var], na.rm=T)
                } else {
                    agg_f_dict[[agg_f]](x, na.rm=T)
                }
            } else if(type == "bin") {
                agg_f_dict[[agg_f]](as.integer(x), na.rm=T)
            }
        })
        
        c("1", values)
    }, FUN.VALUE = character(length(itemid_list) + 1), USE.NAMES = FALSE)
    
    # 更新结果数据框
    result_df$keep <- results[1,]
    result_df[itemid_list] <- t(results[-1,])
    
    # 转换数据类型
    for(i in seq_along(itemid_list)) {
        if(type_list[i] == "num") {
            result_df[[itemid_list[i]]] <- as.numeric(result_df[[itemid_list[i]]])
        } else if(type_list[i] == "bin") {
            result_df[[itemid_list[i]]] <- as.integer(result_df[[itemid_list[i]]])
        }
    }
    
    # 处理 keepNArow 和 keep_first
    if(!keepNArow) {
        result_df <- result_df[result_df$keep == "1", ]
        
        # 如果过滤后没有观测值,返回第一个时间点的全 NA 行
        if(nrow(result_df) == 0) {
            result_df <- data.frame(
                time = time_list[1],
                keep = "0",
                stringsAsFactors = FALSE
            )
            result_df[itemid_list] <- NA
            # 确保正确的数据类型
            for(i in seq_along(itemid_list)) {
                if(type_list[i] == "num") {
                    result_df[[itemid_list[i]]] <- as.numeric(NA)
                } else if(type_list[i] == "bin") {
                    result_df[[itemid_list[i]]] <- as.integer(NA)
                } else {
                    result_df[[itemid_list[i]]] <- as.character(NA)
                }
            }
            return(result_df)
        }
    }
    
    if(nrow(result_df) > 1 && all(is.na(result_df[1, itemid_list]))) {
        if(keep_first) {
            result_df$keep[1] <- "0"
        } else {
            result_df <- result_df[-1, ]
        }
    }
    
    return(result_df)
}

#######################################
# One hot
#######################################
to_onehot <- function(df, col_list, time_col = NULL, type_list, stats) {
    # 初始化结果数据框
    if (is.null(time_col) || is.na(time_col)) {
        result_df <- data.frame(row.names = 1:nrow(df))  # 使用行号作为行名
    } else {
        result_df <- data.frame(time = df[[time_col]])
    }
    
    # 使用 mapply 处理每个变量
    encoded_cols <- mapply(function(col, name, type) {
        if (type %in% c("num","bin")) {
            return(df[, col, drop=FALSE])
        } else if (type %in% c("ord", "cat")) {
            N <- length(stats[[name]][["unique_values"]])
            vec <- stats[[name]][["unique_values"]]
            
            # 使用 sapply 创建 one-hot 编码
            onehot_matrix <- sapply(1:N, function(i) {
                as.integer(df[[col]] == vec[i])
            })
            # 确保单行数据时返回矩阵格式
            if (!is.matrix(onehot_matrix)) {
                onehot_matrix <- matrix(onehot_matrix, nrow=1)
            }
            onehot_df <- as.data.frame(onehot_matrix)
            colnames(onehot_df) <- paste0(name, "___", 1:N)
            return(onehot_df)
        }
    }, col_list, names(df)[col_list], type_list, SIMPLIFY = FALSE)
    
    # 合并所有列
    if (length(result_df) == 0) {
        result_df <- do.call(cbind, encoded_cols)  # 如果没有时间列，直接合并编码列
    } else {
        result_df <- cbind(result_df, do.call(cbind, encoded_cols))  # 有时间列，则包含时间列
    }
    
    return(result_df)
}

rev_onehot <- function(df, col_list, time_col = NULL, type_list, stats) {
    # 初始化结果数据框和偏移量
    if (is.null(time_col) || is.na(time_col)) {
        result_df <- data.frame(row.names = 1:nrow(df))
        time_offset <- 0
    } else {
        result_df <- data.frame(time = df[[time_col]])
        time_offset <- 1
    }
    
    # col_list 直接包含原始变量名
    orig_names <- col_list
    
    # 使用 mapply 处理每个变量
    decoded_cols <- mapply(function(orig_name, type) {
        if (type %in% c("num", "bin")) {
            # 直接使用原始变量名
            return(df[, orig_name, drop=FALSE])
        } else if (type %in% c("ord", "cat")) {
            # 找到所有属于当前变量的列
            pattern <- paste0("^", orig_name, "___")
            matched_cols <- grep(pattern, names(df))
            
            if (length(matched_cols) == 0) {
                stop(paste("No columns found for variable:", orig_name))
            }
            
            vec <- stats[[orig_name]][["unique_values"]]
            
            # 获取 one-hot 编码列
            onehot_cols <- as.matrix(df[, matched_cols])
            # 确保单行数据时是矩阵格式
            if (!is.matrix(onehot_cols)) {
                onehot_cols <- matrix(onehot_cols, nrow=1)
            }
            
            # 转换回分类变量
            cat_var <- apply(onehot_cols, 1, function(row) {
                if (all(is.na(row))) return(NA)
                ind <- which(row == 1)
                if (length(ind) == 0) return(NA)
                vec[ind[1]]
            })
            
            return(data.frame(cat_var))
        }
    }, orig_names, type_list, SIMPLIFY = FALSE)
    
    # 合并所有列
    if (length(result_df) == 0) {
        result_df <- do.call(cbind, decoded_cols)
    } else {
        result_df <- cbind(result_df, do.call(cbind, decoded_cols))
    }
    
    # 设置列名
    if (is.null(time_col) || is.na(time_col)) {
        colnames(result_df) <- orig_names
    } else {
        colnames(result_df)[-1] <- orig_names
    }
    
    return(result_df)
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

fill <- function(df, col_list, type_list, fill1_list, fill2_list, stats, time_col = "time") {
    
    # 参数检查
    if (length(col_list) != length(type_list) || 
        length(col_list) != length(fill1_list) || 
        length(col_list) != length(fill2_list)) {
        stop("Length mismatch in input parameters")
    }
    
    # 创建结果数据框
    result_df <- data.frame(time = df[[time_col]])
    
    # 获取变量名列表
    itemid_list <- names(df)[col_list]
    if (any(is.na(itemid_list))) {
        stop("Some column indices in col_list are out of range for stats names")
    }
    
    # 使用 mapply 处理每个变量
    filled_cols <- mapply(function(col, name, type, fill1, fill2) {
        x <- df[[col]]
        ti <- df[[time_col]]
        
        # 检查统计信息是否存在
        if (is.null(stats[[name]])) {
            stop(sprintf("Statistics not found for variable: %s", name))
        }
        
        # 根据变量类型获取统计量
        if(type == "num") {
            m <- stats[[name]][["mean"]]
            m1 <- stats[[name]][["median"]]
            mo <- stats[[name]][["mode"]]
            cont <- stats[[name]][["cont"]]
            
            # 处理第一个缺失值
            if(is.na(x[1])) {
                x[1] <- switch(fill1,
                    "mean" = m,
                    "median" = m1,
                    "cont" = cont,
                    "mean_k" = if(all(is.na(x))) cont else mean(x, na.rm = TRUE),
                    "median_k" = if(all(is.na(x))) cont else median(x, na.rm = TRUE),
                    "mode_k" = if(all(is.na(x))) cont else Mode(x),
                    "zero" = 0,
                    "nocb" = if(all(is.na(x))) cont else x[which(!is.na(x))[1]],  # 添加 nocb 的情况
                    stop(sprintf("wrong fill1 method: %s for num type", fill1))
                )
            }
            
            # 处理其他缺失值
            x1 <- switch(fill2,
                "lin" = fill_lin(x, ti),
                "zero" = fill_cont(x, 0),
                "locf" = fill_locf(x),
                "cont" = fill_cont(x, cont),
                "mean" = fill_cont(x, m),
                "median" = fill_cont(x, m1),
                "mean_k" = if(all(is.na(x))) fill_cont(x, cont) else fill_cont(x, mean(x, na.rm = TRUE)),
                "median_k" = if(all(is.na(x))) fill_cont(x, cont) else fill_cont(x, median(x, na.rm = TRUE)),
                "mode_k" = if(all(is.na(x))) fill_cont(x, cont) else fill_cont(x, Mode(x)),
                stop(sprintf("wrong fill2 method: %s for num type", fill2))
            )
            
        } else if(type %in% c("ord", "cat")) {
            mo <- stats[[name]][["mode"]]
            cont <- stats[[name]][["cont"]]
            
            # 处理第一个缺失值
            if(is.na(x[1])) {
                x[1] <- switch(fill1,
                    "mode" = mo,
                    "cont" = cont,
                    "nocb" = if(all(is.na(x))) cont else x[which(!is.na(x))[1]],
                    "zero" = 0,
                    "mode_k" = if(all(is.na(x))) cont else Mode(x),
                    stop(sprintf("wrong fill1 method: %s for %s type", fill1, type))
                )
            }
            
            # 处理其他缺失值
            x1 <- switch(fill2,
                "zero" = fill_cont(x, 0),
                "locf" = fill_locf(x),
                "cont" = fill_cont(x, cont),
                "nocb" = fill_nocb(x),
                "mode" = fill_cont(x, mo),
                "mode_k" = if(all(is.na(x))) fill_cont(x, cont) else fill_cont(x, Mode(x)),
                stop(sprintf("wrong fill2 method: %s for %s type", fill2, type))
            )
            
        } else if(type == "bin") {
            cont <- stats[[name]][["cont"]]
            
            # 处理第一个缺失值
            if(is.na(x[1])) {
                x[1] <- switch(fill1,
                    "cont" = cont,
                    "nocb" = if(all(is.na(x))) cont else x[which(!is.na(x))[1]],
                    "zero" = 0,
                    stop(sprintf("wrong fill1 method: %s for bin type", fill1))
                )
            }
            
            # 处理其他缺失值
            x1 <- switch(fill2,
                "zero" = fill_cont(x, 0),
                "cont" = fill_cont(x, cont),
                "locf" = fill_locf(x),
                "nocb" = fill_nocb(x),
                stop(sprintf("wrong fill2 method: %s for bin type", fill2))
            )
        }
        
        return(x1)
    }, col_list, itemid_list, type_list, fill1_list, fill2_list, SIMPLIFY = FALSE)
    
    # 合并所有列
    result_df <- cbind(result_df, do.call(cbind, filled_cols))
    
    # 设置列名
    colnames(result_df)[-1] <- itemid_list
    
    return(result_df)
}


fill_lin <- function(x, time) {
    # 转换为数值型
    x <- as.numeric(x)
    time <- as.numeric(time)
    
    # 获取非NA和NA的索引
    non_na_indices <- which(!is.na(x))
    na_indices <- which(is.na(x))
    
    if (length(na_indices) == 0) return(x)
    if (length(non_na_indices) == 0) return(rep(NA_real_, length(x)))
    
    # 使用 sapply 进行线性插值
    x_filled <- x
    x_filled[na_indices] <- sapply(na_indices, function(i) {
        left_indices <- non_na_indices[non_na_indices < i]
        right_indices <- non_na_indices[non_na_indices > i]
        
        if (length(left_indices) > 0) {
            last_left_index <- max(left_indices)
            if (length(right_indices) > 0) {
                first_right_index <- min(right_indices)
                # 线性插值
                return((x[first_right_index] - x[last_left_index]) / 
                       (time[first_right_index] - time[last_left_index]) * 
                       (time[i] - time[last_left_index]) + x[last_left_index])
            }
            return(x[last_left_index])
        } else if (length(right_indices) > 0) {
            return(x[min(right_indices)])
        }
        return(NA_real_)
    })
    
    return(x_filled)
}

fill_locf <- function(x) {
    non_na_indices <- which(!is.na(x))
    na_indices <- which(is.na(x))
    
    if(length(na_indices) == 0) return(x)
    if(length(non_na_indices) == 0) return(rep(NA_real_, length(x)))
    
    x_filled <- x
    x_filled[na_indices] <- sapply(na_indices, function(i) {
        left_indices <- non_na_indices[non_na_indices < i]
        right_indices <- non_na_indices[non_na_indices > i]
        
        if(length(left_indices) > 0) {
            return(x[max(left_indices)])
        } else if(length(right_indices) > 0) {
            return(x[min(right_indices)])
        }
        return(NA_real_)
    })
    
    return(x_filled)
}

fill_nocb <- function(x) {
    non_na_indices <- which(!is.na(x))
    na_indices <- which(is.na(x))
    
    if(length(na_indices) == 0) return(x)
    if(length(non_na_indices) == 0) return(rep(NA_real_, length(x)))
    
    x_filled <- x
    x_filled[na_indices] <- sapply(na_indices, function(i) {
        left_indices <- non_na_indices[non_na_indices < i]
        right_indices <- non_na_indices[non_na_indices > i]
        
        if(length(right_indices) > 0) {
            return(x[min(right_indices)])
        } else if(length(left_indices) > 0) {
            return(x[max(left_indices)])
        }
        return(NA_real_)
    })
    
    return(x_filled)
}

fill_cont <- function(x, cont) {
    x_filled <- x
    x_filled[is.na(x_filled)] <- cont
    return(x_filled)
}

fill_last_values <- function(df, mask, col_list, time_col, var_dict) {
    # 创建结果数据框
    result_df <- data.frame(time = df[[time_col]])
    
    # 使用 mapply 处理每个变量
    filled_cols <- mapply(function(col, last_value) {
        values <- df[[col]]
        mask_col <- mask[[paste0(col, "_mask")]]
        
        # 找到最后一个观测的位置
        ind <- which(mask_col == 1)
        if (length(ind) < 1) return(values)
        
        first_ind <- ind[length(ind)] + 1
        if (first_ind > length(values)) return(values)
        
        # 如果有last_value，用它填充后续值
        if (!is.na(last_value)) {
            values[first_ind:length(values)] <- last_value
        }
        
        return(values)
    }, col_list, var_dict$last_value, SIMPLIFY = FALSE)
    
    # 合并所有列
    result_df <- cbind(result_df, do.call(cbind, filled_cols))
    
    # 设置列名
    colnames(result_df) <- c(names(df)[time_col], names(df)[col_list])
    
    return(result_df)
}



#######################################
# mask and delta_time
#######################################
get_mask <- function(df, itemid_list, time_col="time") {
    # 创建结果数据框
    result_df <- data.frame(time = df[[time_col]])
    
    # 使用 sapply 计算每个变量的 mask
    masks <- sapply(itemid_list, function(itemid) {
        as.integer(!is.na(df[[itemid]]))
    })
    
    # 如果 df 只有一行,确保 masks 是矩阵格式
    if (!is.matrix(masks)) {
        masks <- matrix(masks, nrow=1)
        colnames(masks) <- itemid_list
    }
    
    # 添加 mask 列到结果数据框
    result_df <- cbind(result_df, masks)
    colnames(result_df)[-1] <- paste0(itemid_list, "_mask")
    
    return(result_df)
}


get_deltamat <- function(mask, itemid_list, time_col="time") {
    # 创建结果数据框
    result_df <- data.frame(time = mask[[time_col]])
    
    # 使用 sapply 计算每个变量的 delta
    deltas <- sapply(itemid_list, function(itemid) {
        # 从 mask 获取非缺失值的位置
        mask_col <- mask[[paste0(itemid, "_mask")]]
        times <- mask[[time_col]]
        
        # 初始化 delta 向量
        delta <- rep(NA_real_, length(times))
        
        # 找到非缺失值的位置（mask 为 1 的位置）
        valid_idx <- which(mask_col == 1)
        if(length(valid_idx) > 0) {
            # 计算第一个非缺失值的 delta
            delta[valid_idx[1]] <- 0
            
            # 计算其余非缺失值的 delta
            if(length(valid_idx) > 1) {
                for(i in 2:length(valid_idx)) {
                    delta[valid_idx[i]] <- times[valid_idx[i]] - times[valid_idx[i-1]]
                }
            }
        }
        delta
    })
    
    # 添加 delta 列到结果数据框
    result_df <- cbind(result_df, deltas)
    colnames(result_df)[-1] <- paste0(itemid_list, "_delta")
    
    return(result_df)
}



shape_as_onehot <- function(df, col_list, time_col, type_list, stats) {
    # 创建新的列名列表
    new_cols <- mapply(function(stat, name) {
        if(stat[["type"]] %in% c("num", "bin")) {
            return(name)
        } else {
            return(paste0(name, "___", seq_along(stat[["unique_values"]])))
        }
    }, stats[names(stats)[col_list]], names(stats)[col_list], SIMPLIFY = FALSE)
    
    # 初始化结果数据框
    result_df <- data.frame(time = df[[time_col]])
    
    # 使用 mapply 处理每个变量
    encoded_cols <- mapply(function(col, type, stat_name) {
        if(type %in% c("num", "bin")) {
            # 数值型、有序型和二值型变量直接返回
            return(df[, col, drop = FALSE])
        } else if(type %in% c("ord", "cat")) {
            # 分类变量展开为多列
            values <- df[[col]]
            n_cats <- length(stats[[stat_name]][["unique_values"]])
            
            # 使用矩阵运算创建 one-hot 编码
            onehot_matrix <- matrix(rep(values, n_cats), ncol = n_cats)
            result <- matrix(as.numeric(onehot_matrix == stats[[stat_name]][["unique_values"]]), 
                           ncol = n_cats)
            
            # 转换为数据框并设置列名
            result_df <- as.data.frame(result)
            colnames(result_df) <- paste0(stat_name, "___", seq_len(n_cats))
            return(result_df)
        }
    }, col_list, type_list, names(stats)[col_list], SIMPLIFY = FALSE)
    
    # 合并所有列
    result_df <- cbind(result_df, do.call(cbind, encoded_cols))
    
    # 设置列名
    colnames(result_df) <- c(colnames(df)[time_col], unlist(new_cols))
    
    return(result_df)
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
        } else if (type %in% c("ord", "cat")) {  # 合并处理 ord 和 cat
            # 从 var_dict 获取 valid_value
            valid_values <- var_dict$valid_value[i]
            if (!is.na(valid_values)) {
                # 使用 sep 分割并去除空白字符
                valid_list <- trimws(unlist(strsplit(valid_values, sep)))
                x <- as.character(df[[itemid]])
                x[!x %in% valid_list] <- NA
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
        } else if (type %in% c("ord", "cat")) {  # 合并处理 ord 和 cat
            # 从 var_dict 获取 valid_value
            valid_values <- var_dict$valid_value[i]
            if (!is.na(valid_values)) {
                # 使用 sep 分割并去除空白字符
                valid_list <- trimws(unlist(strsplit(valid_values, sep)))
                x <- as.character(df_cur[[value_col]])
                x[!x %in% valid_list] <- NA
                df_cur[[value_col]] <- x
            }
            return(df_cur)
        } else if (type == "bin") {
            return(df_cur)
        }
    }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F) %>% do.call(rbind, .)
    return(df_new)
}

#' 初始化 z 转换参数
#' 
#' @param col_list 变量名列表
#' @param var_dict 变量字典数据框，包含 itemid 和 value_type 列
#' @param stats 包含每个变量统计信息的列表
#' @return 包含每个变量(one-hot后)均值和标准差的列表
init_z_param <- function(col_list, var_dict, stats) {
  z_param <- list()
  
  for (col in col_list) {
    # 获取变量类型
    var_type <- var_dict$value_type[var_dict$itemid == col]
    
    if (var_type == "num") {
      # 数值型变量：使用 stats 中的统计值
      z_param[[col]] <- list(
        mean = stats[[col]]$mean,
        sd = stats[[col]]$sd
      )
    } else if (var_type %in% c("cat", "ord")) {
      # 分类型和有序型变量：为每个 one-hot 列设置 mean=0, sd=1
      n_categories <- length(stats[[col]]$unique_values)
      for (i in 1:n_categories) {
        onehot_col <- paste(col, i, sep = "___")
        z_param[[onehot_col]] <- list(
          mean = 0,
          sd = 1
        )
      }
    } else {  # bin 类型
      # 二值型变量：设置 mean=0, sd=1
      z_param[[col]] <- list(
        mean = 0,
        sd = 1
      )
    }
  }
  
  return(z_param)
}

#' Z 标准化转换
#' 
#' @param df 输入数据框
#' @param z_param z转换参数列表
#' @return z标准化后的数据框
z_trans <- function(df, z_param) {
  # 创建结果数据框
  result <- df
  
  # 对每一列进行 z 转换
  for (col in names(df)) {
    if (col %in% names(z_param)) {
      mean <- z_param[[col]]$mean
      sd <- z_param[[col]]$sd
      # 避免除以0
      if (sd == 0) {
        result[[col]] <- 0
      } else {
        result[[col]] <- (df[[col]] - mean) / sd
      }
    }
    # 不在 z_param 中的列保持原样
  }
  
  return(result)
}

#' 逆向 Z 标准化转换
#' 
#' @param df 输入数据框
#' @param z_param z转换参数列表
#' @return 逆向z标准化后的数据框
rev_z_trans <- function(df, z_param) {
  # 创建结果数据框
  result <- df
  
  # 对每一列进行逆向 z 转换
  for (col in names(df)) {
    if (col %in% names(z_param)) {
      mean <- z_param[[col]]$mean
      sd <- z_param[[col]]$sd
      # 对于标准差为0的列，直接赋值为均值
      if (sd == 0) {
        result[[col]] <- mean
      } else {
        result[[col]] <- df[[col]] * sd + mean
      }
    }
    # 不在 z_param 中的列保持原样
  }
  
  return(result)
}


#' 将 z_param 保存到 JSON 文件
#' 
#' @param z_param z转换参数列表
#' @param file_path JSON文件的保存路径
#' @return 无返回值，保存文件到指定路径
#' @import jsonlite
save_z_param_to_json <- function(z_param, file_path) {
  # 检查 z_param 是否存在
  if (is.null(z_param)) {
    stop("z_param 未初始化")
  }
  
  # 尝试保存文件
  tryCatch({
    jsonlite::write_json(
      z_param,
      path = file_path,
      pretty = TRUE,    # 等同于 Python 中的 indent=4
      auto_unbox = TRUE # 避免单个值被转换为数组
    )
  }, error = function(e) {
    stop(sprintf("保存 z_param 到 %s 失败: %s", file_path, e$message))
  })
}

#' 从 JSON 文件加载 z_param
#' 
#' @param file_path JSON文件的路径
#' @return z转换参数列表
#' @import jsonlite
load_z_param_from_json <- function(file_path) {
  # 检查文件是否存在
  if (!file.exists(file_path)) {
    stop(sprintf("文件不存在: %s", file_path))
  }
  
  # 尝试读取文件
  tryCatch({
    z_param <- jsonlite::read_json(
      file_path,
      simplifyVector = FALSE  # 保持列表结构
    )
    return(z_param)
  }, error = function(e) {
    if (grepl("parse error", e$message, ignore.case = TRUE)) {
      stop(sprintf("JSON 格式不正确: %s", e$message))
    } else {
      stop(sprintf("读取文件 %s 失败: %s", file_path, e$message))
    }
  })
}
