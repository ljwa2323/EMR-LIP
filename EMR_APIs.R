

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
    if(verbo) print(paste0(root_path," 已经删除"))
  }
  if (!dir.exists(root_path)) {
    dir.create(root_path)
    if(verbo) print(paste0(root_path," 已经创建"))
  }
}

#######################################
# suppplement statistic function
#######################################
Mode <- function(x){
    # Mode函数用于计算众数
    # 输入参数:
    #   - x: 一个向量或数据框的列，用于计算众数
    # 输出:
    #   - 众数的值，类型为字符
    x <- na.omit(x)
    x <- as.character(x)
    tab <- table(x, useNA = "no")
    r <- names(tab)[which.max(tab)]
    if (length(r) == 0) return(NA) else return(r)
}

#######################################
# get variable statistic
#######################################

get_stat_wide <- function(df, itemid_list, type_list) {

    stats <- mapply(function(i, itemid, type){
        if (i %% 100 == 0) print(i)
        if (type == "num") {
            x <- df[[itemid]]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)

            # 如果 mean_obj 为 NA，就赋值为0
            if (is.na(mean_obj)) {
                mean_obj <- 0
            }
            
            # 计算分位数
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)
            # 如果 quantile_obj 全为 NA，则赋值为 全0
            if (all(is.na(quantile_obj))) {
                quantile_obj <- rep(0, length(quantile_obj))
            }
            
            # 计算标准差
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # 如果 sd_obj ==0 或 为 NA，就赋值为0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type, mean_obj, sd_obj, quantile_obj))

        } else if (type == "cat") {
            # 计算众数
            x <- df[[itemid]]
            x <- as.character(x)
            mode_obj <- names(which.max(table(x, useNA = "no")))

            uniq_value <- sort(unique(na.omit(x)), decreasing = F)

            # 计算所有可能取值的个数
            unique_count <- length(uniq_value)

            # 返回一个列表，包含2个对象
            return(list(type, mode_obj, unique_count, uniq_value)) 
        } else if (type == "ord") {

            x <- df[[itemid]]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)
            
            # 计算分位数
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # 计算标准差
            if (abs(mean_obj - quantile_obj[6]) > 0.05 * min(mean_obj, quantile_obj[6])) {
                x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
                sd_obj <- sd(x_in_range, na.rm = TRUE)
            } else {
                sd_obj <- sd(x, na.rm = TRUE)
            }

            # 如果 sd_obj ==0 或 为 NA，就赋值为0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type, mean_obj, sd_obj, quantile_obj))
        } else if (type == "bin") {
            return(list(type))
        }
        }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F)

  names(stats)<-as.character(itemid_list)

  return(stats)
}

get_stat_long <- function(df, itemid_list, type_list, itemid_col, value_col) {

    stats<-mapply(function(i, itemid, type){
        
        if (i %% 100 == 0) print(i)

        ind <- which(df[[itemid_col]] == itemid)

        if(type == "num"){
            x <- df[[value_col]][ind]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)
            
            # 计算分位数
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # 计算标准差
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # 如果 sd_obj ==0 或 为 NA，就赋值为0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type, mean_obj, sd_obj, quantile_obj))

        } else if(type == "cat"){
            # 计算众数
            x <- df[[value_col]][ind]
            x <- as.character(x)
            mode_obj <- names(which.max(table(x, useNA = "no")))

            uniq_value <- sort(unique(na.omit(x)), decreasing = F)

            # 计算所有可能取值的个数
            unique_count <- length(uniq_value)

            # 返回一个列表，包含2个对象
            return(list(type, mode_obj, unique_count, uniq_value)) 
        } else if(type == "ord"){

            x <- df[[value_col]][ind]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)
            
            # 计算分位数
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # 计算标准差
            if (abs(mean_obj - quantile_obj[6]) > 0.05 * min(mean_obj, quantile_obj[6])) {
                x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
                sd_obj <- sd(x_in_range, na.rm = TRUE)
            } else {
                sd_obj <- sd(x, na.rm = TRUE)
            }

            # 如果 sd_obj ==0 或 为 NA，就赋值为0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type, mean_obj, sd_obj, quantile_obj))
        } else if (type == "bin") {
            return(list(type))
        }
        }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F)

  names(stats)<-as.character(itemid_list)

  return(stats)
}

get_type <- function(stat) {
    return(lapply(stat, function(x) x[[1]]) %>% unlist)
}

#######################################
# resampling
#######################################

resample_data_wide <- function(df, itemid_list, type_list, time_list, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        
        # 筛选数据
        ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                    (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))

        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid, type) {
                            x<-ds_cur[[itemid]]
                            if (type == "num") {
                                x<-as.numeric(x)
                                return(median(x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                return(Mode(na.omit(x)))
                            } else if (type == "bin"){
                                x<-as.numeric(x)
                                return(ifelse(sum(x,na.rm = T)>=1,1,0))
                            }
                            }, itemid_list, type_list ,SIMPLIFY = T) %>% unlist

        return(c("1", cur_x))

        }) %>% do.call(rbind, .)

    # 检查 mat 是否是一个矩阵，如果不是，就将其转换为一个矩阵
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat<-which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if(!keep_first){
        if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
            mat<-mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames

    return(mat)
}

resample_data_long <- function(df,itemid_list, type_list, time_list, itemid_col, value_col, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        
        # 筛选数据
        ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                    (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))

        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid, type) {
                            ind <- which(ds_cur[[itemid_col]] == itemid)
                            x<-ds_cur[[value_col]][ind]
                            if (type == "num") {
                                x<-as.numeric(x)
                                return(median(x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                return(Mode(na.omit(x)))
                            } else if (type == "bin"){
                                x<-as.numeric(x)
                                return(ifelse(sum(x,na.rm = T)>=1,1,0))
                            }
                            }, itemid_list, type_list ,SIMPLIFY = T) %>% unlist

        return(c("1", cur_x))

        }) %>% do.call(rbind, .)

    # 检查 mat 是否是一个矩阵，如果不是，就将其转换为一个矩阵
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat<-which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if(!keep_first){
        if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
            mat<-mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames

    return(mat)
}

resample_process_wide <- function(df,itemid_list, type_list, time_list, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
    
    # 筛选数据
    # cur_t<-14
    ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
    if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
    ds_cur <- df[ind_time, ]

    # 计算交集的长度
    overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window/2) - pmax(ds_cur[[time_col1]], cur_t - time_window/2)
    overlap <- pmax(overlap, 0)

    # 计算[start_time, end_time]的长度
    total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]

    # 计算比例
    ds_cur$proportion <- overlap / total

    cur_x <- mapply(function(itemid, type) {
        x<-ds_cur[[itemid]]
            if (type == "num") {
                x<-as.numeric(x)
                x<-x*ds_cur$proportion
                return(median(x, na.rm=T))
            } else if (type %in% c("cat","ord")){
                x<-as.character(x)
                return(Mode(x))
            } else if (type == "bin") {
                x<-as.numeric(x)
                return(ifelse(sum(x,na.rm = T)>=1,1,0))
            }
        }, itemid_list, type_list ,SIMPLIFY = T) %>% unlist

        return(c("1", cur_x))
    }) %>% do.call(rbind, .)
    
    # 检查 mat 是否是一个矩阵，如果不是，就将其转换为一个矩阵
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat<-which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if(!keep_first){
        if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
            mat<-mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames
    return(mat)

}

resample_process_long <- function(df,itemid_list, type_list, time_list, itemid_col, value_col, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
    
    # 筛选数据
    # cur_t<-14
    ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
    if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
    ds_cur <- df[ind_time, ]

    # 计算交集的长度
    overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window/2) - pmax(ds_cur[[time_col1]], cur_t - time_window/2)
    overlap <- pmax(overlap, 0)

    # 计算[start_time, end_time]的长度
    total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]

    # 计算比例
    ds_cur$proportion <- overlap / total

    cur_x <- mapply(function(itemid, type) {
                        ind <- which(ds_cur[[itemid_col]] == itemid)
                        x<-ds_cur[[value_col]][ind]
                            if (type == "num") {
                                x<-as.numeric(x)
                                x<-x*ds_cur$proportion
                                return(median(x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                x<-as.character(x)
                                return(Mode(x))
                            } else if (type == "bin"){
                                x<-as.numeric(x)
                                return(ifelse(sum(x,na.rm = T)>=1,1,0))
                            }
                        }, itemid_list, type_list ,SIMPLIFY = T) %>% unlist

        return(c("1", cur_x))
    }) %>% do.call(rbind, .)
    
    # 检查 mat 是否是一个矩阵，如果不是，就将其转换为一个矩阵
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat<-which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if(!keep_first){
        if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
            mat<-mat[-1,,drop=F]
        }
    }

    colnames(mat) <- Colnames

    return(mat)

}

resample_binary_long <- function(df, itemid_list, time_list, itemid_col, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {
    # resample_binary函数用于对二进制变量进行重采样
    # 输入参数:
    #   - df: 数据框，包含需要重采样的数据
    #   - itemid_list: 字符串向量，表示需要重采样的变量的ID列表
    #   - time_col1: 字符串，表示数据框中的列名，用于指定起始时间
    #   - time_col2: 字符串，表示数据框中的列名，用于指定结束时间
    #   - time_list: 数值向量，表示需要重采样的时间点列表
    #   - time_window: 数值，表示重采样的时间窗口大小
    # 输出:
    #   - 重采样后的矩阵，包含时间和各个变量的值，类型为矩阵
    Colnames <- c("time", as.character(itemid_list))
    mat <- lapply(time_list, function(cur_t) {
        ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid){
            ind<-which(ds_cur[[itemid_col]]==itemid)
            if(length(ind) > 0) return(1) else {return(0)}
        }, itemid_list, SIMPLIFY = T) %>% unlist

        return(c("1", cur_x))

    }) %>% do.call(rbind, .)

    # 检查 mat 是否是一个矩阵，如果不是，就将其转换为一个矩阵
    if (!is.matrix(mat)) {
        mat <- matrix(mat, ncol = length(itemid_list), byrow=T)
    }

    mat[1,1] <- "1"

    if(!keepNArow) {
        ind_mat<-which(mat[,1]=="1")
        mat <- rbind(cbind(time_list, mat)[ind_mat,])
    } else{
        mat <- rbind(cbind(time_list, mat))
    }

    if(!keep_first){
        if (nrow(mat) > 1 && all(is.na(mat[1,3:ncol(mat),drop=T]))) {
            mat<-mat[-1,,drop=F]
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
            N <- as.integer(stats[[name]][[3]])
            vec <- stats[[name]][[4]]
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
    # rev_onehot函数用于将独热编码还原为原始数据
    # 输入参数:
    #   - mat1: 数据框，包含进行独热编码后的数据
    #   - col_list: 整数向量，表示进行独热编码的变量列的索引
    #   - time_col: 整数，表示时间列的索引
    #   - type_list: 字符串向量，表示变量的类型列表
    #   - stats: 列表，包含各个变量的统计信息
    # 输出:
    #   - 还原后的原始数据框，类型为数据框
    itemid_list <- names(stats)

    num_list <-mapply(function(type, stat){
        if (type %in% c("num","ord","bin")) return(1) else return(stat[[3]])
    }, type_list, stats, SIMPLIFY = T) %>% unlist

    mat <- lapply(1:nrow(mat1), function(r){
        x <- mat1[r, col_list]
        cur <- 1
        x1 <- list()
        for(i in 1:length(num_list)){
            if(type_list[i] %in% c("num","ord")){
                y <- x[cur]
                cur <- cur+1
                x1[[i]] <- y
            } else {
                vec <- stats[[i]][[4]]
                enc <- as.integer(x[cur:(cur+num_list[i]-1)])
                y <- vec[which(enc==1)]
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
            m <- stats[[name]][[2]]
            s <- stats[[name]][[3]]
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
    # rev_normnum函数用于对归一化后的数值变量进行逆归一化处理
    # 输入参数:
    #   - mat: 矩阵，包含需要进行逆归一化处理的数据
    #   - col_list: 整数向量，表示需要进行逆归一化处理的变量列的索引
    #   - time_col: 整数，表示时间列的索引
    #   - type_list: 字符向量，表示每个变量的类型（"num"表示数值变量，"cat"表示分类变量）
    #   - stats: 列表，包含每个数值变量的统计信息（均值和标准差）
    # 输出:
    #   - 逆归一化处理后的矩阵
    itemid_list <- as.character(colnames(mat)[col_list])
    mat1 <- mapply(function(col, itemid, type){
        if(type %in% c("num","ord")) {
            m <- stats[[itemid]][[2]]
            s <- stats[[itemid]][[3]]
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
fill <- function(mat, col_list, time_col, type_list, fillfun_list, allNAfillfun_list, stats){
    # fill函数用于填充缺失值
    # 输入参数:
    #   - mat: 矩阵，包含需要填充缺失值的数据
    #   - col_list: 整数向量，表示需要填充缺失值的变量列的索引
    #   - time_col: 整数，表示时间列的索引
    #   - type_list: 字符串向量，表示变量的类型列表
    #   - stats: 列表，包含各个变量的统计信息

    # 输出:
    #   - 填充缺失值后的矩阵
    itemid_list <- as.character(colnames(mat)[col_list])
    
    mat1 <- mapply(function(col, name, type, fillfun, allNAfillfun){
        if(type == "num") {
            # col <- 3;name<-itemid_list[2];type<-type_list[2]
            m <- stats[[name]][[2]]
            x <- mat[,col,drop=T]
            if (all(is.na(x))) {
                if (allNAfillfun == "mean") {
                    return(cbind(rep(m, length(x))))
                } else if (allNAfillfun == "zero") {
                    return(cbind(rep(0, length(x))))
                } else {
                    stop("wrong fill value")
                }
            }
            
            ti <- mat[, time_col,drop=T]
            ti <- as.numeric(ti)
            x <- as.numeric(x)
            if (fillfun == "lin") {
                x1 <- fill_lin(x, ti)
            } else if (fillfun == "zero"){
                x1 <- fill_zero(x)
            } else if (fillfun == "locf"){
                x1 <- fill_locf(x)
            } else{
                stop("wrong fill value")
            }
            return(cbind(x1))
        } else if(type=="cat") {
            mo <- stats[[name]][[2]]
            x <- mat[,col,drop=T]
            x <- as.character(x)
            if (all(is.na(x))) {
                if (allNAfillfun == "mode"){
                    return(cbind(rep(mo, length(x))))
                } else if (allNAfillfun == "zero") {
                    return(cbind(rep(0, length(x))))
                } else {
                    stop("wrong fill value")
                }
            }

            if (fillfun == "zero"){
                x1 <- fill_zero(x)
            } else if (fillfun == "locf"){
                x1 <- fill_locf(x)
            } else{
                stop("wrong fill value")
            }
            return(cbind(x1))
        } else if(type == "ord"){
            m <- stats[[name]][[2]]
            x <- mat[,col,drop=T]
            x <- as.numeric(x)
            if (all(is.na(x))) {
                if (allNAfillfun == "mean") {
                    return(cbind(rep(m, length(x))))
                } else if (allNAfillfun == "zero") {
                    return(cbind(rep(0, length(x))))
                } else {
                    stop("wrong fill value")
                }
            }
            
            if (fillfun == "zero"){
                x1 <- fill_zero(x)
            } else if (fillfun == "locf"){
                x1 <- fill_locf(x)
            } else{
                stop("wrong fill value")
            }
            return(cbind(x1))
        } else if(type == "bin"){
            x <- mat[,col,drop=T]
            x <- as.numeric(x)
            if (all(is.na(x))) {
                if (allNAfillfun == "zero") {
                    return(cbind(rep(0, length(x))))
                } else {
                    stop("wrong fill value")
                }
            }
            
            if (fillfun == "zero"){
                x1 <- fill_zero(x)
            } else if (fillfun == "locf"){
                x1 <- fill_locf(x)
            } else{
                stop("wrong fill value")
            }
            return(cbind(x1))
        }
    }, col_list, itemid_list, type_list, fillfun_list, allNAfillfun_list, SIMPLIFY = F) %>% do.call(cbind, .)
    mat1 <- cbind(mat[,time_col,drop=F], mat1)
    row.names(mat1) <- NULL
    colnames(mat1)[2:ncol(mat1)] <- itemid_list
    mat1
}

fill_lin <- function(x, time) {
    # fill_lin函数用于填充线性插值
    # 输入参数:
    #   - x: 数值向量，表示需要进行线性插值的数据
    #   - time: 数值向量，表示时间
    # 输出:
    #   - 填充线性插值后的数据，类型为数值向量
    x <- as.numeric(x)
    time <- as.numeric(time)
    
    ind1.1 <- which(!is.na(x))
    ind1.2 <- which(is.na(x))
    
    if (length(ind1.2) == 0) {
        return(x)
    }
    
    if (length(ind1.1) == 0) {
        return(rep(NA, length(x)))
    }
    
    # 预分配内存
    x_filled <- x
    
    # 使用 sapply 替代 for 循环
    x_filled[ind1.2] <- sapply(ind1.2, function(i) {
        ind2.1 <- ind1.1[ind1.1 < i] # 左侧
        ind2.2 <- ind1.1[ind1.1 > i] # 右侧
        
        if (length(ind2.1) > 0) {
        ind3.1 <- max(ind2.1) # 左侧最近观测值位置
        if (length(ind2.2) > 0) {
            ind3.2 <- min(ind2.2) # 右侧最近观测值位置
            return((x[ind3.2] - x[ind3.1]) / (time[ind3.2] - time[ind3.1]) * (time[i] - time[ind3.1]) + x[ind3.1])
        } else {
            return(x[ind3.1])
        }
        } else {
        if (length(ind2.2) > 0) {
            ind3.2 <- min(ind2.2) # 右侧最近观测值位置
            return(x[ind3.2])
        } else {
            return(NA)
        }
        }
    })
    
    return(x_filled)
}

fill_locf <- function(x) {
    # fill_locf函数用于使用最近观测值进行缺失值填充
    # 输入参数:
    #   - x: 向量，表示需要填充缺失值的数据
    # 输出:
    #   - 填充缺失值后的数据，类型为向量
    # x <- as.character(x)
    
    ind1.1 <- which(!is.na(x))
    ind1.2 <- which(is.na(x))
    
    if(length(ind1.2) == 0) {
        return(x)
    }
    
    if(length(ind1.1) == 0) {
        return(rep(NA, length(x)))
    }
    
    x_filled <- x
    
    x_filled[ind1.2] <- sapply(ind1.2, function(i) {
        ind2.1 <- ind1.1[ind1.1 < i] # 左侧
        ind2.2 <- ind1.1[ind1.1 > i] # 右侧
        
        if(length(ind2.1) > 0) {
            ind3.1 <- max(ind2.1) # 左侧最近观测值位置
            return(x[ind3.1])
        } else if(length(ind2.2) > 0) {
            ind3.2 <- min(ind2.2) # 右侧最近观测值位置
            return(x[ind3.2])
        } else {
            return(NA)
        }
    })
    
    return(x_filled)
}

fill_zero <- function(x) {
    # fill_zero函数用于将缺失值填充为0
    # 输入参数:
    #   - x: 向量，表示需要填充缺失值的数据
    # 输出:
    #   - 填充缺失值后的数据，类型为向量
    # x<-as.character(x)
    x_filled <- x
    x_filled[is.na(x_filled)]<-"0"
    return(x_filled)
}


#######################################
# mask and delta_time
#######################################
get_mask <- function(mat, col_list, time_col){
    # get_mask函数用于生成缺失值掩码
    # 输入参数:
    #   - mat: 矩阵，包含需要生成缺失值掩码的数据
    #   - col_list: 整数向量，表示需要生成缺失值掩码的变量列的索引
    #   - time_col: 整数，表示时间列的索引
    # 输出:
    #   - 生成的缺失值掩码矩阵
    mat1 <- rbind(apply(mat[,col_list,drop=F], 2, function(x) ifelse(is.na(x), 0, 1)))
    mat1 <- cbind(mat[,time_col,drop=F], mat1)
    colnames(mat1) <- colnames(mat)
    mat1
}

get_deltamat <- function(mat, col_list, time_col) {
  # 假设 s 是一个矩阵，m 是 mat 的第二列到最后一列
  s <- as.numeric(mat[,time_col])
  m <- mat[,col_list,drop=F]

  # 初始化 delta
  delta <- matrix(0, nrow = nrow(m), ncol = ncol(m))

  if (nrow(m) > 1) {
    # 计算每个时间点的 delta
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

  # 输出 delta
  delta<- cbind(mat[, time_col, drop = FALSE], delta)
  colnames(delta) <- colnames(mat)
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

    mat1 <- lapply(1:nrow(mat), function(r){
        x <- mat[r, col_list]
        cur <- 1
        x1 <- list()
        for(i in 1:length(num_list)){
            if(type_list[i] %in% c("num", "ord","bin")){
                y <- x[cur]
                cur <- cur+1
                x1[[i]] <- y
            } else if (type_list[i] == "cat") {
                x1[[i]] <- rep(x[cur], as.integer(num_list[i]))
                cur <- cur + 1
            }
        }
        x1 <- unlist(x1)
        return(x1)
    }) %>% do.call(rbind, .)

    mat1 <- cbind(mat[,time_col,drop=F], mat1)
    colnames(mat1) <- c(colnames(mat)[time_col], itemid_list)
    mat1
}

#######################################
# remove extreme value
#######################################
remove_extreme_value_long <- function(df, itemid_list, type_list, itemid_col, value_col, neg_valid=F){

    df_new <- mapply(function(i, itemid, type){
                        ind <- which(df[[itemid_col]] == itemid)
                        df_cur <- df[ind, ]
                        if(type %in% c("num","ord")){
                            x <- df_cur$value
                            # 将 x 中的值再 99% 以上的值设为NA
                            x[x > quantile(x, 0.99, na.rm = TRUE)] <- NA
                            if(!neg_valid) {
                                x[x<0] <- NA
                            }
                            df_cur$value <- x
                            return(df_cur)
                        } else if(type %in% c("cat", "bin")){
                            return(df_cur)
                        }
                    }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F) %>% do.call(rbind, .)
    return(df_new)
}

remove_extreme_value_wide <- function(df, itemid_list, type_list, cols_keep, neg_valid=F){

    mat <- mapply(function(i, itemid, type){
                        if(type %in% c("num","ord")){
                            x <- df[[itemid]]
                            x <- as.numeric(x)
                            # 将 x 中的值再 99% 以上的值设为NA
                            x[x > quantile(x, 0.99, na.rm = TRUE)] <- NA
                            if(!neg_valid) {
                                x[x<0] <- NA
                            }
                            return(x)
                        } else if(type %in% c("cat", "bin")){
                            return(df[[itemid]])
                        }
                    }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F) %>% do.call(cbind, .)
    mat1 <- cbind(df[,cols_keep, drop=F], mat)
    row.names(mat1) <- NULL
    colnames(mat1)[(length(cols_keep)+1):ncol(mat1)] <- itemid_list
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
        }
    }
    
    mat1 <- rbind(stats)
    row.names(mat1) <- NULL
    return(mat1)
}

#######################################
# select fill method
#######################################
get_fill_method <- function(ds_var_dict) {
  ds_var_dict$fill <- ifelse(ds_var_dict$value_type %in% c("num", "ord", "bin"), "locf", "zero")
  ds_var_dict$fillall <- ifelse(ds_var_dict$value_type %in% c("num", "ord"), "mean", "zero")
  ds_var_dict$fill[ds_var_dict$acqu_type == "oper"] <- "zero"
  ds_var_dict$fillall[ds_var_dict$acqu_type == "oper"] <- "zero"
  return(ds_var_dict)
}

# var_dict <- read.csv("/home/luojiawei/Benchmark_project_data/mimiciv_data/var_dict.csv",header=T)
# var_dict <- get_fill_method(var_dict)
# Operational Variable