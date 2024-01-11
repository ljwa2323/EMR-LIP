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
# suppplement statistic function
#######################################
Mode <- function(x, na.rm=T){
    # this function is used to calculate mode
    # Input:
    #   - x: A column of vectors or data frames used to calculate the mode
    # Output:
    #   - The value of the mode, which is of type character
    if(na.rm) x <- na.omit(x)
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
    if(na.rm) {
        na_index <- is.na(x) | is.na(w)
        x <- x[!na_index]
        w <- w[!na_index]
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
    # Calculate the weighted median
    ord <- order(x)
    cw <- cumsum(w[ord])
    p <- cw / sum(w)
    weighted_median <- x[ord][max(which(p <= 0.5))]
    return(weighted_median)
}


#######################################
# get variable statistic
#######################################

get_stat_wide <- function(df, itemid_list, type_list) {

    stats <- mapply(function(i, itemid, type){
        if (type == "num") {
            x <- df[[itemid]]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)

            # If mean_obj is NA, the value is assigned to 0
            if (is.na(mean_obj)) {
                mean_obj <- 0
            }
            
            # Calculated quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)
            #If quantile_obj is all NA, the value is assigned to all zeros
            if (all(is.na(quantile_obj))) {
                quantile_obj <- rep(0, length(quantile_obj))
            }
            
            # Calculated standard deviation
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # If sd_obj ==0 or NA, the value is 0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type, mean_obj, sd_obj, quantile_obj))

        } else if (type == "cat") {
            # Computational mode
            x <- df[[itemid]]
            x <- as.character(x)
            mode_obj <- names(which.max(table(x, useNA = "no")))

            uniq_value <- sort(unique(na.omit(x)), decreasing = F)

            # Count the number of all possible values
            unique_count <- length(uniq_value)

            # Returns a list of 2 objects
            return(list(type, mode_obj, unique_count, uniq_value))
        } else if (type == "ord") {

            x <- df[[itemid]]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)
            
            # Calculate quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # Calculate standard deviation
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # If sd_obj ==0 or NA, the value is 0.001
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

        ind <- which(df[[itemid_col]] == itemid)

        if(type == "num"){
            x <- df[[value_col]][ind]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)
            
            # Calculate quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # Calculate standard deviation
            x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
            sd_obj <- sd(x_in_range, na.rm = TRUE)

            # If sd_obj ==0 or NA, the value is 0.001
            if (is.na(sd_obj) || sd_obj == 0) {
                sd_obj <- 1
            }

            return(list(type, mean_obj, sd_obj, quantile_obj))

        } else if(type == "cat"){
            # Calculate mode
            x <- df[[value_col]][ind]
            x <- as.character(x)
            mode_obj <- names(which.max(table(x, useNA = "no")))

            uniq_value <- sort(unique(na.omit(x)), decreasing = F)

            # Count the number of all possible values
            unique_count <- length(uniq_value)

            # Returns a list of 2 objects
            return(list(type, mode_obj, unique_count, uniq_value)) 
        } else if(type == "ord"){

            x <- df[[value_col]][ind]
            x <- as.numeric(x)

            mean_obj <- mean(x, na.rm = TRUE)
            
            # Calculate quantile
            quantile_obj <- quantile(x, probs = seq(0, 1, 0.1), na.rm = TRUE)

            # Calculate standard deviation
            if (abs(mean_obj - quantile_obj[6]) > 0.05 * min(mean_obj, quantile_obj[6])) {
                x_in_range <- x[x >= quantile_obj[2] & x <= quantile_obj[10]]
                sd_obj <- sd(x_in_range, na.rm = TRUE)
            } else {
                sd_obj <- sd(x, na.rm = TRUE)
            }

            # If sd_obj ==0 or NA, the value is 0.001
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


agg_f_dict <- list("mean" = mean, "sum" = sum, "sum_w" = sum, "mode" = Mode, "mode_w" = Mode_w, 
                   "mean_w" = mean_w, "median_w" = median_w, 
                   "min" = min, "max" = max, "median" = median, 
                   "first" = get_first, "last" = get_last)
get_agg_f <- function(fn){
    return(agg_f_dict[[fn]])
}

resample_data_wide <- function(df, itemid_list, type_list, agg_f_list, time_list, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        
        # filtering time
        ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                    (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))

        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid, type, agg_f) {
                            x<-ds_cur[[itemid]]
                            if (type == "num") {
                                x<-as.numeric(x)
                                return(get_agg_f(agg_f)(x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                return(get_agg_f(agg_f)(na.omit(x)))
                            } else if (type == "bin"){
                                x<-as.numeric(x)
                                return(ifelse(sum(x,na.rm = T)>=1,1,0))
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

resample_data_long <- function(df,itemid_list, type_list, agg_f_list, time_list, itemid_col, value_col, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
        
        # filtering time
        ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                    (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
        if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))

        ds_cur <- df[ind_time, ]

        cur_x <- mapply(function(itemid, type, agg_f) {
                            ind <- which(ds_cur[[itemid_col]] == itemid)
                            x<-ds_cur[[value_col]][ind]
                            if (type == "num") {
                                x<-as.numeric(x)
                                return(get_agg_f(agg_f)(x, na.rm=T))
                            } else if (type %in% c("cat","ord")){
                                return(get_agg_f(agg_f)(na.omit(x)))
                            } else if (type == "bin"){
                                x<-as.numeric(x)
                                return(ifelse(sum(x,na.rm = T)>=1,1,0))
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

resample_process_wide <- function(df,itemid_list, type_list, agg_f_list, time_list, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
    
    # filtering time
    # cur_t<-13
    ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
    if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
    ds_cur <- df[ind_time, ]

    # Calculate the length of the intersection
    overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window/2) - pmax(ds_cur[[time_col1]], cur_t - time_window/2)
    overlap <- pmax(overlap, 0)

    # Calculates the length of [start_time, end_time]
    total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]

    # Calculate the ratios
    ds_cur$proportion <- overlap / total

    cur_x <- mapply(function(itemid, type, agg_f) {
        # itemid <- itemid_list[1]
        # type <- type_list[1]
        # agg_f <- agg_f_list[1]
        x<-ds_cur[[itemid]]
        if (type == "num") {
            x<-as.numeric(x)
            if(agg_f %in% c("mean_w", "median_w")) {
                return(get_agg_f(agg_f)(x, overlap, na.rm=T))
            } else if(agg_f == "sum_w"){
                x <- x * ds_cur$proportion
                return(get_agg_f(agg_f)(x, na.rm=T))
            } else{
                return(get_agg_f(agg_f)(x, na.rm=T))
            }
        } else if (type %in% c("cat","ord")){
            x <- as.character(x)
            if(agg_f == "mode_w") {
                return(get_agg_f(agg_f)(x, overlap, na.rm=T))
            } else {
                return(get_agg_f(agg_f)(x, na.rm=T))
            }
        } else if (type == "bin") {
            x<-as.numeric(x)
            return(ifelse(sum(x, na.rm = T)>=1,1,0))
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

resample_process_long <- function(df,itemid_list, type_list, agg_f_list, time_list, itemid_col, value_col, time_col1, time_col2, time_window, keepNArow=F, keep_first=T) {

    Colnames <- c("time", "keep", itemid_list)
    
    mat <- lapply(time_list, function(cur_t){
    
    # Filter time
    # cur_t<-14
    ind_time<-which(((is.na(df[[time_col2]]) & df[[time_col1]] >= (cur_t - time_window/2) & df[[time_col1]] <= (cur_t + time_window/2)) |
                (!is.na(df[[time_col2]]) & (df[[time_col1]] <= (cur_t + time_window/2) & df[[time_col2]] >= (cur_t - time_window/2)))))
    if(length(ind_time) == 0) return(c("0", rep(NA, length(itemid_list))))
    ds_cur <- df[ind_time, ]

    # Calculate the length of the intersection
    overlap <- pmin(ds_cur[[time_col2]], cur_t + time_window/2) - pmax(ds_cur[[time_col1]], cur_t - time_window/2)
    overlap <- pmax(overlap, 0)

    # Calculates the length of [start_time, end_time]
    total <- ds_cur[[time_col2]] - ds_cur[[time_col1]]

    # Calculate the ratios
    ds_cur$proportion <- overlap / total

    cur_x <- mapply(function(itemid, type, agg_f) {
                        ind <- which(ds_cur[[itemid_col]] == itemid)
                        x <- ds_cur[[value_col]][ind]
                            if (type == "num") {
                                x<-as.numeric(x)
                                if(agg_f %in% c("mean_w", "median_w")) {
                                    return(get_agg_f(agg_f)(x, overlap[ind], na.rm=T))
                                } else if(agg_f == "sum_w") {
                                    x <- x * ds_cur$proportion[ind]
                                    return(get_agg_f(agg_f)(x, na.rm=T))
                                } else{
                                    return(get_agg_f(agg_f)(x, na.rm=T))
                                }
                            } else if (type %in% c("cat", "ord")){
                                x <- as.character(x)
                                if(agg_f == "mode_w") {
                                    return(get_agg_f(agg_f)(x, overlap[ind]))
                                } else{
                                    return(get_agg_f(agg_f)(x, na.rm=T))
                                }
                            } else if (type == "bin"){
                                x<-as.numeric(x)
                                return(ifelse(sum(x,na.rm = T)>=1,1,0))
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
    # The resample_binary function is used for resampling binary variables.
    # Input parameters:
    #   - df: A dataframe that contains the data to be resampled.
    #   - itemid_list: A vector of strings representing the IDs of the variables that need to be resampled.
    #   - time_col1: A string specifying the column name in the dataframe for the start time.
    #   - time_col2: A string specifying the column name in the dataframe for the end time.
    #   - time_list: A numeric vector representing the list of time points for resampling.
    #   - time_window: A numeric value representing the size of the resampling time window.
    # Output:
    #   - A resampled matrix, including time and values for each variable, as a matrix type.
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
                vec <- stats[[i]][[4]]
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
    
    itemid_list <- as.character(colnames(mat)[col_list])
    
    mat1 <- mapply(function(col, name, type, fillfun, allNAfillfun){
        if(type == "num") {
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
    # The fill_lin function is used to fill the linear interpolation
    # inputs:
    #   - x: Numerical vector representing the data for which linear interpolation is required
    #   - time: Numerical vector, representing time
    # output:
    #   - a value vector after linear interpolation.
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
    
    # Pre-allocated memory
    x_filled <- x
    
    x_filled[ind1.2] <- sapply(ind1.2, function(i) {
        ind2.1 <- ind1.1[ind1.1 < i] # left
        ind2.2 <- ind1.1[ind1.1 > i] # right
        
        if (length(ind2.1) > 0) {
        ind3.1 <- max(ind2.1) # Location of the most recent observations on the left
        if (length(ind2.2) > 0) {
            ind3.2 <- min(ind2.2) # Location of the most recent observations on the right
            return((x[ind3.2] - x[ind3.1]) / (time[ind3.2] - time[ind3.1]) * (time[i] - time[ind3.1]) + x[ind3.1])
        } else {
            return(x[ind3.1])
        }
        } else {
        if (length(ind2.2) > 0) {
            ind3.2 <- min(ind2.2) # Location of the most recent observations on the right
            return(x[ind3.2])
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
        ind2.1 <- ind1.1[ind1.1 < i] # left
        ind2.2 <- ind1.1[ind1.1 > i] # right
        
        if(length(ind2.1) > 0) {
            ind3.1 <- max(ind2.1) # Location of the most recent observation on the left
            return(x[ind3.1])
        } else if(length(ind2.2) > 0) {
            ind3.2 <- min(ind2.2) # Location of the most recent observations on the right
            return(x[ind3.2])
        } else {
            return(NA)
        }
    })
    
    return(x_filled)
}

fill_zero <- function(x) {
    # The fill_zero function is used to fill the missing value with 0
    # inputs:
    #   - x: Vector representing data that needs to be filled with missing values
    # output:
    #   - Fill the data after the missing value. The type is vector
    # x<-as.character(x)
    x_filled <- x
    x_filled[is.na(x_filled)]<-"0"
    return(x_filled)
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
                        if(type %in% c("num")){
                            x <- as.numeric(df_cur$value)
                            # remove extremes
                            x[x < quantile(x, 0.01, na.rm = TRUE) | x > quantile(x, 0.99, na.rm = TRUE)] <- NA
                            if(!neg_valid) {
                                x[x<0] <- NA
                            }
                            df_cur$value <- x
                            return(df_cur)
                        } else if(type %in% c("cat", "bin","ord")){
                            return(df_cur)
                        }
                    }, 1:length(itemid_list), itemid_list, type_list, SIMPLIFY = F) %>% do.call(rbind, .)
    return(df_new)
}

remove_extreme_value_wide <- function(df, itemid_list, type_list, cols_keep, neg_valid=F){

    mat <- mapply(function(i, itemid, type){
                        if(type %in% c("num")){
                            x <- df[[itemid]]
                            x <- as.numeric(x)
                            # remove extremes
                            x[x < quantile(x, 0.01, na.rm = TRUE) | x > quantile(x, 0.99, na.rm = TRUE)] <- NA
                            if(!neg_valid) {
                                x[x<0] <- NA
                            }
                            return(x)
                        } else if(type %in% c("cat", "bin","ord")){
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