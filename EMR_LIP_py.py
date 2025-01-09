import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional, Any, Callable

def get_dtype_dict(var_dict: pd.DataFrame, table_type: str = "wide") -> Dict:
    """Generate dtype dictionary for pandas read_excel based on var_dict.
    
    Args:
        var_dict: DataFrame containing variable definitions with 'itemid' and 'value_type' columns
        table_type: Type of table format ('long' or 'wide')
        
    Returns:
        Dictionary mapping column names to their data types
    """
    if table_type == "long":
        # 对于 long table，只需要将 value 列设为 str
        return {"value": str}
    else:  # wide table
        dtype_dict = {}
        for _, row in var_dict.iterrows():
            if row['value_type'] in ['cat', 'ord']:
                dtype_dict[row['itemid']] = str
            # num 和 bin 类型可以使用默认的数值类型，不需要特别指定
        return dtype_dict

class EMR_LIP:
    def __init__(self, var_dict: pd.DataFrame, table_type: str = "long"):
        """Initialize EMR_LIP class.
        
        Args:
            var_dict: DataFrame containing variable definitions
            table_type: Type of table format ('long' or 'wide')
        """
        self.var_dict = var_dict.copy()
        self.table_type = table_type
        self._validate_inputs()
        self.stats = {}
        
        # 定义聚合函数字典
        self.AGG_FUNCS = {
            'mean': self._mean,
            'median': self._median,
            'mode': self._mode,
            'mode_w': self._mode_w,
            'mean_w': self._mean_w,
            'median_w': self._median_w,
            'min': self._min,
            'max': self._max,
            'first': self._get_first,
            'last': self._get_last,
            'any': self._any,
            'all': self._all,
            'sum': np.sum,
            'sum_w': np.sum
        }

    def _get_type(self, itemid: str) -> str:
        """Get variable type."""
        return self.var_dict.loc[self.var_dict['itemid'] == itemid, 'value_type'].iloc[0]

    @staticmethod
    def _mean(x: pd.Series, na_rm: bool = True) -> float:
        """Calculate mean.
        
        Args:
            x: Input series
            na_rm: Whether to remove NA values
            
        Returns:
            Mean value as float or None if series is empty
        """
        # 直接使用pandas的mean方法，它内置了NA处理
        result = x.mean() if na_rm else x.mean(skipna=False)
        return float(result) if pd.notna(result) else None

    @staticmethod
    def _median(x: pd.Series, na_rm: bool = True) -> float:
        """Calculate median.
        
        Args:
            x: Input series
            na_rm: Whether to remove NA values
            
        Returns:
            Median value as float or None if series is empty
        """
        # 直接使用pandas的median方法，它内置了NA处理
        result = x.median() if na_rm else x.median(skipna=False)
        return float(result) if pd.notna(result) else None

    @staticmethod
    def _mode(x: pd.Series, na_rm: bool = True) -> str:
        """Calculate mode."""
        if na_rm:
            x = x.dropna()
        if len(x) == 0:
            return None
        
        # 获取值计数
        value_counts = x.value_counts()
        if len(value_counts) == 0:
            return None
            
        # 获取最大频率
        max_freq = value_counts.iloc[0]
        # 获取所有具有最大频率的值
        modes = value_counts[value_counts == max_freq].index.tolist()
        
        if len(modes) == 0:
            return None
        elif len(modes) == 1:
            return str(modes[0])
        else:
            return str(x[x.isin(modes)][::-1].iloc[0])

    @staticmethod
    def _mode_w(x: pd.Series, weights: pd.Series, na_rm: bool = True) -> str:
        """Calculate weighted mode."""
        if na_rm:
            mask = x.notna()
            x = x[mask]
            weights = weights[mask]
        if len(x) == 0:
            return None
        # 计算加权频率
        weighted_counts = pd.Series(weights.values, index=x).groupby(level=0).sum()
        return str(weighted_counts.idxmax())

    @staticmethod
    def _mean_w(x: pd.Series, weights: pd.Series, na_rm: bool = True) -> float:
        """Calculate weighted mean."""
        if na_rm:
            mask = x.notna()
            x = x[mask]
            weights = weights[mask]
        if len(x) == 0:
            return None
        return float(np.average(x, weights=weights))

    @staticmethod
    def _median_w(x: pd.Series, weights: pd.Series, na_rm: bool = True) -> float:
        """Calculate weighted median."""
        if na_rm:
            mask = x.notna()
            x = x[mask]
            weights = weights[mask]
        if len(x) == 0:
            return None
        # 计算加权中位数
        sorted_idx = np.argsort(x)
        sorted_weights = weights.iloc[sorted_idx]
        cumsum = np.cumsum(sorted_weights)
        median_loc = np.searchsorted(cumsum, cumsum[-1] / 2)
        return float(x.iloc[sorted_idx[median_loc]])

    @staticmethod
    def _min(x: pd.Series, na_rm: bool = True) -> float:
        """Calculate minimum."""
        if na_rm:
            x = x.dropna()
        return float(np.min(x)) if len(x) > 0 else None

    @staticmethod
    def _max(x: pd.Series, na_rm: bool = True) -> float:
        """Calculate maximum."""
        if na_rm:
            x = x.dropna()
        return float(np.max(x)) if len(x) > 0 else None

    @staticmethod
    def _get_first(x: pd.Series, na_rm: bool = True) -> Any:
        """Get first value."""
        if na_rm:
            x = x.dropna()
        return x.iloc[0] if len(x) > 0 else None

    @staticmethod
    def _get_last(x: pd.Series, na_rm: bool = True) -> Any:
        """Get last value."""
        if na_rm:
            x = x.dropna()
        return x.iloc[-1] if len(x) > 0 else None

    @staticmethod
    def _any(x: pd.Series, na_rm: bool = True) -> int:
        """Calculate any.
        
        Args:
            x: Input series
            na_rm: Whether to remove NA values
            
        Returns:
            1 if any value is True, 0 if all values are False, None if series is empty
        """
        # 直接使用pandas的any方法，它内置了NA处理
        result = x.any() if na_rm else x.any(skipna=False)
        return int(result) if pd.notna(result) else None

    @staticmethod
    def _all(x: pd.Series, na_rm: bool = True) -> int:
        """Calculate all.
        
        Args:
            x: Input series
            na_rm: Whether to remove NA values
            
        Returns:
            1 if all values are True, 0 if any value is False, None if series is empty
        """
        # 直接使用pandas的all方法，它内置了NA处理
        result = x.all() if na_rm else x.all(skipna=False)
        return int(result) if pd.notna(result) else None

    def _validate_inputs(self):
        """Validate input parameters."""
        if not isinstance(self.var_dict, pd.DataFrame):
            raise TypeError("var_dict must be a pandas DataFrame")
        if self.table_type not in ["wide", "long"]:
            raise ValueError("table_type must be either 'wide' or 'long'")

    ##############################
    # Table Renaming
    ##############################
    def rename_table(self, data: pd.DataFrame, old_name_list: List[str], new_name_list: List[str], 
                    name_col: Optional[str] = None) -> pd.DataFrame:
        """Rename variables in wide or long table.
        
        Args:
            data: Input DataFrame
            old_name_list: List of old variable names
            new_name_list: List of new variable names
            name_col: Column name containing variable names (for long format)
            
        Returns:
            DataFrame with renamed variables
        """
        if len(old_name_list) != len(new_name_list):
            raise ValueError("old_name_list and new_name_list must have the same length")
        
        result = data.copy()    
        if self.table_type == "wide":
            result.rename(columns=dict(zip(old_name_list, new_name_list)), inplace=True)
        elif self.table_type == "long" and name_col:
            result[name_col] = result[name_col].replace(dict(zip(old_name_list, new_name_list)))
        
        return result

    ##############################
    # Remove Extreme Values
    ##############################
    def remove_extreme_values(self, data: pd.DataFrame, 
                              itemid_list: List[str], 
                              type_list: List[str],
                              itemid_col: str = None, 
                              value_col: str = None,
                              sep: str = "___") -> pd.DataFrame:
        """Remove extreme values from variables based on their types.
        
        Args:
            data: Input DataFrame
            itemid_list: List of item IDs
            type_list: List of variable types
            itemid_col: Column name for item IDs (for long format)
            value_col: Column name for values (for long format)
            sep: Separator for splitting valid values string
            
        Returns:
            DataFrame with extreme values removed
        """
        result = data.copy()
        
        if self.table_type == "wide":
            for i, (col, var_type) in enumerate(zip(itemid_list, type_list)):
                if var_type == "num":
                    # 处理数值型变量
                    low = self.var_dict.loc[i, "low"]
                    high = self.var_dict.loc[i, "high"]
                    if pd.notna(low):
                        result[col] = result[col].where(result[col] >= low, np.nan)
                    if pd.notna(high):
                        result[col] = result[col].where(result[col] <= high, np.nan)
                        
                elif var_type in ["ord", "cat"]:
                    # 处理有序型和分类型变量
                    valid_values = self.var_dict.loc[i, "valid_value"]
                    if pd.notna(valid_values):
                        # 首先用sep分割，然后用'|'进一步分割
                        valid_list = [v.strip() for v in valid_values.split('|')]
                        result[col] = result[col].where(result[col].isin(valid_list), np.nan)
                        
                # bin类型不需要处理
                
        else:  # long format
            for i, (itemid, var_type) in enumerate(zip(itemid_list, type_list)):
                ind = (result[itemid_col] == itemid)
                
                if var_type == "num":
                    # 处理数值型变量
                    low = self.var_dict.loc[i, "low"]
                    high = self.var_dict.loc[i, "high"]
                    values = pd.to_numeric(result.loc[ind, value_col], errors='coerce')
                    
                    if pd.notna(low):
                        values = values.where(values >= low, np.nan)
                    if pd.notna(high):
                        values = values.where(values <= high, np.nan)
                        
                    result.loc[ind, value_col] = values
                    
                elif var_type in ["ord", "cat"]:
                    # 处理有序型和分类型变量
                    valid_values = self.var_dict.loc[i, "valid_value"]
                    if pd.notna(valid_values):
                        # 首先用sep分割，然后用'|'进一步分割
                        valid_list = [v.strip() for v in valid_values.split('|')]
                        values = result.loc[ind, value_col].astype(str)
                        result.loc[ind, value_col] = values.where(values.isin(valid_list), np.nan)
                        
                # bin类型不需要处理
                
        return result

    ##############################
    # Calculate Statistics
    ##############################

    def calculate_stats(self, data: pd.DataFrame, itemid_list: List[str], type_list: List[str], 
                    itemid_col: str = None, value_col: str = None,
                    cont_list: List = None, sep: str = "___") -> Dict:
        """Calculate statistics for variables."""
        self.stats = {}
        
        for i, (itemid, var_type) in enumerate(zip(itemid_list, type_list)):
            # Get values based on table type
            if self.table_type == "wide":
                values = data[itemid]
            else:
                mask = data[itemid_col] == itemid
                values = data.loc[mask, value_col]
            
            # Get continuation value
            cont = cont_list[i] if cont_list is not None else None
            
            # Calculate statistics based on variable type
            if var_type == "num":
                numeric_values = pd.to_numeric(values, errors='coerce')
                stats = {
                    "type": var_type,
                    "mean": self._mean(numeric_values),  # 用于 fill_missing
                    "cont": cont if pd.notna(cont) else self._mean(numeric_values)  # 用于 fill_missing
                }
            
            elif var_type in ["cat", "ord"]:  # 合并处理 cat 和 ord
                mode_val = self._mode(values)  # 用于 fill_missing
                # 从 var_dict 获取 valid_value
                valid_value = self.var_dict.loc[self.var_dict['itemid'] == itemid, 'valid_value'].iloc[0]
                if pd.notna(valid_value):
                    # 使用'|'分割并去除空白字符
                    unique_vals = sorted(v.strip() for v in valid_value.split('|'))
                else:
                    unique_vals = sorted(values.dropna().unique())  # 如果 valid_value 为空则使用数据中的唯一值
                stats = {
                    "type": var_type,
                    "mode": mode_val,
                    "unique_values": unique_vals,
                    "cont": cont if pd.notna(cont) else mode_val
                }
            
            elif var_type == "bin":
                numeric_values = pd.to_numeric(values, errors='coerce')
                stats = {
                    "type": var_type,
                    "mean": self._mean(numeric_values),  # 用于 fill_missing
                    "cont": cont if pd.notna(cont) else 0  # 用于 fill_missing
                }
            
            self.stats[itemid] = stats

        return

    ##############################
    # Resample Data
    ##############################
    def resample(self, ds: pd.DataFrame,
                time_list: List[str], 
                itemid_list: List[str] = None,
                type_list: List[str] = None,
                agg_funcs: Dict[str, Callable] = None,
                time_col: str = 'time',
                value_col: str = 'value',
                itemid_col: str = 'itemid',
                time_window: str = '1H',
                direction: str = 'forward',
                start_col: str = None,
                end_col: str = None,
                keep_na_row: bool = True,
                keep_first: bool = True) -> pd.DataFrame:
        """Resample data based on time windows and table type.
        
        Args:
            ds: Input DataFrame
            time_list: List of time points to sample at
            itemid_list: List of item IDs
            type_list: List of variable types
            agg_funcs: List of aggregation functions
            time_col: Name of time column
            value_col: Name of value column (for long format)
            itemid_col: Name of item ID column (for long format)
            time_window: Size of time window
            direction: Direction of time window ('both', 'left', or 'right')
            start_col: Name of start time column (optional)
            end_col: Name of end time column (optional)
            keep_na_row: Whether to keep rows with all NA values
            keep_first: Whether to keep first row if all NA
            
        Returns:
            Resampled DataFrame
        """
        # 根据表格类型选择不同的重采样方法
        if self.table_type == "wide":
            result = self._resample_wide(
                ds=ds,
                time_list=time_list,
                itemid_list=itemid_list,
                type_list=type_list,
                agg_funcs=agg_funcs,
                time_col=time_col,
                end_col=end_col,
                time_window=time_window,
                direction=direction,
                keep_na_row=keep_na_row,
                keep_first=keep_first
            )
        else:
            result = self._resample_long(
                ds=ds,
                time_list=time_list,
                itemid_list=itemid_list,
                type_list=type_list,
                agg_funcs=agg_funcs,
                itemid_col=itemid_col,
                value_col=value_col,
                time_col=time_col,
                end_col=end_col,
                time_window=time_window,
                direction=direction,
                keep_na_row=keep_na_row,
                keep_first=keep_first
            )
        
        return result

    def _resample_wide(self, ds: pd.DataFrame, time_list: List[int], 
                    itemid_list: List[str], type_list: List[str], 
                    agg_funcs: List[Union[str, Callable]], time_col: str, 
                    end_col: str = None, time_window: int = 1, 
                    direction: str = "both", keep_na_row: bool = False, 
                    keep_first: bool = True) -> pd.DataFrame:
        """Resample wide format data."""
        results = []
        
        for cur_t in time_list:
            # 根据是否有区间数据确定时间过滤条件
            if end_col is None:
                # 点数据的时间过滤
                if direction == "both":
                    time_mask = (ds[time_col] >= (cur_t - time_window/2)) & \
                            (ds[time_col] <= (cur_t + time_window/2))
                elif direction == "left":
                    time_mask = (ds[time_col] >= (cur_t - time_window)) & \
                            (ds[time_col] <= cur_t)
                elif direction == "right":
                    time_mask = (ds[time_col] >= cur_t) & \
                            (ds[time_col] <= (cur_t + time_window))
            else:
                # 区间数据的时间过滤
                if direction == "both":
                    time_mask = ((ds[end_col].isna() & 
                                (ds[time_col] >= (cur_t - time_window/2)) & 
                                (ds[time_col] <= (cur_t + time_window/2))) |
                            (~ds[end_col].isna() & 
                                (ds[time_col] <= (cur_t + time_window/2)) & 
                                (ds[end_col] >= (cur_t - time_window/2))))
                elif direction == "left":
                    time_mask = ((ds[end_col].isna() & 
                                (ds[time_col] >= (cur_t - time_window)) & 
                                (ds[time_col] <= cur_t)) |
                            (~ds[end_col].isna() & 
                                (ds[time_col] <= cur_t) & 
                                (ds[end_col] >= (cur_t - time_window))))
                elif direction == "right":
                    time_mask = ((ds[end_col].isna() & 
                                (ds[time_col] >= cur_t) & 
                                (ds[time_col] <= (cur_t + time_window))) |
                            (~ds[end_col].isna() & 
                                (ds[time_col] <= (cur_t + time_window)) & 
                                (ds[end_col] >= cur_t)))
            
            ds_cur = ds[time_mask].copy()
            
            if len(ds_cur) == 0:
                results.append(["0"] + [None] * len(itemid_list))
                continue
                
            # 计算重叠部分（仅对区间数据）
            if end_col is not None:
                if direction == "both":
                    overlap = np.minimum(ds_cur[end_col], cur_t + time_window/2) - \
                            np.maximum(ds_cur[time_col], cur_t - time_window/2)
                elif direction == "left":
                    overlap = np.minimum(ds_cur[end_col], cur_t) - \
                            np.maximum(ds_cur[time_col], cur_t - time_window)
                elif direction == "right":
                    overlap = np.minimum(ds_cur[end_col], cur_t + time_window) - \
                            np.maximum(ds_cur[time_col], cur_t)
                
                overlap = np.maximum(overlap, 0)
                total = ds_cur[end_col] - ds_cur[time_col]
                ds_cur['proportion'] = overlap / total
            
            # 对每个变量应用聚合函数
            cur_x = []
            for itemid, var_type, agg_f in zip(itemid_list, type_list, agg_funcs):
                x = ds_cur[itemid]
                
                if var_type == "num":
                    x = pd.to_numeric(x, errors='coerce')
                    if end_col is not None and agg_f in ['mean_w', 'median_w']:
                        agg_value = self.AGG_FUNCS[agg_f](x, overlap)
                    elif end_col is not None and agg_f == 'sum_w':
                        x = x * ds_cur['proportion']
                        agg_value = self.AGG_FUNCS[agg_f](x)
                    else:
                        agg_value = self.AGG_FUNCS[agg_f](x)
                elif var_type in ["cat", "ord"]:
                    x = x.astype(str)
                    if end_col is not None and agg_f == 'mode_w':
                        agg_value = self.AGG_FUNCS[agg_f](x, overlap)
                    else:
                        agg_value = self.AGG_FUNCS[agg_f](x)
                elif var_type == "bin":
                    x = pd.to_numeric(x, errors='coerce')
                    agg_value = self.AGG_FUNCS[agg_f](x)
                
                cur_x.append(agg_value)
            
            results.append(["1"] + cur_x)
        
        # 转换为DataFrame
        result = pd.DataFrame(results, columns=["keep"] + itemid_list)
        result.insert(0, "time", time_list)
        
        # 处理NA行
        if not keep_na_row:
            result = result[result["keep"] == "1"].copy()
        
        # 处理第一行
        if len(result) > 1 and result.iloc[0, 2:].isna().all():
            if keep_first:
                result.iloc[0, 1] = "0"
            else:
                result = result.iloc[1:].copy()
        
        return result
    
    def _resample_long(self, ds: pd.DataFrame, time_list: List[int], 
                    itemid_list: List[str], type_list: List[str], 
                    agg_funcs: List[Union[str, Callable]], 
                    itemid_col: str, value_col: str, time_col: str, 
                    end_col: str = None, time_window: int = 1,
                    direction: str = "both", keep_na_row: bool = False, 
                    keep_first: bool = True) -> pd.DataFrame:
        """Resample long format data.
        
        Args:
            ds: Input DataFrame
            time_list: List of time points to sample at
            itemid_list: List of item IDs
            type_list: List of variable types
            agg_funcs: List of aggregation functions
            itemid_col: Column name for item IDs
            value_col: Column name for values
            time_col: Name of time column
            end_col: Name of end time column (optional)
            time_window: Size of time window
            direction: Direction of time window ('both', 'left', or 'right')
            keep_na_row: Whether to keep rows with all NA values
            keep_first: Whether to keep first row if all NA
            
        Returns:
            Resampled DataFrame
        """
        results = []
        
        # 对每个时间点进行处理
        for cur_t in time_list:
            # 根据是否有区间数据确定时间过滤条件
            if end_col is None:
                # 点数据的时间过滤
                if direction == "both":
                    time_mask = (ds[time_col] >= (cur_t - time_window/2)) & \
                            (ds[time_col] <= (cur_t + time_window/2))
                elif direction == "left":
                    time_mask = (ds[time_col] >= (cur_t - time_window)) & \
                            (ds[time_col] <= cur_t)
                elif direction == "right":
                    time_mask = (ds[time_col] >= cur_t) & \
                            (ds[time_col] <= (cur_t + time_window))
            else:
                # 区间数据的时间过滤
                if direction == "both":
                    time_mask = ((ds[end_col].isna() & 
                                (ds[time_col] >= (cur_t - time_window/2)) & 
                                (ds[time_col] <= (cur_t + time_window/2))) |
                            (~ds[end_col].isna() & 
                                (ds[time_col] <= (cur_t + time_window/2)) & 
                                (ds[end_col] >= (cur_t - time_window/2))))
                elif direction == "left":
                    time_mask = ((ds[end_col].isna() & 
                                (ds[time_col] >= (cur_t - time_window)) & 
                                (ds[time_col] <= cur_t)) |
                            (~ds[end_col].isna() & 
                                (ds[time_col] <= cur_t) & 
                                (ds[end_col] >= (cur_t - time_window))))
                elif direction == "right":
                    time_mask = ((ds[end_col].isna() & 
                                (ds[time_col] >= cur_t) & 
                                (ds[time_col] <= (cur_t + time_window))) |
                            (~ds[end_col].isna() & 
                                (ds[time_col] <= (cur_t + time_window)) & 
                                (ds[end_col] >= cur_t)))
            
            ds_cur = ds[time_mask].copy()
            
            if len(ds_cur) == 0:
                results.append(["0"] + [None] * len(itemid_list))
                continue
                
            # 计算重叠部分（仅对区间数据）
            if end_col is not None:
                if direction == "both":
                    overlap = np.minimum(ds_cur[end_col], cur_t + time_window/2) - \
                            np.maximum(ds_cur[time_col], cur_t - time_window/2)
                elif direction == "left":
                    overlap = np.minimum(ds_cur[end_col], cur_t) - \
                            np.maximum(ds_cur[time_col], cur_t - time_window)
                elif direction == "right":
                    overlap = np.minimum(ds_cur[end_col], cur_t + time_window) - \
                            np.maximum(ds_cur[time_col], cur_t)
                
                overlap = np.maximum(overlap, 0)
                total = ds_cur[end_col] - ds_cur[time_col]
                ds_cur['proportion'] = overlap / total
            
            # 对每个变量应用聚合函数
            cur_x = []
            for itemid, var_type, agg_f in zip(itemid_list, type_list, agg_funcs):
                # 获取当前变量的数据
                mask = ds_cur[itemid_col] == itemid
                x = ds_cur.loc[mask, value_col]
                
                if len(x) == 0:
                    cur_x.append(None)
                    continue
                
                if var_type == "num":
                    x = pd.to_numeric(x, errors='coerce')
                    if end_col is not None and agg_f in ['mean_w', 'median_w']:
                        agg_value = self.AGG_FUNCS[agg_f](x, overlap[mask])
                    elif end_col is not None and agg_f == 'sum_w':
                        x = x * ds_cur.loc[mask, 'proportion']
                        agg_value = self.AGG_FUNCS[agg_f](x)
                    else:
                        agg_value = self.AGG_FUNCS[agg_f](x)
                elif var_type in ["cat", "ord"]:
                    x = x.astype(str)
                    if end_col is not None and agg_f == 'mode_w':
                        agg_value = self.AGG_FUNCS[agg_f](x, overlap[mask])
                    else:
                        agg_value = self.AGG_FUNCS[agg_f](x)
                elif var_type == "bin":
                    x = pd.to_numeric(x, errors='coerce')
                    agg_value = self.AGG_FUNCS[agg_f](x)
                
                cur_x.append(agg_value)
            
            results.append(["1"] + cur_x)
        
        # 转换为DataFrame
        result = pd.DataFrame(results, columns=["keep"] + itemid_list)
        result.insert(0, "time", time_list)
        
        # 处理NA行
        if not keep_na_row:
            result = result[result["keep"] == "1"].copy()
        
        # 处理第一行
        if len(result) > 1 and result.iloc[0, 2:].isna().all():
            if keep_first:
                result.iloc[0, 1] = "0"
            else:
                result = result.iloc[1:].copy()
        
        return result
    ##############################
    # Fill Missing Values
    ##############################

    def _fill_lin(self, x: pd.Series, time: pd.Index) -> pd.Series:
        """Fill missing values using linear interpolation.
        
        Args:
            x: Series with missing values
            time: Time index or series
        """
        result = x.copy()
        
        # Get indices of NA and non-NA values
        na_idx = x.isna()
        non_na_idx = ~na_idx
        
        # If all values are NA or no values are NA, return original series
        if na_idx.all() or not na_idx.any():
            return result
            
        # Convert time to numeric series if it's not already
        if isinstance(time, pd.Index):
            time = pd.Series(time.values, index=time)
        
        # Iterate through NA indices
        for idx in x[na_idx].index:
            # Find nearest non-missing values on both sides
            left_vals = x[non_na_idx & (time < time[idx])]
            right_vals = x[non_na_idx & (time > time[idx])]
            
            if len(left_vals) > 0 and len(right_vals) > 0:
                # Get nearest values
                left_val = left_vals.iloc[-1]
                right_val = right_vals.iloc[0]
                left_time = time[left_vals.index[-1]]
                right_time = time[right_vals.index[0]]
                
                # Linear interpolation
                result[idx] = (
                    left_val + 
                    (right_val - left_val) * 
                    (time[idx] - left_time) / 
                    (right_time - left_time)
                )
            elif len(left_vals) > 0:
                # If only left values exist, use last left value
                result[idx] = left_vals.iloc[-1]
            elif len(right_vals) > 0:
                # If only right values exist, use first right value
                result[idx] = right_vals.iloc[0]
                
        return result

    def _fill_column(self, series: pd.Series, value_type: str,
                    fill1: str, fill2: str, time_col: pd.Series = None,
                    stats: Dict = None) -> pd.Series:
        """Fill missing values based on value type and fill methods."""
        if stats is None or not isinstance(stats, dict):
            raise ValueError("stats must be provided as a dictionary")
        
        result = series.copy()
        if not result.isna().any():
            return result

        # Get continuation value based on type
        cont = stats.get("cont")
        if pd.isna(cont):
            if value_type == "num":
                cont = stats.get("mean")
            elif value_type in ["cat", "ord"]:  # 合并处理 cat 和 ord
                cont = stats.get("mode")
            elif value_type == "bin":
                cont = 0

        # Fill first missing value
        if pd.isna(result.iloc[0]):
            if value_type == "num":
                if fill1 == "mean":
                    result.iloc[0] = stats.get("mean")
                elif fill1 == "median":
                    result.iloc[0] = stats.get("mean")  # 使用 mean 替代 median
                elif fill1 == "cont":
                    result.iloc[0] = cont
                elif fill1 == "mean_k":
                    result.iloc[0] = (cont if result.isna().all() 
                                    else result.mean(skipna=True))
                elif fill1 == "median_k":
                    result.iloc[0] = (cont if result.isna().all() 
                                    else result.mean(skipna=True))  # 使用 mean 替代 median
            elif value_type in ["cat", "ord"]:  # 合并处理 cat 和 ord
                if fill1 in ["mode", "median", "mean"]:  # 所有数值相关的填充方法都使用 mode
                    result.iloc[0] = stats.get("mode")
                elif fill1 == "cont":
                    result.iloc[0] = cont
                elif fill1 in ["mode_k", "mean_k", "median_k"]:  # 所有 *_k 方法都使用 mode_k
                    result.iloc[0] = (cont if result.isna().all() 
                                    else self._calculate_mode(result))
            elif value_type == "bin":
                if fill1 == "cont":
                    result.iloc[0] = cont
                
            if fill1 == "locb":
                first_valid = result.first_valid_index()
                result.iloc[0] = (cont if first_valid is None 
                                else result.iloc[first_valid])
            elif fill1 == "zero":
                result.iloc[0] = 0

        # Fill remaining missing values
        if value_type == "num":
            if fill2 == "lin" and time_col is not None:
                result = self._fill_lin(result, time_col)
            elif fill2 == "mean":
                result.fillna(stats.get("mean"), inplace=True)
            elif fill2 == "median":
                result.fillna(stats.get("mean"), inplace=True)  # 使用 mean 替代 median
            elif fill2 == "mean_k":
                fill_value = cont if result.isna().all() else result.mean(skipna=True)
                result.fillna(fill_value, inplace=True)
            elif fill2 == "median_k":
                fill_value = cont if result.isna().all() else result.mean(skipna=True)  # 使用 mean 替代 median
                result.fillna(fill_value, inplace=True)
        elif value_type in ["cat", "ord"]:  # 合并处理 cat 和 ord
            if fill2 in ["mode", "mean", "median"]:  # 所有数值相关的填充方法都使用 mode
                result.fillna(stats.get("mode"), inplace=True)
            elif fill2 in ["mode_k", "mean_k", "median_k"]:  # 所有 *_k 方法都使用 mode_k
                fill_value = cont if result.isna().all() else self._calculate_mode(result)
                result.fillna(fill_value, inplace=True)
        
        # Common fill methods for all types
        if fill2 == "zero":
            result.fillna(0, inplace=True)
        elif fill2 == "cont":
            result.fillna(cont, inplace=True)
        elif fill2 == "locf":
            result.fillna(method='ffill', inplace=True)
        elif fill2 == "locb":
            result.fillna(method='bfill', inplace=True)

        return result


    def fill_missing(self, resampled: pd.DataFrame, col_list: List[str], time_col: str,
                    type_list: List[str], fill1_list: List[str],
                    fill2_list: List[str]) -> pd.DataFrame:
        """Fill missing values for resampled data.
        
        Args:
            resampled: Resampled DataFrame (output from resample method)
            col_list: List of column names to process
            time_col: Name of time column
            type_list: List of variable types
            fill1_list: List of methods to fill first missing value
            fill2_list: List of methods to fill remaining missing values
            
        Returns:
            DataFrame with filled missing values
        """
        result = resampled.copy()
        
        # Fill missing values for each column
        for col, vtype, fill1, fill2 in zip(col_list, type_list, fill1_list, fill2_list):
            result[col] = self._fill_column(
                series=result[col],
                value_type=vtype,
                fill1=fill1,
                fill2=fill2,
                time_col=result[time_col],
                stats=self.stats.get(col, {})
            )
        
        return result

    def fill_last_values(self, ds: pd.DataFrame, mask: pd.DataFrame, 
                        itemid_col: str = 'itemid') -> pd.DataFrame:
        """Fill missing values with last non-missing value for each variable.
        
        Args:
            ds: Resampled DataFrame
            mask: Binary mask indicating presence (1) or absence (0) of values
            itemid_col: Name of the itemid column in self.var_dict (default: 'itemid')
            
        Returns:
            DataFrame with missing values filled using last non-missing values
        """
        result = ds.copy()
        itemid_list = self.var_dict[itemid_col].tolist()
        
        # 处理每个变量
        for i, col in enumerate(itemid_list):
            # 找到mask中值为1的位置
            ind = mask[col] == 1
            if not ind.any():
                continue
                
            # 找到最后一个非缺失值的位置
            last_present = ind.values.nonzero()[0][-1]
            first_ind = last_present + 1
            
            # 如果first_ind已经是最后一个观测，则跳过
            if first_ind >= len(result):
                continue
                
            # 使用self.var_dict获取last_value
            last_value = self.var_dict.loc[self.var_dict[itemid_col] == col, 'last_value'].iloc[0]
            if pd.notna(last_value):
                result.iloc[first_ind:, result.columns.get_loc(col)] = last_value
        
        return result

    ##############################
    # Mask and Delta Time
    ##############################
    def shape_as_onehot(self, ds: pd.DataFrame, col_list: List[str], 
                        time_col: str) -> pd.DataFrame:
        """Shape matrix to match one-hot encoding structure without actual encoding."""
        # 生成结果列名
        shaped_cols = []
        for col in col_list:
            var_type = self.var_dict.loc[self.var_dict['itemid'] == col, 'value_type'].iloc[0]
            if var_type in ['num', 'bin']:  # 移除 'ord'
                shaped_cols.append(col)
            else:  # categorical 和 ordinal
                n_categories = len(self.stats[col]['unique_values'])
                shaped_cols.extend([f"{col}___{i+1}" for i in range(n_categories)])
        
        # 创建结果 DataFrame，从时间列开始
        result = [ds[time_col]]
        
        # 处理每一列
        for col in col_list:
            var_type = self.var_dict.loc[self.var_dict['itemid'] == col, 'value_type'].iloc[0]
            if var_type in ['num', 'bin']:  # 移除 'ord'
                result.append(ds[col])
            else:  # categorical 和 ordinal
                n_categories = len(self.stats[col]['unique_values'])
                repeated_cols = [ds[col].copy() for _ in range(n_categories)]
                result.extend(repeated_cols)
        
        # 合并所有列并设置列名
        result = pd.concat(result, axis=1)
        result.columns = [time_col] + shaped_cols
        
        return result

    def get_mask(self, ds: pd.DataFrame, itemid_list: List[str], time_col: str) -> pd.DataFrame:
        """Generate missing value mask for resampled variables.
        
        Args:
            ds: Resampled DataFrame (output from resample method)
            itemid_list: List of item IDs
            time_col: Name of time column
            
        Returns:
            DataFrame with binary mask indicating presence (1) or absence (0) of values
        """
        # Create mask for resampled columns
        mask = ds[itemid_list].notna().astype(int)
        
        # Add time column
        result = pd.concat([ds[time_col], mask], axis=1)
        
        return result

    def get_delta_time(self, ds: pd.DataFrame, itemid_list: List[str], 
                    time_col: str) -> pd.DataFrame:
        """Calculate time differences for resampled variables.
        
        Args:
            ds: Resampled DataFrame (output from resample method)
            itemid_list: List of item IDs
            time_col: Name of time column
            
        Returns:
            DataFrame with time differences since last non-missing value
        """
        # 创建一个新的DataFrame来存储时间差
        delta = pd.DataFrame(index=ds.index)
        time_values = ds[time_col].values
        
        # 为每个变量计算时间差
        for col in itemid_list:
            mask = ds[col].notna().astype(int).values
            delta[col] = np.zeros(len(mask))
            
            for t in range(1, len(mask)):
                if mask[t] == 1:  # 当前时间点有值
                    if mask[t-1] == 0:  # 前一个时间点无值
                        # 找到前一个有值的位置
                        last_valid = t - 1
                        while last_valid >= 0 and mask[last_valid] == 0:
                            last_valid -= 1
                        
                        if last_valid >= 0:
                            delta[col].iloc[t] = time_values[t] - time_values[last_valid]
                        else:
                            delta[col].iloc[t] = 0  # 如果之前没有值，设为0
                    else:  # 前一个时间点有值
                        delta[col].iloc[t] = time_values[t] - time_values[t-1]
                else:  # 当前时间点无值
                    delta[col].iloc[t] = 0
        
        # 添加时间列
        result = pd.concat([ds[time_col], delta], axis=1)
        return result

    def process_temporal_features(self, ds: pd.DataFrame, col_list: List[str], 
                                time_col: str) -> Dict[str, pd.DataFrame]:
        """Process temporal features including mask and delta time.
        
        Args:
            ds: Input DataFrame
            col_list: List of column names
            time_col: Name of time column
            
        Returns:
            Dictionary containing processed features:
            - 'mask': Shaped mask matrix
            - 'delta': Shaped delta time matrix
        """
        result = {}
        
        # 生成 mask 矩阵
        mask = self.get_mask(ds, col_list, time_col)
        # 转换 mask 矩阵结构
        shaped_mask = self.shape_as_onehot(mask, col_list, time_col)
        result['mask'] = shaped_mask
        
        # 生成 delta time 矩阵
        delta = self.get_delta_time(ds, col_list, time_col)
        # 转换 delta time 矩阵结构
        shaped_delta = self.shape_as_onehot(delta, col_list, time_col)
        result['delta'] = shaped_delta
        
        return result

    ##############################
    # One-hot Encoding
    ##############################
    def to_onehot(self, ds: pd.DataFrame, col_list: List[str]) -> pd.DataFrame:
        """Convert categorical columns to one-hot encoding."""
        result_cols = []
        
        for col in col_list:
            # 从 self.var_dict 获取变量类型
            dtype = self.var_dict.loc[self.var_dict['itemid'] == col, 'value_type'].iloc[0]
            
            if dtype in ['num', 'bin']:  # 移除 'ord'
                result_cols.append(ds[col])
            else:  # categorical 和 ordinal
                # 从 self.stats 获取唯一值
                unique_values = self.stats[col]['unique_values']
                
                # 创建 one-hot 列
                for i, val in enumerate(unique_values, 1):
                    col_name = f"{col}___{i}"
                    result_cols.append(
                        pd.Series(
                            (ds[col] == val).astype(int),
                            name=col_name
                        )
                    )
        
        # 合并所有列
        result = pd.concat([ds.iloc[:, 0]] + result_cols, axis=1)  # 保留时间列
        return result

    def rev_onehot(self, ds: pd.DataFrame, col_list: List[str], time_col: str) -> pd.DataFrame:
        """Reverse one-hot encoding back to original data format."""
        result_cols = []
        
        # 保留时间列
        result_cols.append(ds[time_col])
        
        current_pos = 0
        # 处理每个原始列
        for col in col_list:
            # 获取变量类型
            var_type = self.var_dict.loc[self.var_dict['itemid'] == col, 'value_type'].iloc[0]
            
            if var_type in ['num', 'bin']:  # 移除 'ord'
                # 数值型列直接保留
                result_cols.append(ds[col])
                current_pos += 1
            else:  # categorical 和 ordinal
                # 获取该分类变量的所有可能值
                unique_values = self.stats[col]['unique_values']
                n_categories = len(unique_values)
                
                # 获取one-hot列
                onehot_cols = [f"{col}___{i+1}" for i in range(n_categories)]
                onehot_data = ds[onehot_cols]
                
                # 转换回原始分类
                def get_category(row):
                    # 找到值为1的位置
                    ind = row.values.nonzero()[0]
                    if len(ind) == 0:
                        return pd.NA
                    return unique_values[ind[0]]
                
                reversed_col = onehot_data.apply(get_category, axis=1)
                reversed_col.name = col
                result_cols.append(reversed_col)
                current_pos += n_categories
        
        # 合并所有列
        result = pd.concat(result_cols, axis=1)
        return result