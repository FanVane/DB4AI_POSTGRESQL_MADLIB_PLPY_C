#include "postgres.h" // 包含在每个声明postgres函数的C文件中

#include "fmgr.h" // 用于PG_GETARG_XXX 以及 PG_RETURN_XXX
#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h" // 用于提供ArrayType*

#include <stdlib.h>
#include <math.h>
/*
 * Taken from the intarray contrib header
 */
#define ARRPTR(x)  ( (double *) ARR_DATA_PTR(x) )
#define ARRNELEMS(x)  ArrayGetNItems( ARR_NDIM(x), ARR_DIMS(x))


// 由PostgreSQL在加载时检查兼容性
PG_MODULE_MAGIC;

/**
 * 基本功能测试：将输入的两个整数相加
 *  @param arg1 第一个参数
 *  @param arg2 第二个参数
 *  @author none
 */
PG_FUNCTION_INFO_V1(add_ab); // 说明使用的模式是V1
Datum // 所有Postgres函数的返回类型都是Datum
add_ab(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    int32 arg_a = PG_GETARG_INT32(0); // 为参数获取值
    int32 arg_b = PG_GETARG_INT32(1);
    PG_RETURN_INT32(arg_a + arg_b); // 返回函数值
}


/**
 * 输入一个float8数组，对每个元素求绝对值之后返回数组。
 * @param arr_raw 输入的float8数组
 * @return 求绝对值之后的数组
 * @author db4ai_abs
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_abs);
Datum
__db4ai_execute_row_abs(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr[i]<0?-arr[i]:arr[i]);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * 输入一个float8数组，对每个元素求对数之后返回数组。
 * @param arr_raw 输入的float8数组
 * @return 求对数之后的数组
 * @author db4ai_log
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_log);
Datum
__db4ai_execute_row_log(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(log(arr[i]));
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * 输入一个float8数组，对每个元素求平方根之后返回数组。
 * @param arr_raw 输入的float8数组
 * @return 求平方根之后的数组
 * @author db4ai_sqrt
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_sqrt);
Datum
__db4ai_execute_row_sqrt(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(sqrt(arr[i]));
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * 输入一个float8数组，对每个元素求e为底的指数之后返回数组。
 * @param arr_raw 输入的float8数组
 * @return 求e为底的指数之后的数组
 * @author db4ai_exp
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_exp);
Datum
__db4ai_execute_row_exp(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(exp(arr[i]));
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * 输入一个float8数组，对每个元素求余数之后返回数组。
 * @param arr1_raw 输入的float8数组被除数
 * @param arr2_raw 输入的float8数组除数
 * @return 求除法之后的数组
 * @author db4ai_div
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_div);
Datum
__db4ai_execute_row_div(PG_FUNCTION_ARGS){
    ArrayType* arr1_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    ArrayType* arr2_raw = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr1_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr2_raw);
    int size = ARRNELEMS(arr1_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]/arr2[i]);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * 输入一个float8数组，对每个元素为底，求输入元素对应的指数之后返回数组。
 * @param arr_raw 输入的float8数组
 * @param pow_exp 进行指数的值
 * @return 指数运算之后的数组
 * @author db4ai_pow
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_pow);
Datum
__db4ai_execute_row_pow(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8 pow_exp = PG_GETARG_FLOAT8(1);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(pow(arr[i], pow_exp));
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * 输入长度和数值，返回这么长的一个填充数组。
 * @param size 将要输出的float8数组长度
 * @param full_value 进行填充的值
 * @return 指数运算之后的数组
 * @author db4ai_pow db4ai_trace
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_full);
Datum
__db4ai_execute_row_full(PG_FUNCTION_ARGS){
    int32 size = PG_GETARG_INT32(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8 full_value = PG_GETARG_FLOAT8(1);
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(full_value);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * 输入float8数组，返回它各元素总和。
 * @param arr_raw 将入输出的float8数组
 * @return 该数组的元素之和
 * @author db4ai_trace
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_sum);
Datum
__db4ai_execute_row_sum(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8 sum = 0.0;
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    for(int i=0; i<size; i++){
        sum += arr[i];
    }
    PG_RETURN_FLOAT8(sum);
}

/**
 * 输入2个float8数组，求点积之后返回数。
 * @param arr1_raw 输入的float8数组1
 * @param arr2_raw 输入的float8数组2
 * @return 求点积之后的数
 * @author db4ai_div
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_dot);
Datum
__db4ai_execute_row_dot(PG_FUNCTION_ARGS){
    ArrayType* arr1_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    ArrayType* arr2_raw = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr1_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr2_raw);
    int size = ARRNELEMS(arr1_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    float8 dot = 0.0;
    for(int i=0; i<size; i++){
        dot += arr1[i] * arr2[i];
    }
    // 返回该数组
    PG_RETURN_FLOAT8(dot);
}

/**
 * 输入1个float8数组，索引排序之后返回float8数组（在C里面还是纯float8吧）。
 * @param arr_raw 输入的float8数组
 * @return 整型索引数组
 * @author db4ai_argsort
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_argsort);
Datum
__db4ai_execute_row_argsort(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 索引排序：维护一个同样大小的mask数组，扫描n次，每次找到最小值及其索引，添加到返回数组中
    int* mask = (int*) malloc(size * sizeof(int));
    for(int i=0; i<size; i++)   mask[i] = 0;
    int min_cur = -1;
    for(int i=0; i<size; i++){
        float8 min_val = __FLT64_MAX__;
        // 寻找最小的那些东西
        for (int j = 0; j < size; j++)
        {
            if(mask[j]!=0) continue;
            if(arr[j]<min_val){
                min_cur = j;
                min_val = arr[j];
            }
        }
        // 将最小的索引覆盖在mask上并进入返回数组中
        mask[min_cur] = 1;
        ans_arr_back[i] = Float8GetDatum((float8)min_cur+1);
    }
    free(mask);
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * 输入一个float8数组，对每个元素求softmax之后返回数组。
 * @param arr_raw 输入的float8数组
 * @return 求softmax之后的数组
 * @author db4ai_softmax
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_softmax);
Datum
__db4ai_execute_row_softmax(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    float8 bottum = 0.0;
    for(int i=0; i<size; i++){
        bottum += exp(arr[i]);
    }
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(exp(arr[i])/bottum);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * 输入1个float8数组，排序之后返回float8数组。
 * @param arr_raw 输入的float8数组
 * @return 排序结果数组
 * @author db4ai_args
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_sort);
Datum
__db4ai_execute_row_sort(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    float8* ans_arr = (float8*) malloc(size * sizeof(float8));
    // 先复制再说
    for(int i=0; i<size; i++) ans_arr[i] = arr[i];
    // 冒泡排序
    for(int i=0; i<size-1; i++){
        for(int j=0; j<size-i-1; j++){
            if(ans_arr[j]>ans_arr[j+1]){
                float8 temp = ans_arr[j];
                ans_arr[j] = ans_arr[j+1];
                ans_arr[j+1] = temp;
            }
        }
    }
    for(int i=0; i<size; i++)   ans_arr_back[i] = Float8GetDatum(ans_arr[i]);
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * 输入位置相关信息，计算出新的行号。
 * @param row 目前的行
 * @param col 目前的列
 * @param old_dim2 原先列数
 * @param dim2 目标列数
 * @return 新的行号
 * @author db4ai_reshape
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_reshape); // 说明使用的模式是V1
Datum // 所有Postgres函数的返回类型都是Datum
__db4ai_execute_row_reshape(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    int32 row = PG_GETARG_INT32(0)-1; // 注意：-1是为了便于计算，之后的结果再+1
    int32 col = PG_GETARG_INT32(1)-1;
    int32 old_dim2 = PG_GETARG_INT32(2);
    int32 dim2 = PG_GETARG_INT32(3);
    int32 rank = row * old_dim2 + col;
    int32 new_row = rank/dim2+1;
    PG_RETURN_INT32(new_row); // 返回函数值
}

/**
 * 输入位置相关信息，计算出新的列号。
 * @param row 目前的行
 * @param col 目前的列
 * @param old_dim2 原先列数
 * @param dim2 目标列数
 * @return 新的列号
 * @author db4ai_reshape
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_col_reshape); // 说明使用的模式是V1
Datum // 所有Postgres函数的返回类型都是Datum
__db4ai_execute_col_reshape(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    int32 row = PG_GETARG_INT32(0)-1; // 注意：-1是为了便于计算，之后的结果再+1
    int32 col = PG_GETARG_INT32(1)-1;
    int32 old_dim2 = PG_GETARG_INT32(2);
    int32 dim2 = PG_GETARG_INT32(3);
    int32 rank = row * old_dim2 + col;
    int32 new_col = rank - ((rank/dim2)*dim2) +1;
    PG_RETURN_INT32(new_col); // 返回函数值
}

/**
 * 输入1个float8数组和一个数字，复读之后返回float8数组。
 * @param arr_raw 输入的float8数组
 * @param repeat 重复的次数
 * @return 复读结果数组
 * @author db4ai_repeat
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_repeat);
Datum
__db4ai_execute_row_repeat(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    int32 repeat = PG_GETARG_INT32(1);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(repeat * size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        for(int j=0; j<repeat; j++){
            ans_arr_back[j * size+i] = Float8GetDatum(arr[i]);
        }
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, repeat * size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * 输入1个float8数组和一个数字，翻转之后返回float8数组。
 * @param arr_raw 输入的float8数组
 * @return 翻转的结果
 * @author db4ai_reverse
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_flip);
Datum
__db4ai_execute_row_flip(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // 用PG_GETARG_ARRAYTYPE_P(_COPY) 获取数组类型的指针
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // 用ARR_DATA_PTR获取实际的指针（就是数组的头）
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr[size-1-i]);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}