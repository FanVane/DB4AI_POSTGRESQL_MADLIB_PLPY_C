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

