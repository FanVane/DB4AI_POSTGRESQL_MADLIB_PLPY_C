#include "postgres.h" // 包含在每个声明postgres函数的C文件中

#include "fmgr.h" // 用于PG_GETARG_XXX 以及 PG_RETURN_XXX
#include "access/hash.h"
// #include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h" // 用于提供ArrayType*
#include "executor/spi.h" // 用于调用SPI

#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#define MAX_SQL_LEN 1024
/*
 * Taken from the intarray contrib header
 */
#define ARRPTR(x)  ( (double *) ARR_DATA_PTR(x) )
#define ARRNELEMS(x)  ArrayGetNItems( ARR_NDIM(x), ARR_DIMS(x))

PG_MODULE_MAGIC; // 由PostgreSQL在加载时检查兼容性

/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将输入表按元素取绝对值的结果存放到输出表中。
// 参数：输入表名 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_absP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_absP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_abs(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 检测输入表是否存在，不存在则报错(注意无需双引号)
    ESPI_show_message(CHECKING IF INPUT_TABLE EXISTS...);
    
    // if(!ESPI_table_exists(input_table_name)){
    //     // 字符串三部曲：声明，拷贝，连接。
    //     char errmsg[MAX_SQL_LEN]; // 先声明数组，注意不是char*而是char[]
    //     strcpy(errmsg,"TABLE NOT EXIST: "); // 再拷贝字符串
    //     strcat(errmsg, input_table_name); // 再连接字符串
    //     ESPI_show_message(errmsg); // 最后显示字符串
    //     // 在分支的位置一定要及时关闭连接！
    //     SPI_finish();  // 必须：关闭连接
    //     PG_RETURN_INT32(-1); // 返回结果状态码
    // }

    // 清空输出表
    // ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: SELECT rows, cols, trans, inner_db4ai_abs(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    strcpy(sql, "SELECT rows, cols, trans, inner_db4ai_abs(data) as data into ");
    strcat(sql, output_table_name);
    strcat(sql, " from ");
    strcat(sql, input_table_name);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_abs(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组）
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);
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


/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将输入表相加的结果存放到输出表中。
// 参数：输入表名1 输入表名2 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(outer_db4ai_add); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(inner_db4ai_add);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_add(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 检测输入表是否存在，不存在则报错
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name1) || !ESPI_table_exists(input_table_name2)){
        // 字符串三部曲：声明，拷贝，连接。
        char errmsg[MAX_SQL_LEN]; // 先声明数组，注意不是char*而是char[]
        strcpy(errmsg,"TABLE NOT EXIST! "); // 再拷贝字符串
        ESPI_show_message(errmsg); // 最后显示字符串
        // 在分支的位置一定要及时关闭连接！
        SPI_finish();  // 必须：关闭连接
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    // 清空输出表
    ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_add(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组X2）
    ArrayType* arr_raw1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr_raw2 = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr_raw1);
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr_raw2);
    int size = ARRNELEMS(arr_raw1);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]+arr2[i]);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将输入表相除的结果存放到输出表中。
// 参数：输入表名1 / 输入表名2 =  输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(outer_db4ai_div); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(inner_db4ai_div);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_div(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 检测输入表是否存在，不存在则报错
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name1) || !ESPI_table_exists(input_table_name2)){
        // 字符串三部曲：声明，拷贝，连接。
        char errmsg[MAX_SQL_LEN]; // 先声明数组，注意不是char*而是char[]
        strcpy(errmsg,"TABLE NOT EXIST! "); // 再拷贝字符串
        ESPI_show_message(errmsg); // 最后显示字符串
        // 在分支的位置一定要及时关闭连接！
        SPI_finish();  // 必须：关闭连接
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    // 清空输出表
    ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_div(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组X2）
    ArrayType* arr_raw1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr_raw2 = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr_raw1);
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr_raw2);
    int size = ARRNELEMS(arr_raw1);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]/arr2[i]);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}




/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将输入表按元素取以e为底的指数的结果存放到输出表中。
// 参数：输入表名 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(outer_db4ai_exp); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(inner_db4ai_exp);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_exp(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 检测输入表是否存在，不存在则报错
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name)){
        // 字符串三部曲：声明，拷贝，连接。
        char errmsg[MAX_SQL_LEN]; // 先声明数组，注意不是char*而是char[]
        strcpy(errmsg,"TABLE NOT EXIST: "); // 再拷贝字符串
        strcat(errmsg, input_table_name); // 再连接字符串
        ESPI_show_message(errmsg); // 最后显示字符串
        // 在分支的位置一定要及时关闭连接！
        SPI_finish();  // 必须：关闭连接
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    // 清空输出表
    ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: SELECT rows, cols, trans, inner_db4ai_exp(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    strcpy(sql, "SELECT rows, cols, trans, inner_db4ai_exp(data) as data into ");
    strcat(sql, output_table_name);
    strcat(sql, " from ");
    strcat(sql, input_table_name);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_exp(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组）
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);
    int size = ARRNELEMS(arr_raw);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(exp(arr[i]));
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}



/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将输入表相X的结果存放到输出表中。
// 参数：输入表名1 输入表名2 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(outer_db4ai_mul); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(inner_db4ai_mul);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_mul(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 检测输入表是否存在，不存在则报错
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name1) || !ESPI_table_exists(input_table_name2)){
        // 字符串三部曲：声明，拷贝，连接。
        char errmsg[MAX_SQL_LEN]; // 先声明数组，注意不是char*而是char[]
        strcpy(errmsg,"TABLE NOT EXIST! "); // 再拷贝字符串
        ESPI_show_message(errmsg); // 最后显示字符串
        // 在分支的位置一定要及时关闭连接！
        SPI_finish();  // 必须：关闭连接
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    // 清空输出表
    ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_mul(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组X2）
    ArrayType* arr_raw1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr_raw2 = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr_raw1);
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr_raw2);
    int size = ARRNELEMS(arr_raw1);                          // 用ARRNELEMS从源数据中获取数组的元素个数
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]*arr2[i]);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将按照输入的数字，建立全0的输出表。
// 参数：行数 列数 输出表名
// 返回：0正常
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z17outer_db4ai_zerosP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z16inner_db4ai_onesP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_ones(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 清空输出表
    ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_ones(<rows> * <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_ones(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_ones(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组）
    int32 size = PG_GETARG_INT32(0);
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(1.0);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将按照输入的数字，建立全0的输出表。
// 参数：行数 列数 输出表名
// 返回：0正常
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(outer_db4ai_zeros); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(inner_db4ai_zeros);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_zeros(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接
    ESPI_init_message(); // 必须：初始化MSG表格
    // 清空输出表
    ESPI_drop_table_if_exists(output_table_name);
    // 调用INNER函数: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_zeros(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_zeros(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);
    // 关闭连接
    SPI_finish();  // 必须：关闭连接
    // 返回结果
    PG_RETURN_INT32(0); // 返回结果状态码
}
// 内部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
inner_db4ai_zeros(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数（一维double8数组）
    int32 size = PG_GETARG_INT32(0);
    // 构建一个Datum数组
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // 用palloc动态分配内存
    // 主要逻辑部分
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(0.0);
    }
    // 返回该数组
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}



/*
注意这一行！！
if (SPI_processed > 0)
	{
		selected = DatumGetInt32(CStringGetDatum(SPI_getvalue(
													   SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc,
																		 1
																		)));
	}

*/