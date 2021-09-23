#include "postgres.h" // 包含在每个声明postgres函数的C文件中

#include "fmgr.h" // 用于PG_GETARG_XXX 以及 PG_RETURN_XXX
#include "access/hash.h"
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

// 【在执行单行单数sql查询之后】从查询结果中获取数字，用于获取行数、列数、是否转置
// USAGE: int32 val = get_int32_from_qresult();
#define get_int32_from_qresult() atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))

// 【在执行单行单数sql查询之后】从查询结果中获取字符串，备用
// USAGE: char* get_string_from_qresult();
#define get_string_from_qresult() SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)

// 结果状态码：
// 0 -> 正常
// -1 -> 输入表不存在
// -2 -> 两表行数不同
// -3 -> 两表列数不同
// -4 -> 代表行数或列数的数字参数非正数
// -5 -> 新的行数和新的列数的乘积不等于原先元素个数


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
    
    ///////////////////////////////////////////////////////////////////////// 
    // 如果表不存在，报错并打印（用于直接粘贴和稍加修改的代码段）
    char sql_table_exists[MAX_SQL_LEN];
    sprintf(sql_table_exists, "select count(*) from pg_class where relname = '%s';", input_table_name);
    SPI_exec(sql_table_exists, 0);
    int32 if_input_table_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // 调用INNER函数: SELECT rows, cols, trans, inner_db4ai_abs(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT rows, cols, trans, inner_db4ai_abs(data) as data into %s from %s;",
        output_table_name, input_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_addP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_addP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_add(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接

    /////////////////////////////////////////////////////////////////////////
    // 如果表1不存在，报错并打印
    char sql_table1_exists[MAX_SQL_LEN];
    sprintf(sql_table1_exists, "select count(*) from pg_class where relname = '%s';", input_table_name1);
    SPI_exec(sql_table1_exists, 0);
    int32 if_input_table1_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table1_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 如果表2不存在，报错并打印
    char sql_table2_exists[MAX_SQL_LEN];
    sprintf(sql_table2_exists, "select count(*) from pg_class where relname = '%s';", input_table_name2);
    SPI_exec(sql_table2_exists, 0);
    int32 if_input_table2_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table2_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 确保表1和表2具有相同的行数
    // 获取表1的行数
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name1);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();

    // 获取表2的行数
    char sql_table2_rows[MAX_SQL_LEN];
    sprintf(sql_table2_rows, "select rows from %s;", input_table_name2);
    SPI_exec(sql_table2_rows, 0);
    int32 table2_rows = get_int32_from_qresult();

    // 如果不同则报错
    if(table1_rows!=table2_rows){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-2); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 确保表1和表2具有相同的列数
    // 获取表1的列数
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name1);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();

    // 获取表2的列数
    char sql_table2_cols[MAX_SQL_LEN];
    sprintf(sql_table2_cols, "select cols from %s;", input_table_name2);
    SPI_exec(sql_table2_cols, 0);
    int32 table2_cols = get_int32_from_qresult();

    // 如果不同则报错
    if(table1_cols!=table2_cols){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-3); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////


    // 调用INNER函数: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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
// 功能：将输入表相加的结果存放到输出表中。
// 参数：输入表名1 输入表名2 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_divP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_divP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_div(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接

    /////////////////////////////////////////////////////////////////////////
    // 如果表1不存在，报错并打印
    char sql_table1_exists[MAX_SQL_LEN];
    sprintf(sql_table1_exists, "select count(*) from pg_class where relname = '%s';", input_table_name1);
    SPI_exec(sql_table1_exists, 0);
    int32 if_input_table1_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table1_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 如果表2不存在，报错并打印
    char sql_table2_exists[MAX_SQL_LEN];
    sprintf(sql_table2_exists, "select count(*) from pg_class where relname = '%s';", input_table_name2);
    SPI_exec(sql_table2_exists, 0);
    int32 if_input_table2_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table2_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 确保表1和表2具有相同的行数
    // 获取表1的行数
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name1);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();

    // 获取表2的行数
    char sql_table2_rows[MAX_SQL_LEN];
    sprintf(sql_table2_rows, "select rows from %s;", input_table_name2);
    SPI_exec(sql_table2_rows, 0);
    int32 table2_rows = get_int32_from_qresult();

    // 如果不同则报错
    if(table1_rows!=table2_rows){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-2); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 确保表1和表2具有相同的列数
    // 获取表1的列数
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name1);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();

    // 获取表2的列数
    char sql_table2_cols[MAX_SQL_LEN];
    sprintf(sql_table2_cols, "select cols from %s;", input_table_name2);
    SPI_exec(sql_table2_cols, 0);
    int32 table2_cols = get_int32_from_qresult();

    // 如果不同则报错
    if(table1_cols!=table2_cols){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-3); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////


    // 调用INNER函数: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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
// 功能：将输入表按元素取e为底指数值的结果存放到输出表中。
// 参数：输入表名 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_expP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_expP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_exp(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // 启动连接
    SPI_connect();  //必须：建立连接
    
    ///////////////////////////////////////////////////////////////////////// 
    // 如果表不存在，报错并打印（用于直接粘贴和稍加修改的代码段）
    char sql_table_exists[MAX_SQL_LEN];
    sprintf(sql_table_exists, "select count(*) from pg_class where relname = '%s';", input_table_name);
    SPI_exec(sql_table_exists, 0);
    int32 if_input_table_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // 调用INNER函数: SELECT rows, cols, trans, inner_db4ai_exp(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT rows, cols, trans, inner_db4ai_exp(data) as data into %s from %s;",
        output_table_name, input_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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
// 功能：将输入表相乘的结果存放到输出表中。
// 参数：输入表名1 输入表名2 输出表名
// 返回：0正常 -1输入表不存在
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_mulP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_mulP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_mul(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接

    /////////////////////////////////////////////////////////////////////////
    // 如果表1不存在，报错并打印
    char sql_table1_exists[MAX_SQL_LEN];
    sprintf(sql_table1_exists, "select count(*) from pg_class where relname = '%s';", input_table_name1);
    SPI_exec(sql_table1_exists, 0);
    int32 if_input_table1_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table1_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 如果表2不存在，报错并打印
    char sql_table2_exists[MAX_SQL_LEN];
    sprintf(sql_table2_exists, "select count(*) from pg_class where relname = '%s';", input_table_name2);
    SPI_exec(sql_table2_exists, 0);
    int32 if_input_table2_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table2_exists){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-1); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 确保表1和表2具有相同的行数
    // 获取表1的行数
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name1);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();

    // 获取表2的行数
    char sql_table2_rows[MAX_SQL_LEN];
    sprintf(sql_table2_rows, "select rows from %s;", input_table_name2);
    SPI_exec(sql_table2_rows, 0);
    int32 table2_rows = get_int32_from_qresult();

    // 如果不同则报错
    if(table1_rows!=table2_rows){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-2); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 确保表1和表2具有相同的列数
    // 获取表1的列数
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name1);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();

    // 获取表2的列数
    char sql_table2_cols[MAX_SQL_LEN];
    sprintf(sql_table2_cols, "select cols from %s;", input_table_name2);
    SPI_exec(sql_table2_cols, 0);
    int32 table2_cols = get_int32_from_qresult();

    // 如果不同则报错
    if(table1_cols!=table2_cols){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-3); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////


    // 调用INNER函数: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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
// 返回：0正常 -4数字参数不正常
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z16outer_db4ai_onesP20FunctionCallInfoData); // 注册函数为V1版本
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
    
    /////////////////////////////////////////////////////////////////////////
    // 检测数字参数的情况（用于直接粘贴和稍加修改的代码段）
    if(rows<=0 || cols<=0){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-4); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // 调用INNER函数: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_ones(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_ones(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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
// 功能：将输入的矩阵表按照新的行和列“重组”，形成输出矩阵表，返回状态码
// 参数：输入表名 新行数 新列数 输出表名 （要求新的行数*新的列数等于原本的元素个数）
// 返回：0正常 -4参数不正常
/////////////////////////////////////////////////////////////////////////
// 在我们的存储方式中，重组只需要改变行和列数即可，对数据没有实际改动
// 函数注册
PG_FUNCTION_INFO_V1(_Z19outer_db4ai_reshapeP20FunctionCallInfoData); // 注册函数为V1版本
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_reshape(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int32 rows = PG_GETARG_INT32(1);
    int32 cols = PG_GETARG_INT32(2);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(3));
    // 启动连接
    SPI_connect();  //必须：建立连接
    
    /////////////////////////////////////////////////////////////////////////
    // 检测数字参数的情况——作为行数列数（用于直接粘贴和稍加修改的代码段）
    if(rows<=0 || cols<=0){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-4); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 检测数字参数的情况——作为整体数目（用于直接粘贴和稍加修改的代码段）
    // 获取输入表的行数
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();
    // 获取输入表的列数
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();
    // 查看是否有问题
    if((table1_rows*table1_cols)!=(rows*cols)){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-5); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // 调用INNER函数: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_reshape(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, data as data into %s from %s;",
        rows, cols, output_table_name, input_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
}
// 本算子无需内部函数


/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// 功能：将按照输入的数字，建立全0的输出表。
// 参数：行数 列数 输出表名
// 返回：0正常
/////////////////////////////////////////////////////////////////////////
// 函数注册
PG_FUNCTION_INFO_V1(_Z17outer_db4ai_zerosP20FunctionCallInfoData); // 注册函数为V1版本
PG_FUNCTION_INFO_V1(_Z17inner_db4ai_zerosP20FunctionCallInfoData);
// 外部函数
Datum // 所有Postgres函数的参数和返回类型都是Datum
outer_db4ai_zeros(PG_FUNCTION_ARGS){ // 函数名(参数) 必须加上
    // 获取参数
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // 启动连接
    SPI_connect();  //必须：建立连接
    
    /////////////////////////////////////////////////////////////////////////
    // 检测数字参数的情况（用于直接粘贴和稍加修改的代码段）
    if(rows<=0 || cols<=0){
        SPI_finish();   // 在分支的位置一定要及时关闭连接！
        PG_RETURN_INT32(-4); // 返回结果状态码
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // 清空输出表，不报错不打印（用于直接粘贴和稍加修改的代码段）
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // 调用INNER函数: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_zeros(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_zeros(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // 关闭连接返回正常结果（用于直接粘贴和稍加修改的代码段）
    SPI_finish();  // 必须：关闭连接
    PG_RETURN_INT32(0); // 返回结果状态码
    /////////////////////////////////////////////////////////////////////////
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