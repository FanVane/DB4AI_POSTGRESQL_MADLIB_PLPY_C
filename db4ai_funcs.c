#include "postgres.h" // ������ÿ������postgres������C�ļ���

#include "fmgr.h" // ����PG_GETARG_XXX �Լ� PG_RETURN_XXX
#include "access/hash.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h" // �����ṩArrayType*
#include "executor/spi.h" // ���ڵ���SPI

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

PG_MODULE_MAGIC; // ��PostgreSQL�ڼ���ʱ��������

// ����ִ�е��е���sql��ѯ֮�󡿴Ӳ�ѯ����л�ȡ���֣����ڻ�ȡ�������������Ƿ�ת��
// USAGE: int32 val = get_int32_from_qresult();
#define get_int32_from_qresult() atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))

// ����ִ�е��е���sql��ѯ֮�󡿴Ӳ�ѯ����л�ȡ�ַ���������
// USAGE: char* get_string_from_qresult();
#define get_string_from_qresult() SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)

// ���״̬�룺
// 0 -> ����
// -1 -> ���������
// -2 -> ����������ͬ
// -3 -> ����������ͬ
// -4 -> �������������������ֲ���������
// -5 -> �µ��������µ������ĳ˻�������ԭ��Ԫ�ظ���


/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ��������Ԫ��ȡ����ֵ�Ľ����ŵ�������С�
// ������������� �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_absP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_absP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_abs(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // ��������
    SPI_connect();  //���룺��������
    
    ///////////////////////////////////////////////////////////////////////// 
    // ��������ڣ�������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_table_exists[MAX_SQL_LEN];
    sprintf(sql_table_exists, "select count(*) from pg_class where relname = '%s';", input_table_name);
    SPI_exec(sql_table_exists, 0);
    int32 if_input_table_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // ����INNER����: SELECT rows, cols, trans, inner_db4ai_abs(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT rows, cols, trans, inner_db4ai_abs(data) as data into %s from %s;",
        output_table_name, input_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_abs(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8���飩
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr[i]<0?-arr[i]:arr[i]);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ����������ӵĽ����ŵ�������С�
// �������������1 �������2 �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_addP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_addP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_add(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������

    /////////////////////////////////////////////////////////////////////////
    // �����1�����ڣ�������ӡ
    char sql_table1_exists[MAX_SQL_LEN];
    sprintf(sql_table1_exists, "select count(*) from pg_class where relname = '%s';", input_table_name1);
    SPI_exec(sql_table1_exists, 0);
    int32 if_input_table1_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table1_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // �����2�����ڣ�������ӡ
    char sql_table2_exists[MAX_SQL_LEN];
    sprintf(sql_table2_exists, "select count(*) from pg_class where relname = '%s';", input_table_name2);
    SPI_exec(sql_table2_exists, 0);
    int32 if_input_table2_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table2_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ȷ����1�ͱ�2������ͬ������
    // ��ȡ��1������
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name1);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();

    // ��ȡ��2������
    char sql_table2_rows[MAX_SQL_LEN];
    sprintf(sql_table2_rows, "select rows from %s;", input_table_name2);
    SPI_exec(sql_table2_rows, 0);
    int32 table2_rows = get_int32_from_qresult();

    // �����ͬ�򱨴�
    if(table1_rows!=table2_rows){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-2); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ȷ����1�ͱ�2������ͬ������
    // ��ȡ��1������
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name1);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();

    // ��ȡ��2������
    char sql_table2_cols[MAX_SQL_LEN];
    sprintf(sql_table2_cols, "select cols from %s;", input_table_name2);
    SPI_exec(sql_table2_cols, 0);
    int32 table2_cols = get_int32_from_qresult();

    // �����ͬ�򱨴�
    if(table1_cols!=table2_cols){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-3); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////


    // ����INNER����: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_add(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8����X2��
    ArrayType* arr_raw1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr_raw2 = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr_raw1);
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr_raw2);
    int size = ARRNELEMS(arr_raw1);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]+arr2[i]);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ����������ӵĽ����ŵ�������С�
// �������������1 �������2 �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_divP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_divP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_div(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������

    /////////////////////////////////////////////////////////////////////////
    // �����1�����ڣ�������ӡ
    char sql_table1_exists[MAX_SQL_LEN];
    sprintf(sql_table1_exists, "select count(*) from pg_class where relname = '%s';", input_table_name1);
    SPI_exec(sql_table1_exists, 0);
    int32 if_input_table1_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table1_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // �����2�����ڣ�������ӡ
    char sql_table2_exists[MAX_SQL_LEN];
    sprintf(sql_table2_exists, "select count(*) from pg_class where relname = '%s';", input_table_name2);
    SPI_exec(sql_table2_exists, 0);
    int32 if_input_table2_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table2_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ȷ����1�ͱ�2������ͬ������
    // ��ȡ��1������
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name1);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();

    // ��ȡ��2������
    char sql_table2_rows[MAX_SQL_LEN];
    sprintf(sql_table2_rows, "select rows from %s;", input_table_name2);
    SPI_exec(sql_table2_rows, 0);
    int32 table2_rows = get_int32_from_qresult();

    // �����ͬ�򱨴�
    if(table1_rows!=table2_rows){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-2); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ȷ����1�ͱ�2������ͬ������
    // ��ȡ��1������
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name1);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();

    // ��ȡ��2������
    char sql_table2_cols[MAX_SQL_LEN];
    sprintf(sql_table2_cols, "select cols from %s;", input_table_name2);
    SPI_exec(sql_table2_cols, 0);
    int32 table2_cols = get_int32_from_qresult();

    // �����ͬ�򱨴�
    if(table1_cols!=table2_cols){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-3); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////


    // ����INNER����: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_div(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8����X2��
    ArrayType* arr_raw1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr_raw2 = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr_raw1);
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr_raw2);
    int size = ARRNELEMS(arr_raw1);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]/arr2[i]);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}






/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ��������Ԫ��ȡeΪ��ָ��ֵ�Ľ����ŵ�������С�
// ������������� �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_expP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_expP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_exp(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // ��������
    SPI_connect();  //���룺��������
    
    ///////////////////////////////////////////////////////////////////////// 
    // ��������ڣ�������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_table_exists[MAX_SQL_LEN];
    sprintf(sql_table_exists, "select count(*) from pg_class where relname = '%s';", input_table_name);
    SPI_exec(sql_table_exists, 0);
    int32 if_input_table_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // ����INNER����: SELECT rows, cols, trans, inner_db4ai_exp(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT rows, cols, trans, inner_db4ai_exp(data) as data into %s from %s;",
        output_table_name, input_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_exp(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8���飩
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(exp(arr[i]));
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}




/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ����������˵Ľ����ŵ�������С�
// �������������1 �������2 �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z15outer_db4ai_mulP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z15inner_db4ai_mulP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_mul(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������

    /////////////////////////////////////////////////////////////////////////
    // �����1�����ڣ�������ӡ
    char sql_table1_exists[MAX_SQL_LEN];
    sprintf(sql_table1_exists, "select count(*) from pg_class where relname = '%s';", input_table_name1);
    SPI_exec(sql_table1_exists, 0);
    int32 if_input_table1_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table1_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // �����2�����ڣ�������ӡ
    char sql_table2_exists[MAX_SQL_LEN];
    sprintf(sql_table2_exists, "select count(*) from pg_class where relname = '%s';", input_table_name2);
    SPI_exec(sql_table2_exists, 0);
    int32 if_input_table2_exists = get_int32_from_qresult()==0?0:1;
    if(!if_input_table2_exists){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ȷ����1�ͱ�2������ͬ������
    // ��ȡ��1������
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name1);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();

    // ��ȡ��2������
    char sql_table2_rows[MAX_SQL_LEN];
    sprintf(sql_table2_rows, "select rows from %s;", input_table_name2);
    SPI_exec(sql_table2_rows, 0);
    int32 table2_rows = get_int32_from_qresult();

    // �����ͬ�򱨴�
    if(table1_rows!=table2_rows){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-2); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ȷ����1�ͱ�2������ͬ������
    // ��ȡ��1������
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name1);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();

    // ��ȡ��2������
    char sql_table2_cols[MAX_SQL_LEN];
    sprintf(sql_table2_cols, "select cols from %s;", input_table_name2);
    SPI_exec(sql_table2_cols, 0);
    int32 table2_cols = get_int32_from_qresult();

    // �����ͬ�򱨴�
    if(table1_cols!=table2_cols){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-3); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////


    // ����INNER����: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_mul(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8����X2��
    ArrayType* arr_raw1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr_raw2 = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr_raw1);
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr_raw2);
    int size = ARRNELEMS(arr_raw1);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]*arr2[i]);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ���������������֣�����ȫ0�������
// ���������� ���� �������
// ���أ�0���� -4���ֲ���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z16outer_db4ai_onesP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z16inner_db4ai_onesP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_ones(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    
    /////////////////////////////////////////////////////////////////////////
    // ������ֲ��������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    if(rows<=0 || cols<=0){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-4); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // ����INNER����: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_ones(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_ones(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_ones(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8���飩
    int32 size = PG_GETARG_INT32(0);
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(1.0);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/////////////////////////////////////////////////////////////////////////
// ���ܣ�������ľ�������µ��к��С����顱���γ�������������״̬��
// ������������� ������ ������ ������� ��Ҫ���µ�����*�µ���������ԭ����Ԫ�ظ�����
// ���أ�0���� -4����������
/////////////////////////////////////////////////////////////////////////
// �����ǵĴ洢��ʽ�У�����ֻ��Ҫ�ı��к��������ɣ�������û��ʵ�ʸĶ�
// ����ע��
PG_FUNCTION_INFO_V1(_Z19outer_db4ai_reshapeP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_reshape(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int32 rows = PG_GETARG_INT32(1);
    int32 cols = PG_GETARG_INT32(2);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(3));
    // ��������
    SPI_connect();  //���룺��������
    
    /////////////////////////////////////////////////////////////////////////
    // ������ֲ��������������Ϊ��������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    if(rows<=0 || cols<=0){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-4); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ������ֲ��������������Ϊ������Ŀ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    // ��ȡ����������
    char sql_table1_rows[MAX_SQL_LEN];
    sprintf(sql_table1_rows, "select rows from %s;", input_table_name);
    SPI_exec(sql_table1_rows, 0);
    int32 table1_rows = get_int32_from_qresult();
    // ��ȡ����������
    char sql_table1_cols[MAX_SQL_LEN];
    sprintf(sql_table1_cols, "select cols from %s;", input_table_name);
    SPI_exec(sql_table1_cols, 0);
    int32 table1_cols = get_int32_from_qresult();
    // �鿴�Ƿ�������
    if((table1_rows*table1_cols)!=(rows*cols)){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-5); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // ����INNER����: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_reshape(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, data as data into %s from %s;",
        rows, cols, output_table_name, input_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �����������ڲ�����


/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ���������������֣�����ȫ0�������
// ���������� ���� �������
// ���أ�0����
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(_Z17outer_db4ai_zerosP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(_Z17inner_db4ai_zerosP20FunctionCallInfoData);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_zeros(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    
    /////////////////////////////////////////////////////////////////////////
    // ������ֲ��������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    if(rows<=0 || cols<=0){
        SPI_finish();   // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        PG_RETURN_INT32(-4); // ���ؽ��״̬��
    }
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // ����������������ӡ������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    char sql_drop_table_if_exists[MAX_SQL_LEN];
    sprintf(sql_drop_table_if_exists, "DROP TABLE IF EXISTS %s;", output_table_name);
    SPI_exec(sql_drop_table_if_exists, 0);
    /////////////////////////////////////////////////////////////////////////

    // ����INNER����: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_zeros(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_zeros(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);

    /////////////////////////////////////////////////////////////////////////
    // �ر����ӷ����������������ֱ��ճ�����Լ��޸ĵĴ���Σ�
    SPI_finish();  // ���룺�ر�����
    PG_RETURN_INT32(0); // ���ؽ��״̬��
    /////////////////////////////////////////////////////////////////////////
}
// �ڲ�����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
inner_db4ai_zeros(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ������һάdouble8���飩
    int32 size = PG_GETARG_INT32(0);
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��Ҫ�߼�����
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(0.0);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}