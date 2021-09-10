#include "postgres.h" // ������ÿ������postgres������C�ļ���

#include "fmgr.h" // ����PG_GETARG_XXX �Լ� PG_RETURN_XXX
#include "access/hash.h"
#include "access/htup_details.h"
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
#define SQL_MAX_LEN 128 

int DEBUGGER;
/////////////////////////////////////////////////////////////////////////
// ������������
/////////////////////////////////////////////////////////////////////////
void ESPI_init_message(void);

char* ESQL_show_message(char* msg);
int ESPI_show_message(char* msg);

char* ESQL_table_rows(char* table_name);
int ESPI_table_rows(char* table_name);

char* ESQL_table_cols(char* table_name);
int ESPI_table_cols(char* table_name);

char* ESQL_table_trans(char* table_name);
int ESPI_table_trans(char* table_name);

char* ESQL_table_exists(char* table_name);
int ESPI_table_exists(char* table_name);


char* ESQL_drop_table_if_exists(char* table_name);
int ESPI_drop_table_if_exists(char* table_name);




/////////////////////////////////////////////////////////////////////////
//
// ESPI��չ������ʵ�ָ��๦�ܵ�SPI��
//
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ�����MSG�����ڵ��ԡ�
// ���أ�����ִ��״̬��
/////////////////////////////////////////////////////////////////////////
void ESPI_init_message(){
    ESPI_drop_table_if_exists("MSG");
    SPI_exec("CREATE TABLE MSG(message TEXT);", 0);
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ���ӡ��Ϣ��������
// ������msg ��ӡ����Ϣ
// ���أ�����ִ��״̬��
/////////////////////////////////////////////////////////////////////////
// SQL���ɺ���
char* ESQL_show_message(char* msg){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "INSERT INTO MSG VALUES ('");
    strcat(esql, msg);
    strcat(esql, "');");
    return esql;
}
// SQL������
int ESPI_show_message(char* msg){
    return SPI_exec(ESQL_show_message(msg), 0);;
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ����һ�����Ƿ���ڡ�
// ������table_name ���ı���
// ���أ������򷵻�1���������򷵻�0��
/////////////////////////////////////////////////////////////////////////
// SQL���ɺ���
char* ESQL_table_exists(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "select count(*) from pg_class where relname = '");
    strcat(esql, table_name);
    strcat(esql, "';");
    return esql;
}
// SQL������
int ESPI_table_exists(char* table_name){
    // �ڲ��Ͳ�Ҫ����connect��finish�����ˣ�������...
    SPI_exec(ESQL_table_exists(table_name), 0);
    // �����ַ�������ľ����ַ������
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    ESPI_show_message(val);
    return strcmp(val,"0")!=0;
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ���ȡһ�����������
// ������table_name ���ı���
// ���أ����������
/////////////////////////////////////////////////////////////////////////
// SQL���ɺ���
char* ESQL_table_rows(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "select rows from ");
    strcat(esql, table_name);
    strcat(esql, ";");
    return esql;
}
// SQL������
int ESPI_table_rows(char* table_name){
    // �ڲ��Ͳ�Ҫ����connect��finish�����ˣ�������...
    SPI_exec(ESQL_table_rows(table_name), 0);
    // �����ַ�������ľ����ַ������
    int32 val = atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
    return val;
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ���ȡһ�����������
// ������table_name ���ı���
// ���أ����������
/////////////////////////////////////////////////////////////////////////
// SQL���ɺ���
char* ESQL_table_cols(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "select cols from ");
    strcat(esql, table_name);
    strcat(esql, ";");
    return esql;
}
// SQL������
int ESPI_table_cols(char* table_name){
    // �ڲ��Ͳ�Ҫ����connect��finish�����ˣ�������...
    SPI_exec(ESQL_table_cols(table_name), 0);
    // �����ַ�������ľ����ַ������
    int32 val = atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
    return val;
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ���ȡһ�����ת�����
// ������table_name ���ı���
// ���أ�0δת�� 1ת�á�
/////////////////////////////////////////////////////////////////////////
// SQL���ɺ���
char* ESQL_table_trans(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "select trans from ");
    strcat(esql, table_name);
    strcat(esql, ";");
    return esql;
}
// SQL������
int ESPI_table_trans(char* table_name){
    // �ڲ��Ͳ�Ҫ����connect��finish�����ˣ�������...
    SPI_exec(ESQL_table_trans(table_name), 0);
    // �����ַ�������ľ����ַ������������atoi
    int32 val = atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
    return val;
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ�ɾ��һ����
// ������table_name ɾ���ı���
// ���أ����״̬��
/////////////////////////////////////////////////////////////////////////
// SQL���ɺ���
char* ESQL_drop_table_if_exists(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "DROP TABLE IF EXISTS ");
    strcat(esql, table_name);
    strcat(esql, ";");
    return esql;
}
// SQL������
int ESPI_drop_table_if_exists(char* table_name){
    return SPI_exec(ESQL_drop_table_if_exists(table_name), 0);
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ�����һ����ͷ�� ��δʵ�֣���Ҫʹ�ñ�������
// ������table_name ����
// ���أ���δʵ�֣���Ҫʹ�ñ�������
/////////////////////////////////////////////////////////////////////////
// CREATE TABLE <table_name>(rows INT, cols INT, trans INT, data float8[]);
char* ESQL_create_table_head(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "CREATE TABLE ");
    strcat(esql, table_name);
    strcat(esql, "(rows INT, cols INT, trans INT, data float8[]);");
    return esql;
}
/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ��������
// �ⲿ����Ϊ���ñ�����������һЩ
/////////////////////////////////////////////////////////////////////////
PG_MODULE_MAGIC; // ��PostgreSQL�ڼ���ʱ��������

Datum outer_db4ai_abs(PG_FUNCTION_ARGS);
Datum inner_db4ai_abs(PG_FUNCTION_ARGS);

Datum outer_db4ai_add(PG_FUNCTION_ARGS);
Datum inner_db4ai_add(PG_FUNCTION_ARGS);

Datum outer_db4ai_div(PG_FUNCTION_ARGS);
Datum inner_db4ai_div(PG_FUNCTION_ARGS);

Datum outer_db4ai_exp(PG_FUNCTION_ARGS);
Datum inner_db4ai_exp(PG_FUNCTION_ARGS);

Datum outer_db4ai_mul(PG_FUNCTION_ARGS);
Datum inner_db4ai_mul(PG_FUNCTION_ARGS);

Datum outer_db4ai_ones(PG_FUNCTION_ARGS);
Datum inner_db4ai_ones(PG_FUNCTION_ARGS);

Datum outer_db4ai_zeros(PG_FUNCTION_ARGS);
Datum inner_db4ai_zeros(PG_FUNCTION_ARGS);


/////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////
// ���ܣ��������Ԫ��ȡ����ֵ�Ľ����ŵ�������С�
// ������������� �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(outer_db4ai_abs); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_abs);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_abs(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ���������Ƿ���ڣ��������򱨴�
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name)){
        // �ַ��������������������������ӡ�
        char errmsg[MAX_SQL_LEN]; // ���������飬ע�ⲻ��char*����char[]
        strcpy(errmsg,"TABLE NOT EXIST: "); // �ٿ����ַ���
        strcat(errmsg, input_table_name); // �������ַ���
        ESPI_show_message(errmsg); // �����ʾ�ַ���
        // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        SPI_finish();  // ���룺�ر�����
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: SELECT rows, cols, trans, inner_db4ai_abs(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    strcpy(sql, "SELECT rows, cols, trans, inner_db4ai_abs(data) as data into ");
    strcat(sql, output_table_name);
    strcat(sql, " from ");
    strcat(sql, input_table_name);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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
PG_FUNCTION_INFO_V1(outer_db4ai_add); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_add);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_add(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ���������Ƿ���ڣ��������򱨴�
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name1) || !ESPI_table_exists(input_table_name2)){
        // �ַ��������������������������ӡ�
        char errmsg[MAX_SQL_LEN]; // ���������飬ע�ⲻ��char*����char[]
        strcpy(errmsg,"TABLE NOT EXIST! "); // �ٿ����ַ���
        ESPI_show_message(errmsg); // �����ʾ�ַ���
        // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        SPI_finish();  // ���룺�ر�����
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_add(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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
// ���ܣ������������Ľ����ŵ�������С�
// �������������1 / �������2 =  �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(outer_db4ai_div); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_div);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_div(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ���������Ƿ���ڣ��������򱨴�
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name1) || !ESPI_table_exists(input_table_name2)){
        // �ַ��������������������������ӡ�
        char errmsg[MAX_SQL_LEN]; // ���������飬ע�ⲻ��char*����char[]
        strcpy(errmsg,"TABLE NOT EXIST! "); // �ٿ����ַ���
        ESPI_show_message(errmsg); // �����ʾ�ַ���
        // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        SPI_finish();  // ���룺�ر�����
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_div(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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
// ���ܣ��������Ԫ��ȡ��eΪ�׵�ָ���Ľ����ŵ�������С�
// ������������� �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(outer_db4ai_exp); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_exp);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_exp(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ���������Ƿ���ڣ��������򱨴�
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name)){
        // �ַ��������������������������ӡ�
        char errmsg[MAX_SQL_LEN]; // ���������飬ע�ⲻ��char*����char[]
        strcpy(errmsg,"TABLE NOT EXIST: "); // �ٿ����ַ���
        strcat(errmsg, input_table_name); // �������ַ���
        ESPI_show_message(errmsg); // �����ʾ�ַ���
        // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        SPI_finish();  // ���룺�ر�����
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: SELECT rows, cols, trans, inner_db4ai_exp(data) as data into output_table_name from input_table_name;
    char sql[MAX_SQL_LEN];
    strcpy(sql, "SELECT rows, cols, trans, inner_db4ai_exp(data) as data into ");
    strcat(sql, output_table_name);
    strcat(sql, " from ");
    strcat(sql, input_table_name);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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
// ���ܣ����������X�Ľ����ŵ�������С�
// �������������1 �������2 �������
// ���أ�0���� -1���������
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(outer_db4ai_mul); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_mul);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_mul(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    char* input_table_name1 = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* input_table_name2 = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ���������Ƿ���ڣ��������򱨴�
    ESPI_show_message("CHECKING IF INPUT_TABLE EXISTS...");
    if(!ESPI_table_exists(input_table_name1) || !ESPI_table_exists(input_table_name2)){
        // �ַ��������������������������ӡ�
        char errmsg[MAX_SQL_LEN]; // ���������飬ע�ⲻ��char*����char[]
        strcpy(errmsg,"TABLE NOT EXIST! "); // �ٿ����ַ���
        ESPI_show_message(errmsg); // �����ʾ�ַ���
        // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
        SPI_finish();  // ���룺�ر�����
        PG_RETURN_INT32(-1); // ���ؽ��״̬��
    }
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into output_table_name from input_table_name1 as t1, input_table_name2 as t2;
    char sql[MAX_SQL_LEN];
    sprintf(sql, "SELECT t1.rows, t1.cols, t1.trans, inner_db4ai_mul(t1.data, t2.data) as data into %s from %s as t1, %s as t2;"
        , output_table_name, input_table_name1, input_table_name2);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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
// ���أ�0����
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(outer_db4ai_ones); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_ones);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_ones(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_ones(<rows> * <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_ones(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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
/////////////////////////////////////////////////////////////////////////
// ���ܣ���������������֣�����ȫ0�������
// ���������� ���� �������
// ���أ�0����
/////////////////////////////////////////////////////////////////////////
// ����ע��
PG_FUNCTION_INFO_V1(outer_db4ai_zeros); // ע�ắ��ΪV1�汾
PG_FUNCTION_INFO_V1(inner_db4ai_zeros);
// �ⲿ����
Datum // ����Postgres�����Ĳ����ͷ������Ͷ���Datum
outer_db4ai_zeros(PG_FUNCTION_ARGS){ // ������(����) �������
    // ��ȡ����
    int32 rows = PG_GETARG_INT32(0);
    int32 cols = PG_GETARG_INT32(1);
    char* output_table_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // ��������
    SPI_connect();  //���룺��������
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ��������
    ESPI_drop_table_if_exists(output_table_name);
    // ����INNER����: 
    // SELECT <rows> as rows, <cols> as cols, 0 as trans, inner_db4ai_zeros(<rows>, <cols>) as data into <output_table_name>;
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_zeros(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    SPI_exec(sql, 0);
    // �ر�����
    SPI_finish();  // ���룺�ر�����
    // ���ؽ��
    PG_RETURN_INT32(0); // ���ؽ��״̬��
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



/*
ע����һ�У���
if (SPI_processed > 0)
	{
		selected = DatumGetInt32(CStringGetDatum(SPI_getvalue(
													   SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc,
																		 1
																		)));
	}

*/