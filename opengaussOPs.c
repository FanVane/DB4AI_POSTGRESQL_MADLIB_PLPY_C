#include "postgres.h" // ������ÿ������postgres������C�ļ���

#include "fmgr.h" // ����PG_GETARG_XXX �Լ� PG_RETURN_XXX
#include "access/hash.h"
// #include "access/htup_details.h"
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
    ESPI_init_message(); // ���룺��ʼ��MSG���
    // ���������Ƿ���ڣ��������򱨴�(ע������˫����)
    ESPI_show_message(CHECKING IF INPUT_TABLE EXISTS...);
    
    // if(!ESPI_table_exists(input_table_name)){
    //     // �ַ��������������������������ӡ�
    //     char errmsg[MAX_SQL_LEN]; // ���������飬ע�ⲻ��char*����char[]
    //     strcpy(errmsg,"TABLE NOT EXIST: "); // �ٿ����ַ���
    //     strcat(errmsg, input_table_name); // �������ַ���
    //     ESPI_show_message(errmsg); // �����ʾ�ַ���
    //     // �ڷ�֧��λ��һ��Ҫ��ʱ�ر����ӣ�
    //     SPI_finish();  // ���룺�ر�����
    //     PG_RETURN_INT32(-1); // ���ؽ��״̬��
    // }

    // ��������
    // ESPI_drop_table_if_exists(output_table_name);
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
PG_FUNCTION_INFO_V1(_Z17outer_db4ai_zerosP20FunctionCallInfoData); // ע�ắ��ΪV1�汾
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