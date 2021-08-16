#include "postgres.h" // ������ÿ������postgres������C�ļ���

#include "fmgr.h" // ����PG_GETARG_XXX �Լ� PG_RETURN_XXX
#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h" // �����ṩArrayType*

#include <stdlib.h>
#include <math.h>
/*
 * Taken from the intarray contrib header
 */
#define ARRPTR(x)  ( (double *) ARR_DATA_PTR(x) )
#define ARRNELEMS(x)  ArrayGetNItems( ARR_NDIM(x), ARR_DIMS(x))


// ��PostgreSQL�ڼ���ʱ��������
PG_MODULE_MAGIC;

/**
 * �������ܲ��ԣ�������������������
 *  @param arg1 ��һ������
 *  @param arg2 �ڶ�������
 *  @author none
 */
PG_FUNCTION_INFO_V1(add_ab); // ˵��ʹ�õ�ģʽ��V1
Datum // ����Postgres�����ķ������Ͷ���Datum
add_ab(PG_FUNCTION_ARGS){ // ������(����) �������
    int32 arg_a = PG_GETARG_INT32(0); // Ϊ������ȡֵ
    int32 arg_b = PG_GETARG_INT32(1);
    PG_RETURN_INT32(arg_a + arg_b); // ���غ���ֵ
}


/**
 * ����һ��float8���飬��ÿ��Ԫ�������ֵ֮�󷵻����顣
 * @param arr_raw �����float8����
 * @return �����ֵ֮�������
 * @author db4ai_abs
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_abs);
Datum
__db4ai_execute_row_abs(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
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


/**
 * ����һ��float8���飬��ÿ��Ԫ�������֮�󷵻����顣
 * @param arr_raw �����float8����
 * @return �����֮�������
 * @author db4ai_log
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_log);
Datum
__db4ai_execute_row_log(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(log(arr[i]));
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}




/**
 * ����һ��float8���飬��ÿ��Ԫ����ƽ����֮�󷵻����顣
 * @param arr_raw �����float8����
 * @return ��ƽ����֮�������
 * @author db4ai_sqrt
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_sqrt);
Datum
__db4ai_execute_row_sqrt(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(sqrt(arr[i]));
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

