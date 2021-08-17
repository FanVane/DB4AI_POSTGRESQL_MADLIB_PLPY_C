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

/**
 * ����һ��float8���飬��ÿ��Ԫ����eΪ�׵�ָ��֮�󷵻����顣
 * @param arr_raw �����float8����
 * @return ��eΪ�׵�ָ��֮�������
 * @author db4ai_exp
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_exp);
Datum
__db4ai_execute_row_exp(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(exp(arr[i]));
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * ����һ��float8���飬��ÿ��Ԫ��������֮�󷵻����顣
 * @param arr1_raw �����float8���鱻����
 * @param arr2_raw �����float8�������
 * @return �����֮�������
 * @author db4ai_div
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_div);
Datum
__db4ai_execute_row_div(PG_FUNCTION_ARGS){
    ArrayType* arr1_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    ArrayType* arr2_raw = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr1_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr2_raw);
    int size = ARRNELEMS(arr1_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr1[i]/arr2[i]);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * ����һ��float8���飬��ÿ��Ԫ��Ϊ�ף�������Ԫ�ض�Ӧ��ָ��֮�󷵻����顣
 * @param arr_raw �����float8����
 * @param pow_exp ����ָ����ֵ
 * @return ָ������֮�������
 * @author db4ai_pow
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_pow);
Datum
__db4ai_execute_row_pow(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8 pow_exp = PG_GETARG_FLOAT8(1);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(pow(arr[i], pow_exp));
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * ���볤�Ⱥ���ֵ��������ô����һ��������顣
 * @param size ��Ҫ�����float8���鳤��
 * @param full_value ��������ֵ
 * @return ָ������֮�������
 * @author db4ai_pow db4ai_trace
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_full);
Datum
__db4ai_execute_row_full(PG_FUNCTION_ARGS){
    int32 size = PG_GETARG_INT32(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8 full_value = PG_GETARG_FLOAT8(1);
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(full_value);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * ����float8���飬��������Ԫ���ܺ͡�
 * @param arr_raw ���������float8����
 * @return �������Ԫ��֮��
 * @author db4ai_trace
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_sum);
Datum
__db4ai_execute_row_sum(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8 sum = 0.0;
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    for(int i=0; i<size; i++){
        sum += arr[i];
    }
    PG_RETURN_FLOAT8(sum);
}

/**
 * ����2��float8���飬����֮�󷵻�����
 * @param arr1_raw �����float8����1
 * @param arr2_raw �����float8����2
 * @return ����֮�����
 * @author db4ai_div
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_dot);
Datum
__db4ai_execute_row_dot(PG_FUNCTION_ARGS){
    ArrayType* arr1_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    ArrayType* arr2_raw = PG_GETARG_ARRAYTYPE_P(1);
    float8* arr1 = (float8 *) ARR_DATA_PTR(arr1_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    float8* arr2 = (float8 *) ARR_DATA_PTR(arr2_raw);
    int size = ARRNELEMS(arr1_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    float8 dot = 0.0;
    for(int i=0; i<size; i++){
        dot += arr1[i] * arr2[i];
    }
    // ���ظ�����
    PG_RETURN_FLOAT8(dot);
}

/**
 * ����1��float8���飬��������֮�󷵻�float8���飨��C���滹�Ǵ�float8�ɣ���
 * @param arr_raw �����float8����
 * @return ������������
 * @author db4ai_argsort
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_argsort);
Datum
__db4ai_execute_row_argsort(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    // ��������ά��һ��ͬ����С��mask���飬ɨ��n�Σ�ÿ���ҵ���Сֵ������������ӵ�����������
    int* mask = (int*) malloc(size * sizeof(int));
    for(int i=0; i<size; i++)   mask[i] = 0;
    int min_cur = -1;
    for(int i=0; i<size; i++){
        float8 min_val = __FLT64_MAX__;
        // Ѱ����С����Щ����
        for (int j = 0; j < size; j++)
        {
            if(mask[j]!=0) continue;
            if(arr[j]<min_val){
                min_cur = j;
                min_val = arr[j];
            }
        }
        // ����С������������mask�ϲ����뷵��������
        mask[min_cur] = 1;
        ans_arr_back[i] = Float8GetDatum((float8)min_cur+1);
    }
    free(mask);
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}


/**
 * ����һ��float8���飬��ÿ��Ԫ����softmax֮�󷵻����顣
 * @param arr_raw �����float8����
 * @return ��softmax֮�������
 * @author db4ai_softmax
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_softmax);
Datum
__db4ai_execute_row_softmax(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    float8 bottum = 0.0;
    for(int i=0; i<size; i++){
        bottum += exp(arr[i]);
    }
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(exp(arr[i])/bottum);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * ����1��float8���飬����֮�󷵻�float8���顣
 * @param arr_raw �����float8����
 * @return ����������
 * @author db4ai_args
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_sort);
Datum
__db4ai_execute_row_sort(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    float8* ans_arr = (float8*) malloc(size * sizeof(float8));
    // �ȸ�����˵
    for(int i=0; i<size; i++) ans_arr[i] = arr[i];
    // ð������
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
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * ����λ�������Ϣ��������µ��кš�
 * @param row Ŀǰ����
 * @param col Ŀǰ����
 * @param old_dim2 ԭ������
 * @param dim2 Ŀ������
 * @return �µ��к�
 * @author db4ai_reshape
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_reshape); // ˵��ʹ�õ�ģʽ��V1
Datum // ����Postgres�����ķ������Ͷ���Datum
__db4ai_execute_row_reshape(PG_FUNCTION_ARGS){ // ������(����) �������
    int32 row = PG_GETARG_INT32(0)-1; // ע�⣺-1��Ϊ�˱��ڼ��㣬֮��Ľ����+1
    int32 col = PG_GETARG_INT32(1)-1;
    int32 old_dim2 = PG_GETARG_INT32(2);
    int32 dim2 = PG_GETARG_INT32(3);
    int32 rank = row * old_dim2 + col;
    int32 new_row = rank/dim2+1;
    PG_RETURN_INT32(new_row); // ���غ���ֵ
}

/**
 * ����λ�������Ϣ��������µ��кš�
 * @param row Ŀǰ����
 * @param col Ŀǰ����
 * @param old_dim2 ԭ������
 * @param dim2 Ŀ������
 * @return �µ��к�
 * @author db4ai_reshape
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_col_reshape); // ˵��ʹ�õ�ģʽ��V1
Datum // ����Postgres�����ķ������Ͷ���Datum
__db4ai_execute_col_reshape(PG_FUNCTION_ARGS){ // ������(����) �������
    int32 row = PG_GETARG_INT32(0)-1; // ע�⣺-1��Ϊ�˱��ڼ��㣬֮��Ľ����+1
    int32 col = PG_GETARG_INT32(1)-1;
    int32 old_dim2 = PG_GETARG_INT32(2);
    int32 dim2 = PG_GETARG_INT32(3);
    int32 rank = row * old_dim2 + col;
    int32 new_col = rank - ((rank/dim2)*dim2) +1;
    PG_RETURN_INT32(new_col); // ���غ���ֵ
}

/**
 * ����1��float8�����һ�����֣�����֮�󷵻�float8���顣
 * @param arr_raw �����float8����
 * @param repeat �ظ��Ĵ���
 * @return �����������
 * @author db4ai_repeat
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_repeat);
Datum
__db4ai_execute_row_repeat(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    int32 repeat = PG_GETARG_INT32(1);
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(repeat * size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        for(int j=0; j<repeat; j++){
            ans_arr_back[j * size+i] = Float8GetDatum(arr[i]);
        }
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, repeat * size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}

/**
 * ����1��float8�����һ�����֣���ת֮�󷵻�float8���顣
 * @param arr_raw �����float8����
 * @return ��ת�Ľ��
 * @author db4ai_reverse
 */ 
PG_FUNCTION_INFO_V1(__db4ai_execute_row_flip);
Datum
__db4ai_execute_row_flip(PG_FUNCTION_ARGS){
    ArrayType* arr_raw = PG_GETARG_ARRAYTYPE_P(0);          // ��PG_GETARG_ARRAYTYPE_P(_COPY) ��ȡ�������͵�ָ��
    float8* arr = (float8 *) ARR_DATA_PTR(arr_raw);         // ��ARR_DATA_PTR��ȡʵ�ʵ�ָ�루���������ͷ��
    int size = ARRNELEMS(arr_raw);                          // ��ARRNELEMS��Դ�����л�ȡ�����Ԫ�ظ���
    // ����һ��Datum����
    Datum* ans_arr_back = (Datum*) palloc(size * sizeof(Datum));    // ��palloc��̬�����ڴ�
    for(int i=0; i<size; i++){
        ans_arr_back[i] = Float8GetDatum(arr[size-1-i]);
    }
    // ���ظ�����
    ArrayType* result = construct_array(ans_arr_back, size, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(result);
}