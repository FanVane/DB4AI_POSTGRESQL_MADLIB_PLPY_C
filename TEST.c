
/** ������postgres.hͷ�ļ���
 * �����Ƕ�postgresql����⣬
 * �������������ֱ�ӵ�����postgresql�����ݡ�
 * */

// �򵥵���ѧ�����
#include "math.h"
#include "float.h"
#include <stdio.h>
#define nullptr NULL
#define MATRIX_CACHE 16
#define bool int
#define false 0
#define true 1
typedef double gd_float;
#define palloc0 malloc
#define int32_t int

// ֻҪ�ҵ��ⲿ�����������ķ����;������ݷ����ⲿ����ķ�������

// һ���������������������Ƿ�ת�á����䡢���ݡ�����Ľṹ����ʽ���д洢��
typedef struct Matrix {
    int rows;
    int columns;
    bool transposed;
    int allocated;
    gd_float *data;
    gd_float cache[MATRIX_CACHE];
} Matrix;

int matrix_expected_size(int rows, int columns);

// initializes a bidimensional matrix or a vector (#columns = 1) to zeros
// ���볤����һ�������ʼ����������������������ݵĶ�ȡ��
void matrix_init(Matrix *matrix, int rows, int columns);

// initializes with a copy of a matrix
// ������ľ����ʼ��
void matrix_init_clone(Matrix *matrix, const Matrix *src);

// initializes with a transposes a matrix, it is a virtual operation
// ��һ���������ת�ã����Ǹ����������ä����ֻ�ı������ԣ��ڷ���ʱͨ��������Խ�����Ӧ�ķ���
void matrix_init_transpose(Matrix *matrix, const Matrix *src);

// releases the memory of a matrix and makes it emmpty
// �ͷ�һ������Ŀռ�
void matrix_release(Matrix *matrix);

// copies the shape and data from another matrix
// ��Ϊ��һ������ĸ���Ʒ
void matrix_copy(Matrix *matrix, const Matrix *src);

// fills a matrix with zeroes
// ����һ��ȫ�����
void matrix_zeroes(Matrix *matrix);

// fills a matrix with ones
// ����һ��ȫһ����
void matrix_ones(Matrix *matrix);

// changes the shape of a matrix
// only number of rows will be done later
// �ı�����С��֮�󿴿���ô���
void matrix_resize(Matrix *matrix, int rows, int columns);

// multiplies a matrix by a vector, row by row
// �þ�����һ�����������о������
void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result);

// multiple two matrices by coefficients (a.k.a. Hadamard or Schur product)
// ����������Ԫ�س�
void matrix_mult_entrywise(Matrix *m1, const Matrix *m2);

// multiplies a matrix by a scalar, coeeficient by coefficient
// ������Ա���
void matrix_mult_scalar(Matrix *matrix, gd_float factor);

// divides a matrix by a scalar, coeeficient by coefficient
// ������Ա���
void matrix_divide(Matrix *matrix, gd_float factor);

// adds a matrix by another matrix, coeeficient by coefficient
// ����ӷ�
void matrix_add(Matrix *m1, const Matrix *m2);

// adds a matrix by the product of another matrix with a scalar, coefficient by coefficient
// A = A + B * c
void matrix_mult_scalar_add(Matrix *m1, const Matrix *m2, const gd_float factor);

// subtracts a matrix by another matrix, coeeficient by coefficient
// �������
void matrix_subtract(Matrix *m1, const Matrix *m2);

// squares all coefficients
// ����ƽ��
void matrix_square(Matrix *matrix);

// obtains the square root of all coefficients
// ����ƽ����
void matrix_square_root(Matrix *matrix);

// computes the sigmoid of all coefficients: 1.0 / (1.0 + exp(-c))
// ������sigmoid
void matrix_sigmoid(Matrix *matrix);

// computes the natural logarithm of all coefficients
// ���������
void matrix_log(Matrix *matrix);

// computes the natural logarithm log(1+c) of all coefficients
// ��������һ�ֶ���
void matrix_log1p(Matrix *matrix);

// negates all coefficients (-c)
// ����ȡ����
void matrix_negate(Matrix *matrix);

// ֮���Ҳ�����ƣ��Ҿ�ֻע�Ͳ������ġ�

// complements all coefficients (1-c)
void matrix_complement(Matrix *matrix);

// make sure all coeeficients are c>=0
void matrix_positive(Matrix *matrix);

// return the sum of all coefficients
gd_float matrix_get_sum(const Matrix *matrix);

// scales a matrix row by row, using two vectors ( N & D)
// where each coefficient is scale c'=(c-N)/D
// - normalization: N=min, D=(max-min)
// - standardization: N=avg, D=stdev
void matrix_scale(Matrix *matrix, const Matrix *m_n, const Matrix *m_d);

// computes the dot product of two vectors
gd_float matrix_dot(const Matrix *v1, const Matrix *v2);

// converts all coefficients to binary values w.r.t. a threshold
// low: v<threshold; high: v>=threshold
void matrix_binary(Matrix *matrix, gd_float threshold, gd_float low, gd_float high);

// prints into the log
void elog_matrix(int elevel, const char *msg, const Matrix *matrix);

void matrix_gram_schmidt(Matrix *matrix, int32_t const num_vectors);

// �����Ƕ�һЩ������ʵ�֣�������������ʵ��Ϊ��������������������������ôʵ�ֵİɣ�
// ///////////////////////////////////////////////////////////////////////////
// inline

inline int matrix_expected_size(int rows, int columns)
{
    
    
    int cells = rows * columns;
    if (cells <= MATRIX_CACHE)
        cells = 0; // cached, no extra memory
    // С���ܹ�ֱ���ڽṹ���л��棬�Ͳ�������������ڴ档

    return sizeof(Matrix) + cells * sizeof(gd_float);
    // ���Կ��������ص��ǰ���������������ľ���ʵ�ʴ�С��
    // ÿ��������gd_float�洢��ʵ��Ϊdouble��
}


inline void matrix_init(Matrix *matrix, int rows, int columns)
{
    
    
    
    matrix->transposed = false;
    matrix->rows = rows;
    matrix->columns = columns;
    matrix->allocated = rows * columns;
    if (matrix->allocated <= MATRIX_CACHE) {
        matrix->data = matrix->cache;
        errno_t rc = memset_s(matrix->data, MATRIX_CACHE * sizeof(gd_float), 0, matrix->allocated * sizeof(gd_float));
        // ��ʼ��ʱȫΪ0��
    } else
        matrix->data = (gd_float *)palloc0(matrix->allocated * sizeof(gd_float));
}

inline void matrix_init_clone(Matrix *matrix, const Matrix *src)
{
    
    
    
    // �ȳ�ʼ��
    matrix_init(matrix, src->rows, src->columns);
    size_t bytes = src->rows * src->columns * sizeof(gd_float);
    // ���ƾ�����ڴ�λ�ã���errnoȷ���ɹ�
    errno_t rc = memcpy_s(matrix->data, matrix->allocated * sizeof(gd_float), src->data, bytes);
}

// fake transpose, only points to the data of the other matrix
inline void matrix_init_transpose(Matrix *matrix, const Matrix *src)
{
    
    
    
    matrix->transposed = true;
    matrix->rows = src->columns;
    matrix->columns = src->rows;
    // Ϊ��ʵ�־���ת�ã�ֻҪ�б��У��б��м��ɣ�������᲻�����ǳ������ص������أ���
    matrix->allocated = 0;
    matrix->data = src->data;
}

inline void matrix_release(Matrix *matrix)
{
    
    if (matrix->allocated > 0) {
        
        if (matrix->data != matrix->cache)
            pfree(matrix->data);

        matrix->allocated = 0;
    }
    matrix->data = nullptr;
    matrix->rows = 0;
    matrix->columns = 0;
}

inline void matrix_copy(Matrix *matrix, const Matrix *src)
{
    
    
    
    
    int count = src->rows * src->columns;
    if (count > matrix->allocated) {
        // resize
        matrix->allocated = count;
        if (matrix->allocated > MATRIX_CACHE) {
            // realloc
            if (matrix->data != matrix->cache)
                pfree(matrix->data);

            matrix->data = (gd_float *)palloc0(matrix->allocated * sizeof(gd_float));
        }
    }
    
    matrix->rows = src->rows;
    matrix->columns = src->columns;
    errno_t rc = memcpy_s(matrix->data, matrix->allocated * sizeof(gd_float),
                            src->data, sizeof(gd_float) * count);
}

inline void matrix_zeroes(Matrix *matrix)
{
    
    
    errno_t rc = memset_s(matrix->data, sizeof(gd_float) * matrix->allocated, 0,
        sizeof(gd_float) * matrix->rows * matrix->columns);
}

inline void matrix_ones(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data; // ����һ��double��ָ����һ����ά��������
    // ���Կ�������Щ�������Ǻܵײ�ģ����ڴ�ռ�ĸı�
    while (count-- > 0)
        *pd++ = 1.0;
}

// resizeʵ�ֵ��Ǻ�������������������ü���ӳ�
inline void matrix_resize(Matrix *matrix, int rows, int columns)
{
    
    
    
    

    if (rows != matrix->rows) {
        if (rows > matrix->rows) {
            int required = rows * matrix->columns;
        }
        matrix->rows = rows;
    }
}

// ֮��ĺ���Ҳ�����Ƶĵ�����ֻ��ע��Ҫ�Ĳ���
inline void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result)
{
    
    
    
    
    
    
    
    
    

    // ��Ȼû�´����վ����Ƿ���ת�þ���Բ������зָ�
    if (matrix->transposed) {
        gd_float *pd = result->data;
        // loop assumes that the data has not been physically transposed
        for (int r = 0; r < matrix->rows; r++) {
            const gd_float *pm = matrix->data + r;
            const gd_float *pv = vector->data;
            gd_float x = 0.0;
            for (int c = 0; c < matrix->columns; c++) {
                x += *pm * *pv++;
                pm += matrix->rows;
            }
            *pd++ = x;
        }
    } else {
        const gd_float *pm = matrix->data;
        gd_float *pd = result->data;
        for (int r = 0; r < matrix->rows; r++) {
            const gd_float *pv = vector->data;
            size_t count = matrix->columns;
            gd_float x = 0.0;
            while (count-- > 0)
                x += *pv++ * *pm++;
            *pd++ = x;
        }
    }
}

inline void matrix_mult_entrywise(Matrix *m1, const Matrix *m2)
{
    
    
    
    
    
    

    size_t count = m1->rows * m1->columns;
    gd_float *pd = m1->data;
    const gd_float *ps = m2->data;
    while (count-- > 0)
        *pd++ *= *ps++;
}

inline void matrix_mult_scalar(Matrix *matrix, gd_float factor)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0)
        *pd++ *= factor;
}

inline void matrix_divide(Matrix *matrix, gd_float factor)
{
    
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0)
        *pd++ /= factor;
}

inline void matrix_add(Matrix *m1, const Matrix *m2)
{
    
    
    
    
    
    
    size_t count = m1->rows * m1->columns;
    gd_float *p1 = m1->data;
    const gd_float *p2 = m2->data;
    while (count-- > 0)
        *p1++ += *p2++;
}

inline void matrix_mult_scalar_add(Matrix *m1, const Matrix *m2, const gd_float factor)
{
    
    
    
    
    
    
    size_t count = m1->rows * m1->columns;
    gd_float *p1 = m1->data;
    const gd_float *p2 = m2->data;
    while (count-- > 0)
        *p1++ += factor * *p2++;
}

inline void matrix_subtract(Matrix *m1, const Matrix *m2)
{
    
    
    
    
    
    
    size_t count = m1->rows * m1->columns;
    gd_float *p1 = m1->data;
    const gd_float *p2 = m2->data;
    while (count-- > 0)
        *p1++ -= *p2++;
}

inline gd_float matrix_dot(const Matrix *v1, const Matrix *v2)
{
    
    
    
    
    
    
    

    size_t count = v1->rows;
    const gd_float *p1 = v1->data;
    const gd_float *p2 = v2->data;
    gd_float result = 0;
    while (count-- > 0)
        result += *p1++ * *p2++;

    return result;
}

inline void matrix_square(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        *pd *= *pd;
        pd++;
    }
}

inline void matrix_square_root(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        *pd = sqrt(*pd);
        pd++;
    }
}

inline void matrix_sigmoid(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float c = *pd;
        *pd++ = 1.0 / (1.0 + exp(-c));
    }
}

inline void matrix_log(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        *pd++ = log(v);
    }
}

inline void matrix_log1p(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd + 1;
        *pd++ = log(v);
    }
}

inline void matrix_negate(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        *pd++ = -v;
    }
}

inline void matrix_complement(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = 1.0 - *pd;
        *pd++ = v;
    }
}

inline void matrix_positive(Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        if (v < 0.0)
            *pd = 0.0;
        pd++;
    }
}

inline gd_float matrix_get_sum(const Matrix *matrix)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *ps = matrix->data;
    gd_float s = 0.0;
    while (count-- > 0)
        s += *ps++;
    return s;
}

inline void matrix_scale(Matrix *matrix, const Matrix *m_n, const Matrix *m_d)
{
    
    
    
    
    
    
    
    
    
    
    gd_float *pd = matrix->data;
    for (int r = 0; r < matrix->rows; r++) {
        const gd_float *p1 = m_n->data;
        const gd_float *p2 = m_d->data;
        for (int c = 0; c < matrix->columns; c++) {
            *pd = (*pd - *p1++) / *p2++;
            pd++;
        }
    }
}

inline void matrix_binary(Matrix *matrix, gd_float threshold, gd_float low, gd_float high)
{
    
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        *pd++ = (v < threshold ? low : high);
    }
}



inline void matrix_gram_schmidt(Matrix *matrix, int32_t const num_vectors)
{
    int32_t const dimension = matrix->rows;
    Matrix done_vector;
    Matrix current_vector;
    done_vector.rows = current_vector.rows = dimension;
    done_vector.columns = current_vector.columns = 1;
    done_vector.allocated = current_vector.allocated = dimension;
    done_vector.transposed = current_vector.transposed = false;
    gd_float projection = 0.;
    gd_float squared_magnitude = 0.;
    gd_float magnitude = 0.;
    int32_t first_non_zero = -1;
    
    /*
     * we first find the very first eigenvector with non-zero magnitude
     * and normalize it (the order of the test in the loop matters)
     */
    while ((magnitude == 0.) && (++first_non_zero < num_vectors)) {
        current_vector.data = matrix->data + (first_non_zero * dimension);
        squared_magnitude = matrix_dot(&current_vector, &current_vector);
        if (squared_magnitude == 0.)
            continue;
        magnitude = sqrt(squared_magnitude);
        matrix_mult_scalar(&current_vector, 1. / magnitude);
    }
    
    /*
     * no valid vector found :(
     */
    
    /*
     * we can indeed orthonormalized at least one vector, let's go
     */
    for (int32_t cv = first_non_zero + 1; cv < num_vectors; ++cv) {
        current_vector.data = matrix->data + (cv * dimension);
        // we go thru previously orthonormalized vectors to produce the next one
        for (int32_t dv = first_non_zero; dv < cv; ++dv) {
            done_vector.data = matrix->data + (dv * dimension);
            
            projection = matrix_dot(&current_vector, &done_vector);
            
            /*
             * no shadow is cast, and thus the vectors are perpendicular
             * and we can continue with the next vector in the upper loop
             */
            if (projection == 0.) {
                dv = cv;
                continue;
            }
            
            matrix_mult_scalar(&done_vector, projection);
            matrix_subtract(&current_vector, &done_vector);
            /*
             * for the time being we are using no extra space and thus we have
             * to normalize again a previously-ready vector
             */
            squared_magnitude = matrix_dot(&done_vector, &done_vector);
            magnitude = sqrt(squared_magnitude);
            matrix_mult_scalar(&done_vector, 1. / magnitude);
        }
        squared_magnitude = matrix_dot(&current_vector, &current_vector);
        magnitude = sqrt(squared_magnitude);
        matrix_mult_scalar(&current_vector, 1. / magnitude);
    }
}
// ���Կ������󲿷ֵļ򵥵ľ������������������ʵ���ˣ����Ҫ���䣬�ҵ����Ƶ�ģ���޸ļ���



int main(){
    // char* sql = ESQL_table_exists("a");
    // printf("SQL: %s", sql);
    char msg[1000];
    sprintf(msg, "Whether exists: %d", 666);
    printf("SQL: %s", msg);
}