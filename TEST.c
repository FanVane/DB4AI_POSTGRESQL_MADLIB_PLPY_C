
/** 包含了postgres.h头文件，
 * 以我们对postgresql的理解，
 * 不难理解这里是直接调用了postgresql的内容。
 * */

// 简单的数学运算库
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

// 只要找到外部数据输入矩阵的方法和矩阵数据返回外部矩阵的方法即可

// 一个矩阵按照行数、列数、是否转置、分配、数据、缓存的结构体形式进行存储。
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
// 输入长宽，对一个矩阵初始化。依赖这个函数进行数据的读取等
void matrix_init(Matrix *matrix, int rows, int columns);

// initializes with a copy of a matrix
// 对输入的矩阵初始化
void matrix_init_clone(Matrix *matrix, const Matrix *src);

// initializes with a transposes a matrix, it is a virtual operation
// 对一个矩阵进行转置，这是个虚拟操作，盲猜是只改变了属性，在访问时通过这个属性进行相应的访问
void matrix_init_transpose(Matrix *matrix, const Matrix *src);

// releases the memory of a matrix and makes it emmpty
// 释放一个矩阵的空间
void matrix_release(Matrix *matrix);

// copies the shape and data from another matrix
// 成为另一个矩阵的复制品
void matrix_copy(Matrix *matrix, const Matrix *src);

// fills a matrix with zeroes
// 生成一个全零矩阵
void matrix_zeroes(Matrix *matrix);

// fills a matrix with ones
// 生成一个全一矩阵
void matrix_ones(Matrix *matrix);

// changes the shape of a matrix
// only number of rows will be done later
// 改变矩阵大小，之后看看怎么搞的
void matrix_resize(Matrix *matrix, int rows, int columns);

// multiplies a matrix by a vector, row by row
// 让矩阵与一个向量（单行矩阵）相乘
void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result);

// multiple two matrices by coefficients (a.k.a. Hadamard or Schur product)
// 两个矩阵做元素乘
void matrix_mult_entrywise(Matrix *m1, const Matrix *m2);

// multiplies a matrix by a scalar, coeeficient by coefficient
// 矩阵乘以标量
void matrix_mult_scalar(Matrix *matrix, gd_float factor);

// divides a matrix by a scalar, coeeficient by coefficient
// 矩阵除以标量
void matrix_divide(Matrix *matrix, gd_float factor);

// adds a matrix by another matrix, coeeficient by coefficient
// 矩阵加法
void matrix_add(Matrix *m1, const Matrix *m2);

// adds a matrix by the product of another matrix with a scalar, coefficient by coefficient
// A = A + B * c
void matrix_mult_scalar_add(Matrix *m1, const Matrix *m2, const gd_float factor);

// subtracts a matrix by another matrix, coeeficient by coefficient
// 矩阵减法
void matrix_subtract(Matrix *m1, const Matrix *m2);

// squares all coefficients
// 矩阵平方
void matrix_square(Matrix *matrix);

// obtains the square root of all coefficients
// 矩阵平方根
void matrix_square_root(Matrix *matrix);

// computes the sigmoid of all coefficients: 1.0 / (1.0 + exp(-c))
// 矩阵做sigmoid
void matrix_sigmoid(Matrix *matrix);

// computes the natural logarithm of all coefficients
// 矩阵求对数
void matrix_log(Matrix *matrix);

// computes the natural logarithm log(1+c) of all coefficients
// 矩阵求另一种对数
void matrix_log1p(Matrix *matrix);

// negates all coefficients (-c)
// 矩阵取负数
void matrix_negate(Matrix *matrix);

// 之后的也是类似，我就只注释不好理解的。

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

// 以下是对一些函数的实现，处于性能需求，实现为内联函数，我们来看看它是怎么实现的吧！
// ///////////////////////////////////////////////////////////////////////////
// inline

inline int matrix_expected_size(int rows, int columns)
{
    
    
    int cells = rows * columns;
    if (cells <= MATRIX_CACHE)
        cells = 0; // cached, no extra memory
    // 小到能够直接在结构体中缓存，就不再消耗外面的内存。

    return sizeof(Matrix) + cells * sizeof(gd_float);
    // 可以看到，返回的是按照行数列数计算的矩阵实际大小。
    // 每个矩阵都由gd_float存储，实际为double。
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
        // 初始化时全为0！
    } else
        matrix->data = (gd_float *)palloc0(matrix->allocated * sizeof(gd_float));
}

inline void matrix_init_clone(Matrix *matrix, const Matrix *src)
{
    
    
    
    // 先初始化
    matrix_init(matrix, src->rows, src->columns);
    size_t bytes = src->rows * src->columns * sizeof(gd_float);
    // 复制矩阵的内存位置，用errno确保成功
    errno_t rc = memcpy_s(matrix->data, matrix->allocated * sizeof(gd_float), src->data, bytes);
}

// fake transpose, only points to the data of the other matrix
inline void matrix_init_transpose(Matrix *matrix, const Matrix *src)
{
    
    
    
    matrix->transposed = true;
    matrix->rows = src->columns;
    matrix->columns = src->rows;
    // 为了实现矩阵转置，只要行变列，列变行即可，妙啊！（会不会出现浅拷贝相关的问题呢？）
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
    gd_float *pd = matrix->data; // 就是一个double的指针在一个二维数组上爬
    // 可以看到，这些操作还是很底层的，即内存空间的改变
    while (count-- > 0)
        *pd++ = 1.0;
}

// resize实现的是函数的列数不变的行数裁剪或加长
inline void matrix_resize(Matrix *matrix, int rows, int columns)
{
    
    
    
    

    if (rows != matrix->rows) {
        if (rows > matrix->rows) {
            int required = rows * matrix->columns;
        }
        matrix->rows = rows;
    }
}

// 之后的函数也是类似的道理，我只标注重要的部分
inline void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result)
{
    
    
    
    
    
    
    
    
    

    // 果然没猜错，按照矩阵是否是转置矩阵对操作进行分隔
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
// 可以看到，大部分的简单的矩阵操作都按内联函数实现了，如果要补充，找到类似的模板修改即可



int main(){
    // char* sql = ESQL_table_exists("a");
    // printf("SQL: %s", sql);
    char msg[1000];
    sprintf(msg, "Whether exists: %d", 666);
    printf("SQL: %s", msg);
}