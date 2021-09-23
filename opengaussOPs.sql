------------------------------------------------------------
------------------------------------------------------------
-- db4ai_abs(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求绝对值，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_abs(input_table_name TEXT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z15outer_db4ai_absP20FunctionCallInfoData' -- 前者手动改为绝对路径 后者手动改成.so中的符号名
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_abs(float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_absP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求和，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER
AS 'db4ai_funcs','_Z15outer_db4ai_addP20FunctionCallInfoData'
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_add(float8[], float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_addP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求除，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER
AS 'db4ai_funcs','_Z15outer_db4ai_divP20FunctionCallInfoData'
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_div(float8[], float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_divP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_exp(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求e为底的指数，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_exp(input_table_name TEXT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z15outer_db4ai_expP20FunctionCallInfoData' -- 前者手动改为绝对路径 后者手动改成.so中的符号名
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_exp(float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_expP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求乘积，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER
AS 'db4ai_funcs','_Z15outer_db4ai_mulP20FunctionCallInfoData'
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_mul(float8[], float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_mulP20FunctionCallInfoData'
LANGUAGE C STRICT;

-- CREATE TABLE a(rows INT ,cols INT, trans INT, data float8[]);
-- INSERT INTO a values(2, 2, 0, '{1.0,2.0,3.0,-4.0}');

------------------------------------------------------------
------------------------------------------------------------
-- db4ai_ones(rows INT, cols INT, output_table_name TEXT)
-- rows cols 矩阵的行数 列数
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 按照参数的规格生成一个全0的矩阵
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_ones(rows INT, cols INT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z16outer_db4ai_onesP20FunctionCallInfoData' -- 前者手动改为绝对路径 后者手动改成.so中的符号名
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_ones(INT)
RETURNS float8[]
AS 'db4ai_funcs','_Z16inner_db4ai_onesP20FunctionCallInfoData'
LANGUAGE C STRICT;

------------------------------------------------------------
------------------------------------------------------------
-- db4ai_reshape(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim1 新的行数
-- dim2 新的列数 要求dim1*dim2 = old_dim1 * old_dim2 
-- output_table_name 输出的向量表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表重组，形成输出矩阵表，返回状态码
-- 测试 select db4ai_reshape('ones', 2, 6, 'reshape');
------------------------------------------------------------
--[客户端接口]--
--[执行函数]--
CREATE OR REPLACE FUNCTION
db4ai_reshape(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z19outer_db4ai_reshapeP20FunctionCallInfoData'
LANGUAGE C STRICT;



------------------------------------------------------------
------------------------------------------------------------
-- db4ai_zeros(rows INT, cols INT, output_table_name TEXT)
-- rows cols 矩阵的行数 列数
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 按照参数的规格生成一个全0的矩阵
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_zeros(rows INT, cols INT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z17outer_db4ai_zerosP20FunctionCallInfoData' -- 前者手动改为绝对路径 后者手动改成.so中的符号名
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
inner_db4ai_zeros(INT)
RETURNS float8[]
AS 'db4ai_funcs','_Z17inner_db4ai_zerosP20FunctionCallInfoData'
LANGUAGE C STRICT;