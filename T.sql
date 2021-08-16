--[基本功能测试]--
CREATE OR REPLACE FUNCTION
add_ab(int, int)
RETURNS INT
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','add_ab'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_abs(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求对数，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_abs(input_table_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name,output_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_abs("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_abs(float8[])
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_abs'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求对数，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_add('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名（被除数）
-- input_table2_name 输入的矩阵表2名（除数）
-- output_table_name 输出的矩阵表名  (商)
-- 返回 执行状态码
-- 效果 将输入的矩阵表按元素进行除法运算，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table1_name+".row as row,__db4ai_execute_row_div("+input_table1_name+".val,"+input_table2_name+".val) as val "+
" into "+output_table_name+\
" from "+input_table1_name+", "+input_table2_name+\
" where "+input_table1_name+".row = "+input_table2_name+".row;")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_div(float8[], float8[])
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_div'
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
db4ai_exp(input_table_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_exp("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_exp(float8[]) --传输一个数组，传出一个数组
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_exp'
LANGUAGE C STRICT;
-------------------------------------------------------------
-------------------------------------------------------------
-- db4ai_full(dim1 INT, dim2 INT, full_value float8,output_table_name TEXT)
-- dim1 行数
-- dim2 列数
-- full_value 填充值
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 创建一个dim1行dim2的名为output_table_name的全0矩阵表
-- 性能 经过测试，在和zeros比较时性能稍差，但差得不多
-------------------------------------------------------------
----[客户端接口]----
CREATE OR REPLACE FUNCTION
db4ai_full(dim1 INT, dim2 INT, full_value float8,output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保dim1,dim2非负数
if dim1<0 or dim2<0:
    # 数字参数为负
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("CREATE TABLE "+output_table_name+"(row int, val float8[]);")
# 插入表
for i in range(dim1):
    plpy.execute("INSERT INTO "+output_table_name+" SELECT "+str(i+1)+" as row, __db4ai_execute_row_full("+str(dim2)+","+str(full_value)+") as val;")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_full(int, float8) --传两个数字，出一个数组
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_full'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_log(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求对数，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_log(input_table_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_log("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_log(float8[])
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_log'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_matmul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表矩阵相乘，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_matmul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_mult('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_max(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表1名
-- dim 聚合最大值的维度 1为按列 2为按行
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表按选择的维度寻找最大值，形成索引表和数值表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_max(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保参数输入正确
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_max('"+input_table_name+"','row=row,col=val',"+str(dim)+",'"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_mean(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表1名
-- dim 聚合最小值的维度 1为按列 or 2为按行
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表按选择的维度计算均值，形成数值表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_mean(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保参数输入正确
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_mean('"+input_table_name+"','row=row,col=val',"+str(dim)+") as val into "+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_min(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表1名
-- dim 聚合最小值的维度 1为按列 2为按行
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表按选择的维度寻找最小值，形成索引表和数值表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_min(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保参数输入正确
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_min('"+input_table_name+"','row=row,col=val',"+str(dim)+",'"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表按照元素相乘，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_elem_mult('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_ones(dim1 INT, dim2 INT, output_table_name TEXT)
-- dim1 行数
-- dim2 列数
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 创建一个dim1行dim2的名为output_table_name的全1矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_ones(dim1 INT, dim2 INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保dim1,dim2非负数
if dim1<0 or dim2<0:
    # 数字参数为负
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("Select madlib.matrix_ones("+str(dim1)+" ,"+str(dim2)+" ,'"+output_table_name+"','fmt=dense');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_pow(input_table_name TEXT,pow_exp float8,output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- pow_exp 为每个元素进行指数运算的指数值
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求相应的指数，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_pow(input_table_name TEXT,pow_exp float8,output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_pow("+input_table_name+".val, "+str(pow_exp)+") as val " +\
" INTO "+output_table_name+\
" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_pow(float8[], float8) -- DONT'T FORGET TO CHANGE IT AFTER COPY.
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_pow'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sqrt(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求平方根，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_sqrt(input_table_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_sqrt("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_sqrt(float8[]) -- DONT'T FORGET TO CHANGE IT AFTER COPY.
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_sqrt'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sub(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名（被减数）
-- input_table2_name 输入的矩阵表2名（减数）
-- output_table_name 输出的矩阵表名 （差）
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都求对数，形成输出矩阵表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_sub(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_sub('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
-------------------------------------------------------------
-------------------------------------------------------------
-- db4ai_zeros(dim1 INT, dim2 INT, output_table_name TEXT)
-- dim1 行数
-- dim2 列数
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 创建一个dim1行dim2的名为output_table_name的全0矩阵表
-------------------------------------------------------------
----[客户端接口]----
CREATE OR REPLACE FUNCTION
db4ai_zeros(dim1 INT, dim2 INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保dim1,dim2非负数
if dim1<0 or dim2<0:
    # 数字参数为负
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("Select madlib.matrix_zeros("+str(dim1)+" ,"+str(dim2)+" ,'"+output_table_name+"','fmt=dense');")
return 0
$$ LANGUAGE plpythonu;
----------------------------------------------------------
----------------------------------------------------------