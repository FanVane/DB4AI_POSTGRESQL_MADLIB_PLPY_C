--[基本功能测试]--
CREATE OR REPLACE FUNCTION
add_ab(int, int)
RETURNS INT
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','add_ab'
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_abs'
LANGUAGE C STRICT;

------------------------------------------------------------
------------------------------------------------------------
-- db4ai_acc(input_table1_name TEXT, input_table2_name TEXT, option TEXT,output_table_name TEXT)
-- input_table1_name 输入的矩阵（向量）表名
-- input_table2_name 输入的矩阵（向量）表名
-- norp 'N' or 'P' : 数量 or 比例
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的向量求匹配形成输出表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_acc(input_table1_name TEXT, input_table2_name TEXT, norp TEXT,output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (norp=="N" or norp=="P" or norp=="n" or norp=="p"):
    # 字母参数不当
    return -4
# 确保两个向量的长度相同（暂未实装）
# 建立输出表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
if norp=="N" or norp=="n":
    # 调用执行函数
    plpy.execute("SELECT "+input_table1_name+".row as row, __db4ai_execute_row_full(1, __db4ai_execute_row_acc("+input_table1_name+".val,"+input_table2_name+".val)) as val "+
    " into "+output_table_name+\
    " from "+input_table1_name+", "+input_table2_name+\
    " where "+input_table1_name+".row = "+input_table2_name+".row"+\
    " ;")
elif norp=="P" or norp=="p":
    # 获取长度
    len = plpy.execute("select madlib.matrix_ndims('"+input_table1_name+"', 'row=row, val=val') as shape;")[0]["shape"][1]
    # 调用执行函数
    plpy.execute("SELECT "+input_table1_name+".row as row, __db4ai_execute_row_full(1, __db4ai_execute_row_acc("+input_table1_name+".val,"+input_table2_name+".val)/"+str(len)+" ) as val "+
    " into "+output_table_name+\
    " from "+input_table1_name+", "+input_table2_name+\
    " where "+input_table1_name+".row = "+input_table2_name+".row"+\
    " ;")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_acc(float8[], float8[])
RETURNS float8
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_acc'
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
-- db4ai_argmax(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim 处理的维度
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都找最大值的索引，形成输出表
-- 注意 输入的dim是1或者2，返回的索引从1开始，和torch不同。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_argmax(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保 input_table_name 存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_max('"+input_table_name+"','row=row,col=val',"+str(dim)+",'_"+output_table_name+"',true);")
plpy.execute("SELECT 1 as row, index as val into "+output_table_name+" from _"+output_table_name+";")
# 删除中间表
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_argmin(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim 处理的维度
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表的每个元素都找最小值的索引，形成输出表
-- 注意 输入的dim是1或者2，返回的索引从1开始，和torch不同。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_argmin(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保 input_table_name 存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT madlib.matrix_min('"+input_table_name+"','row=row,col=val',"+str(dim)+",'_"+output_table_name+"',true);")
plpy.execute("SELECT 1 as row, index as val into "+output_table_name+" from _"+output_table_name+";")
# 删除中间表
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;

------------------------------------------------------------
------------------------------------------------------------
-- db4ai_argsort(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim 处理的维度
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 将输入的矩阵按照维度索引排序，形成输出表
-- 注意 输入的dim是1或者2。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_argsort(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保 input_table_name 存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
if dim==1: # 按列看，不是很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    # # 建立一个转置的中间矩阵 _XXX
    plpy.execute("SELECT madlib.matrix_trans('"+input_table_name+"', 'row=row, val=val','_"+output_table_name+"');")
    plpy.execute("SELECT row, __db4ai_execute_row_argsort(val) as val into "+output_table_name+" from _"+output_table_name+" order by row;")
    # 转置回来
    plpy.execute("SELECT madlib.matrix_trans('__"+output_table_name+"', 'row=row, val=val','"+output_table_name+"');")
    # # 删除中间矩阵
    plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
    plpy.execute("DROP TABLE IF EXISTS __"+output_table_name+";")
    return 0
else: # 按行看，很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    plpy.execute("SELECT row, __db4ai_execute_row_argsort(val) as val into "+output_table_name+" from "+input_table_name+" order by row;")
    return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_argsort(float8[])
RETURNS INT[]
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_argsort'
LANGUAGE C STRICT;
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_div'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_dot(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵（向量）表名
-- input_table2_name 输入的矩阵（向量）表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的向量求数量积形成输出表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_dot(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保两个向量的长度相同（暂未实装）
# 建立输出表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table1_name+".row as row, __db4ai_execute_row_full(1, __db4ai_execute_row_dot("+input_table1_name+".val,"+input_table2_name+".val)) as val "+
" into "+output_table_name+\
" from "+input_table1_name+", "+input_table2_name+\
" where "+input_table1_name+".row = "+input_table2_name+".row"+\
" ;")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_dot(float8[], float8[])
RETURNS float8
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_dot'
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_exp'
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_full'
LANGUAGE C STRICT;



--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_full(int, float8) --传两个数字，出一个数组
RETURNS float8[]
AS 'db4ai_funcs','__db4ai_execute_row_full'
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_log'
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_pow'
LANGUAGE C STRICT;
------------------------------------------------------------
-- db4ai_precision(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵（向量）表名
-- input_table2_name 输入的矩阵（向量）表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的向量求分数形成输出表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_precision(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保两个向量的长度相同（暂未实装）
# 建立输出表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table1_name+".row as row, __db4ai_execute_row_full(1, __db4ai_execute_row_precision("+input_table1_name+".val,"+input_table2_name+".val)) as val "+
" into "+output_table_name+\
" from "+input_table1_name+", "+input_table2_name+\
" where "+input_table1_name+".row = "+input_table2_name+".row"+\
" ;")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_precision(float8[], float8[])
RETURNS float8
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_precision'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_random(dim1 INT, dim2 INT, _distribution TEXT, _args TEXT, output_table_name TEXT) 
-- dim1 行数
-- dim2 列数
-- _distribution 分布方式: Normal Uniform Bernoulli
-- ——args 更多参数，和_distribution参数有关：
    -- Supported parameters:
        -- Normal: mu, sigma
        -- Uniform: min, max
        -- Bernoulli: lower, upper, prob
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 创建一个dim1行dim2的名为output_table_name的随机矩阵表
-- 实例 select db4ai_random(10,10,'normal','mu=0,sigma=1','random');
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_random(dim1 INT, dim2 INT, _distribution TEXT, _args TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保dim1,dim2非负数
if dim1<0 or dim2<0:
    # 数字参数为负
    return -2
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("Select madlib.matrix_random( "+str(dim1)+" ,"+str(dim2)+", '"+_args+"','"+_distribution+"', '"+output_table_name+"','fmt=dense');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
-- db4ai_recall(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵（向量）表名
-- input_table2_name 输入的矩阵（向量）表名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的向量求分数形成输出表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_recall(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table1_name, input_table2_name存在
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保两个向量的长度相同（暂未实装）
# 建立输出表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT "+input_table1_name+".row as row, __db4ai_execute_row_full(1, __db4ai_execute_row_recall("+input_table1_name+".val,"+input_table2_name+".val)) as val "+
" into "+output_table_name+\
" from "+input_table1_name+", "+input_table2_name+\
" where "+input_table1_name+".row = "+input_table2_name+".row"+\
" ;")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_recall(float8[], float8[])
RETURNS float8
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_recall'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_repeat(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim1 新的行数
-- dim2 新的列数 要求dim1*dim2 = old_dim1 * old_dim2 
-- output_table_name 输出的向量表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表重组，形成输出矩阵表，返回状态码
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_repeat(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保dim1,dim2非负数
if dim1<0 or dim2<0:
    # 数字参数为负
    return -2
# # 复读 转置 复读 转置
# 建立表环境
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("DROP TABLE IF EXISTS _"+input_table_name+";")
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
# 复读第一次 input_table_name -> _input_table_name
plpy.execute("SELECT row, __db4ai_execute_row_repeat("+input_table_name+".val, "+str(dim2)+") as val into _"+input_table_name+" from "+input_table_name+";")
# 转置第一次 _input_table_name -> _output_table_name
plpy.execute("SELECT madlib.matrix_trans('_"+input_table_name+"', 'row=row, val=val','_"+output_table_name+"');")
# 复读第二次 _output_table_name -> __output_table_name
plpy.execute("SELECT row, __db4ai_execute_row_repeat(_"+output_table_name+".val, "+str(dim1)+") as val into __"+output_table_name+" from _"+output_table_name+";")
# 转置第二次 __output_table_name -> output_table_name
plpy.execute("SELECT madlib.matrix_trans('__"+output_table_name+"', 'row=row, val=val','"+output_table_name+"');")
# # 清理中间产物
plpy.execute("DROP TABLE IF EXISTS _"+input_table_name+";")
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
plpy.execute("DROP TABLE IF EXISTS __"+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_repeat(float8[], INT)
RETURNS float8[]
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_repeat'
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
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_reshape(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保dim1,dim2非负数
if dim1<0 or dim2<0:
    # 数字参数为负
    return -2
# 确保dim1*dim2=old_dim1*old_dim2,目前未实现
# 获取原先的列数
old_dim2 = plpy.execute("select madlib.matrix_ndims('"+input_table_name+"', 'row=row, val=val') as shape;")[0]["shape"][1]
# 建立表环境
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("DROP TABLE IF EXISTS _"+input_table_name+";")
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
# # 将表转为稀疏 中间产物 input_table_name -> _input_table_name
plpy.execute("Select madlib.matrix_sparsify('"+input_table_name+"', 'row=row, val=val', '_"+input_table_name+"', 'row=row, col=col, val=val');")
# # 处理表 中间产物 _input_table_name -> _output_table_name
plpy.execute("Select __db4ai_execute_row_reshape(row, col, "+str(old_dim2)+","+str(dim2)+") as row, "+\
"__db4ai_execute_col_reshape(row, col, "+str(old_dim2)+","+str(dim2)+") as col, "+\
" val as val" +\
" INTO _"+output_table_name+\
" FROM _"+input_table_name+";")
# # 将表转回稠密 output_table_name -> _output_table_name
plpy.execute("Select madlib.matrix_densify('_"+output_table_name+"', 'row=row, col=col, val=val', '"+output_table_name+"', 'row=row, val=val');")
# # 清理中间产物
plpy.execute("DROP TABLE IF EXISTS _"+input_table_name+";")
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_reshape(INT, INT, INT, INT)
RETURNS INT
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_reshape'
LANGUAGE C STRICT;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_col_reshape(INT, INT, INT, INT)
RETURNS INT
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_col_reshape'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_reverse(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim 处理的维度
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 将输入的矩阵按照维度flip，形成输出表
-- 注意 输入的dim是1或者2
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_reverse(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保 input_table_name 存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
if dim==1: # 按列看，不是很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    # # 建立一个转置的中间矩阵 _out __out
    plpy.execute("SELECT madlib.matrix_trans('"+input_table_name+"', 'row=row, val=val','_"+output_table_name+"');")
    plpy.execute("SELECT row, __db4ai_execute_row_flip(val) as val into __"+output_table_name+" from _"+output_table_name+" order by row;")
    # 转置回来
    plpy.execute("SELECT madlib.matrix_trans('__"+output_table_name+"', 'row=row, val=val','"+output_table_name+"');")
    # # 删除中间矩阵
    plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
    plpy.execute("DROP TABLE IF EXISTS __"+output_table_name+";")
    return 0
else: # 按行看，很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    plpy.execute("SELECT row, __db4ai_execute_row_flip(val) as val into "+output_table_name+" from "+input_table_name+" order by row;")
    return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_flip(float8[])
RETURNS float8[]
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_flip'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_shape(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- output_table_name 输出的向量表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表求行数和列数，形成输出矩阵表，返回状态码
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_shape(input_table_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT 1 as row ,madlib.matrix_ndims('"+input_table_name+"', 'row=row, val=val') as val INTO "+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_slice(input_table_name TEXT, dim1_start INT, dim1_end INT, dim2_start INT, dim2_end INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim1_start 起始行
-- dim1_end 终止行
-- dim2_start 起始列
-- dim2_end 终止列
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表切片，保存到输出表中。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_slice(input_table_name TEXT, dim1_start INT, dim1_end INT, dim2_start INT, dim2_end INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# 调用执行函数
plpy.execute("SELECT row, val["+str(dim2_start)+":"+str(dim2_end)+"] as val "+\
" INTO "+output_table_name+\
" FROM "+input_table_name+\
" WHERE row>="+str(dim1_start)+" and row<= "+str(dim1_end)+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_softmax(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim 处理的维度
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 将输入的矩阵按照维度softmax，形成输出表
-- 注意 输入的dim是1或者2。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_softmax(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保 input_table_name 存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
if dim==1: # 按列看，不是很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    # # 建立一个转置的中间矩阵 _XXX
    plpy.execute("SELECT madlib.matrix_trans('"+input_table_name+"', 'row=row, val=val','_"+output_table_name+"');")
    plpy.execute("SELECT row, __db4ai_execute_row_softmax(val) as val into "+output_table_name+" from _"+output_table_name+" order by row;")
    # 转置回来
    plpy.execute("SELECT madlib.matrix_trans('__"+output_table_name+"', 'row=row, val=val','"+output_table_name+"');")
    # # 删除中间矩阵
    plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
    plpy.execute("DROP TABLE IF EXISTS __"+output_table_name+";")
    return 0
else: # 按行看，很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    plpy.execute("SELECT row, __db4ai_execute_row_softmax(val) as val into "+output_table_name+" from "+input_table_name+" order by row;")
    return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_softmax(float8[])
RETURNS float8[]
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_softmax'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sort(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表名
-- dim 处理的维度
-- output_table_name 输出的表名
-- 返回 执行状态码
-- 效果 将输入的矩阵按照维度sort，形成输出表
-- 注意 输入的dim是1或者2。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_sort(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保 input_table_name 存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
if not (dim==1 or dim==2):
    # 数字参数不当
    return -2
if dim==1: # 按列看，不是很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    # # 建立一个转置的中间矩阵 _XXX
    plpy.execute("SELECT madlib.matrix_trans('"+input_table_name+"', 'row=row, val=val','_"+output_table_name+"');")
    plpy.execute("SELECT row, __db4ai_execute_row_sort(val) as val into "+output_table_name+" from _"+output_table_name+" order by row;")
    # 转置回来
    plpy.execute("SELECT madlib.matrix_trans('__"+output_table_name+"', 'row=row, val=val','"+output_table_name+"');")
    # # 删除中间矩阵
    plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
    plpy.execute("DROP TABLE IF EXISTS __"+output_table_name+";")
    return 0
else: # 按行看，很顺
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    plpy.execute("SELECT row, __db4ai_execute_row_sort(val) as val into "+output_table_name+" from "+input_table_name+" order by row;")
    return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_sort(float8[])
RETURNS float8[]
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_sort'
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
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_sqrt'
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
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sum(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name 输入的矩阵表1名
-- dim 聚合最大值的维度 1为按列 2为按行
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表按选择的维度求和，形成数值表
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_sum(input_table_name TEXT, dim INT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 确保input_table_name存在
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # 字符串参数代表的表不存在数据库中
    return -1
# 确保参数输入正确
if not (dim==1 or dim==2 or dim==0):
    # 数字参数不当
    return -2
if dim==0:
    # 将一次求和的结果求和再形成张量表返回
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    plpy.execute("SELECT 1 as row, __db4ai_execute_row_full(1, __db4ai_execute_row_sum(madlib.matrix_sum('"+input_table_name+"','row=row,col=val', 1))) as val INTO "+output_table_name+";")
    return 0
else:
    # 建立表
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # 调用执行函数
    plpy.execute("SELECT 1 as row, madlib.matrix_sum('"+input_table_name+"','row=row,col=val',"+str(dim)+") as val INTO "+output_table_name+";")
    return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_tensordot(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name 输入的矩阵表1名
-- input_table2_name 输入的矩阵表2名
-- output_table_name 输出的矩阵表名
-- 返回 执行状态码
-- 效果 将输入的矩阵表做tensorfot，形成输出矩阵表
-- 注意 目前仅实装为dot
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_tensordot(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
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
plpy.execute("SELECT db4ai_dot('"+input_table1_name+"','"+input_table2_name+"','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_trace(input_table_name TEXT, output_table_name TEXT)
-- input_table_name 输入的表名
-- output_table_name 输出的表名
-- 返回 执行状态码 如果不是方阵则返回-3。
-- 效果 将输入的表名对应的方阵求trace后返回，和其它张量操作不同。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_trace(input_table_name TEXT, output_table_name TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 表不存在则返回-1
table_ans = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")
not_exists = table_ans[0]["count"]==0
if not_exists:
    return -1
# 表不是方阵则返回-3（尚未实现）
not_square = False
if not_square:
    return -3
# 计算trace并返回：用matrix_extract_diag提取对角线向量，再求和
# 建立表
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("SELECT 1 as row, __db4ai_execute_row_full(1, __db4ai_execute_row_sum(madlib.matrix_extract_diag('"+input_table_name+"','row=row,col=val'))) as val into "+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[执行函数]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_sum(float8[]) -- 别忘了改这里！！！
RETURNS float8
AS '/home/postgres/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_sum'
LANGUAGE C STRICT;
------------------------------------------------------------
-- db4ai_val 设置常数变量的模块，包括set和get方法
------------------------------------------------------------
-- db4ai_val_set(var_name TEXT, val TEXT)
-- var_name 输入的变量名
-- val 输入的变量值
-- 返回 执行状态码
-- 效果 将输入的变量名存储起来
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_val_set(var_name TEXT, val TEXT) --调用时表名不用加什么引号，传个字符串即可
RETURNS INTEGER AS $$
# 如果常量表不存在，则新建
val_table_name = "vals"
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+val_table_name+"'")[0]["count"]==0
if not_exists:
    plpy.execute("CREATE TABLE "+val_table_name+"(var_name TEXT, val TEXT);") 
# 移除变量并重新添加
plpy.execute("DELETE FROM "+val_table_name+" WHERE var_name='"+val+"';")
plpy.execute("INSERT INTO "+val_table_name+" VALUES ('"+var_name+"','"+val+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
-- db4ai_val_get(var_name TEXT)
-- var_name 输出的变量名
-- 返回 执行状态码
-- 效果 读取请求的变量名以字符串的方式返回，不存在返回"NONE"。
------------------------------------------------------------
--[客户端接口]--
CREATE OR REPLACE FUNCTION
db4ai_val_get(var_name TEXT)
RETURNS TEXT AS $$
# 如果常量表不存在，则返回无
val_table_name = "vals"
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+val_table_name+"'")[0]["count"]==0
if not_exists:
    return "NONE"
# 返回变量数值字符串
ans = plpy.execute("select val from "+val_table_name+" WHERE var_name='"+var_name+"';")
if len(ans)==0:
    return "NONE"
else: 
    return ans[0]["val"]
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