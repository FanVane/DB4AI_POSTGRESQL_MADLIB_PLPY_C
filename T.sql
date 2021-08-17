--[�������ܲ���]--
CREATE OR REPLACE FUNCTION
add_ab(int, int)
RETURNS INT
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','add_ab'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_abs(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ľ������
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���������γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_abs(input_table_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name,output_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_abs("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_abs(float8[])
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_abs'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1��
-- input_table2_name ����ľ����2��
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���������γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table1_name, input_table2_name����
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_add('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_argmax(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name ����ľ������
-- dim �����ά��
-- output_table_name ����ı���
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض������ֵ���������γ������
-- ע�� �����dim��1����2�����ص�������1��ʼ����torch��ͬ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_argmax(input_table_name TEXT, dim INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ�� input_table_name ����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
if not (dim==1 or dim==2):
    # ���ֲ�������
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_max('"+input_table_name+"','row=row,col=val',"+str(dim)+",'_"+output_table_name+"',true);")
plpy.execute("SELECT 1 as row, index as val into "+output_table_name+" from _"+output_table_name+";")
# ɾ���м��
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_argmin(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name ����ľ������
-- dim �����ά��
-- output_table_name ����ı���
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض�����Сֵ���������γ������
-- ע�� �����dim��1����2�����ص�������1��ʼ����torch��ͬ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_argmin(input_table_name TEXT, dim INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ�� input_table_name ����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
if not (dim==1 or dim==2):
    # ���ֲ�������
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_min('"+input_table_name+"','row=row,col=val',"+str(dim)+",'_"+output_table_name+"',true);")
plpy.execute("SELECT 1 as row, index as val into "+output_table_name+" from _"+output_table_name+";")
# ɾ���м��
plpy.execute("DROP TABLE IF EXISTS _"+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1������������
-- input_table2_name ����ľ����2����������
-- output_table_name ����ľ������  (��)
-- ���� ִ��״̬��
-- Ч�� ������ľ����Ԫ�ؽ��г������㣬�γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table1_name, input_table2_name����
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT "+input_table1_name+".row as row,__db4ai_execute_row_div("+input_table1_name+".val,"+input_table2_name+".val) as val "+
" into "+output_table_name+\
" from "+input_table1_name+", "+input_table2_name+\
" where "+input_table1_name+".row = "+input_table2_name+".row;")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_div(float8[], float8[])
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_div'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_dot(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ�������������
-- input_table2_name ����ľ�������������
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ��������������������γ������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_dot(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table1_name, input_table2_name����
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # �ַ�����������ı��������ݿ���
    return -1
# ȷ�����������ĳ�����ͬ����δʵװ��
# ���������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT 1 as row, __db4ai_execute_row_full(1, __db4ai_execute_row_dot("+input_table1_name+".val,"+input_table2_name+".val)) as val "+
" into "+output_table_name+\
" from "+input_table1_name+", "+input_table2_name+\
" ;")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_dot(float8[], float8[])
RETURNS float8
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_dot'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_exp(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ľ������
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���eΪ�׵�ָ�����γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_exp(input_table_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_exp("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_exp(float8[]) --����һ�����飬����һ������
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_exp'
LANGUAGE C STRICT;
-------------------------------------------------------------
-------------------------------------------------------------
-- db4ai_full(dim1 INT, dim2 INT, full_value float8,output_table_name TEXT)
-- dim1 ����
-- dim2 ����
-- full_value ���ֵ
-- output_table_name ����ı���
-- ���� ִ��״̬��
-- Ч�� ����һ��dim1��dim2����Ϊoutput_table_name��ȫ0�����
-- ���� �������ԣ��ں�zeros�Ƚ�ʱ�����Բ����ò���
-------------------------------------------------------------
----[�ͻ��˽ӿ�]----
CREATE OR REPLACE FUNCTION
db4ai_full(dim1 INT, dim2 INT, full_value float8,output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��dim1,dim2�Ǹ���
if dim1<0 or dim2<0:
    # ���ֲ���Ϊ��
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("CREATE TABLE "+output_table_name+"(row int, val float8[]);")
# �����
for i in range(dim1):
    plpy.execute("INSERT INTO "+output_table_name+" SELECT "+str(i+1)+" as row, __db4ai_execute_row_full("+str(dim2)+","+str(full_value)+") as val;")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_full(int, float8) --���������֣���һ������
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_full'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_log(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ľ������
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���������γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_log(input_table_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_log("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_log(float8[])
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_log'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_matmul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1��
-- input_table2_name ����ľ����2��
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ���������ˣ��γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_matmul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table1_name, input_table2_name����
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_mult('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_max(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name ����ľ����1��
-- dim �ۺ����ֵ��ά�� 1Ϊ���� 2Ϊ����
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ����ѡ���ά��Ѱ�����ֵ���γ����������ֵ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_max(input_table_name TEXT, dim INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ȷ������������ȷ
if not (dim==1 or dim==2):
    # ���ֲ�������
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_max('"+input_table_name+"','row=row,col=val',"+str(dim)+",'"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_mean(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name ����ľ����1��
-- dim �ۺ���Сֵ��ά�� 1Ϊ���� or 2Ϊ����
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ����ѡ���ά�ȼ����ֵ���γ���ֵ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_mean(input_table_name TEXT, dim INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ȷ������������ȷ
if not (dim==1 or dim==2):
    # ���ֲ�������
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_mean('"+input_table_name+"','row=row,col=val',"+str(dim)+") as val into "+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_min(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name ����ľ����1��
-- dim �ۺ���Сֵ��ά�� 1Ϊ���� 2Ϊ����
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ����ѡ���ά��Ѱ����Сֵ���γ����������ֵ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_min(input_table_name TEXT, dim INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ȷ������������ȷ
if not (dim==1 or dim==2):
    # ���ֲ�������
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_min('"+input_table_name+"','row=row,col=val',"+str(dim)+",'"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1��
-- input_table2_name ����ľ����2��
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ������Ԫ����ˣ��γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table1_name, input_table2_name����
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_elem_mult('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_ones(dim1 INT, dim2 INT, output_table_name TEXT)
-- dim1 ����
-- dim2 ����
-- output_table_name ����ı���
-- ���� ִ��״̬��
-- Ч�� ����һ��dim1��dim2����Ϊoutput_table_name��ȫ1�����
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_ones(dim1 INT, dim2 INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��dim1,dim2�Ǹ���
if dim1<0 or dim2<0:
    # ���ֲ���Ϊ��
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("Select madlib.matrix_ones("+str(dim1)+" ,"+str(dim2)+" ,'"+output_table_name+"','fmt=dense');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_pow(input_table_name TEXT,pow_exp float8,output_table_name TEXT)
-- input_table_name ����ľ������
-- pow_exp Ϊÿ��Ԫ�ؽ���ָ�������ָ��ֵ
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض�����Ӧ��ָ�����γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_pow(input_table_name TEXT,pow_exp float8,output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_pow("+input_table_name+".val, "+str(pow_exp)+") as val " +\
" INTO "+output_table_name+\
" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_pow(float8[], float8) -- DONT'T FORGET TO CHANGE IT AFTER COPY.
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_pow'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_random(dim1 INT, dim2 INT, output_table_name TEXT)
-- dim1 ����
-- dim2 ����
-- _distribution �ֲ���ʽ: Normal Uniform Bernoulli
-- ����args �����������_distribution�����йأ�
    -- Supported parameters:
        -- Normal: mu, sigma
        -- Uniform: min, max
        -- Bernoulli: lower, upper, prob
-- output_table_name ����ı���
-- ���� ִ��״̬��
-- Ч�� ����һ��dim1��dim2����Ϊoutput_table_name����������
-- ʵ�� select db4ai_random(10,10,'normal','mu=0,sigma=1','random');
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_random(dim1 INT, dim2 INT, _distribution TEXT, _args TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��dim1,dim2�Ǹ���
if dim1<0 or dim2<0:
    # ���ֲ���Ϊ��
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("Select madlib.matrix_random( "+str(dim1)+" ,"+str(dim2)+", '"+_args+"','"+_distribution+"', '"+output_table_name+"','fmt=dense');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_shape(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ľ������
-- output_table_name �������������
-- ���� ִ��״̬��
-- Ч�� ������ľ�������������������γ�������������״̬��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_shape(input_table_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT 1 as row ,madlib.matrix_ndims('"+input_table_name+"', 'row=row, val=val') as val INTO "+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sqrt(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ľ������
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���ƽ�������γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_sqrt(input_table_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT "+input_table_name+".row as row, __db4ai_execute_row_sqrt("+input_table_name+".val) as val " + " INTO "+output_table_name+" FROM "+input_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_sqrt(float8[]) -- DONT'T FORGET TO CHANGE IT AFTER COPY.
RETURNS float8[]
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_sqrt'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sub(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1������������
-- input_table2_name ����ľ����2����������
-- output_table_name ����ľ������ ���
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���������γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_sub(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table1_name, input_table2_name����
not_exists1 = plpy.execute("select count(*) from pg_class where relname = '"+input_table1_name+"'")[0]["count"]==0
not_exists2 = plpy.execute("select count(*) from pg_class where relname = '"+input_table2_name+"'")[0]["count"]==0
if not_exists1 or not_exists2:
    # �ַ�����������ı��������ݿ���
    return -1
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
# ����ִ�к���
plpy.execute("SELECT madlib.matrix_sub('"+input_table1_name+"','row=row,col=val','"+input_table2_name+"','row=row,col=val','"+output_table_name+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_sum(input_table_name TEXT, dim INT, output_table_name TEXT)
-- input_table_name ����ľ����1��
-- dim �ۺ����ֵ��ά�� 1Ϊ���� 2Ϊ����
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ����ѡ���ά����ͣ��γ���ֵ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_sum(input_table_name TEXT, dim INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��input_table_name����
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")[0]["count"]==0
if not_exists:
    # �ַ�����������ı��������ݿ���
    return -1
# ȷ������������ȷ
if not (dim==1 or dim==2 or dim==0):
    # ���ֲ�������
    return -2
if dim==0:
    # ��һ����͵Ľ��������γ���������
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # ����ִ�к���
    plpy.execute("SELECT 1 as row, __db4ai_execute_row_full(1, __db4ai_execute_row_sum(madlib.matrix_sum('"+input_table_name+"','row=row,col=val', 1))) as val INTO "+output_table_name+";")
    return 0
else:
    # ������
    plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
    # ����ִ�к���
    plpy.execute("SELECT 1 as row, madlib.matrix_sum('"+input_table_name+"','row=row,col=val',"+str(dim)+") as val INTO "+output_table_name+";")
    return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_trace(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ı���
-- output_table_name ����ı���
-- ���� ִ��״̬�� ������Ƿ����򷵻�-3��
-- Ч�� ������ı�����Ӧ�ķ�����trace�󷵻أ�����������������ͬ��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_trace(input_table_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# �������򷵻�-1
table_ans = plpy.execute("select count(*) from pg_class where relname = '"+input_table_name+"'")
not_exists = table_ans[0]["count"]==0
if not_exists:
    return -1
# ���Ƿ����򷵻�-3����δʵ�֣�
not_square = False
if not_square:
    return -3
# ����trace�����أ���matrix_extract_diag��ȡ�Խ��������������
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("SELECT 1 as row, __db4ai_execute_row_full(1, __db4ai_execute_row_sum(madlib.matrix_extract_diag('"+input_table_name+"','row=row,col=val'))) as val into "+output_table_name+";")
return 0
$$ LANGUAGE plpythonu;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
__db4ai_execute_row_sum(float8[]) -- �����˸��������
RETURNS float8
AS '/home/lbx/soft/db4ai_funcs/db4ai_funcs','__db4ai_execute_row_sum'
LANGUAGE C STRICT;
------------------------------------------------------------
-- db4ai_val ���ó���������ģ�飬����set��get����
------------------------------------------------------------
-- db4ai_val_set(var_name TEXT, val TEXT)
-- var_name ����ı�����
-- val ����ı���ֵ
-- ���� ִ��״̬��
-- Ч�� ������ı������洢����
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_val_set(var_name TEXT, val TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ������������ڣ����½�
val_table_name = "vals"
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+val_table_name+"'")[0]["count"]==0
if not_exists:
    plpy.execute("CREATE TABLE "+val_table_name+"(var_name TEXT, val TEXT);") 
# �Ƴ��������������
plpy.execute("DELETE FROM "+val_table_name+" WHERE var_name='"+val+"';")
plpy.execute("INSERT INTO "+val_table_name+" VALUES ('"+var_name+"','"+val+"');")
return 0
$$ LANGUAGE plpythonu;
------------------------------------------------------------
-- db4ai_val_get(var_name TEXT)
-- var_name ����ı�����
-- ���� ִ��״̬��
-- Ч�� ��ȡ����ı��������ַ����ķ�ʽ���أ������ڷ���"NONE"��
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_val_get(var_name TEXT)
RETURNS TEXT AS $$
# ������������ڣ��򷵻���
val_table_name = "vals"
not_exists = plpy.execute("select count(*) from pg_class where relname = '"+val_table_name+"'")[0]["count"]==0
if not_exists:
    return "NONE"
# ���ر�����ֵ�ַ���
ans = plpy.execute("select val from "+val_table_name+" WHERE var_name='"+var_name+"';")
if len(ans)==0:
    return "NONE"
else: 
    return ans[0]["val"]
$$ LANGUAGE plpythonu;
-------------------------------------------------------------
-------------------------------------------------------------
-- db4ai_zeros(dim1 INT, dim2 INT, output_table_name TEXT)
-- dim1 ����
-- dim2 ����
-- output_table_name ����ı���
-- ���� ִ��״̬��
-- Ч�� ����һ��dim1��dim2����Ϊoutput_table_name��ȫ0�����
-------------------------------------------------------------
----[�ͻ��˽ӿ�]----
CREATE OR REPLACE FUNCTION
db4ai_zeros(dim1 INT, dim2 INT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER AS $$
# ȷ��dim1,dim2�Ǹ���
if dim1<0 or dim2<0:
    # ���ֲ���Ϊ��
    return -2
# ������
plpy.execute("DROP TABLE IF EXISTS "+output_table_name+";")
plpy.execute("Select madlib.matrix_zeros("+str(dim1)+" ,"+str(dim2)+" ,'"+output_table_name+"','fmt=dense');")
return 0
$$ LANGUAGE plpythonu;
----------------------------------------------------------
----------------------------------------------------------