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