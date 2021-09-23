------------------------------------------------------------
------------------------------------------------------------
-- db4ai_abs(input_table_name TEXT, output_table_name TEXT)
-- input_table_name ����ľ������
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض������ֵ���γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_abs(input_table_name TEXT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z15outer_db4ai_absP20FunctionCallInfoData' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_abs(float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_absP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1��
-- input_table2_name ����ľ����2��
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���ͣ��γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_add(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER
AS 'db4ai_funcs','_Z15outer_db4ai_addP20FunctionCallInfoData'
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_add(float8[], float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_addP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1��
-- input_table2_name ����ľ����2��
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض�������γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_div(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER
AS 'db4ai_funcs','_Z15outer_db4ai_divP20FunctionCallInfoData'
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_div(float8[], float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_divP20FunctionCallInfoData'
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
db4ai_exp(input_table_name TEXT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z15outer_db4ai_expP20FunctionCallInfoData' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_exp(float8[])
RETURNS float8[]
AS 'db4ai_funcs','_Z15inner_db4ai_expP20FunctionCallInfoData'
LANGUAGE C STRICT;
------------------------------------------------------------
------------------------------------------------------------
-- db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT)
-- input_table1_name ����ľ����1��
-- input_table2_name ����ľ����2��
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ������ľ�����ÿ��Ԫ�ض���˻����γ���������
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_mul(input_table1_name TEXT, input_table2_name TEXT, output_table_name TEXT) --����ʱ�������ü�ʲô���ţ������ַ�������
RETURNS INTEGER
AS 'db4ai_funcs','_Z15outer_db4ai_mulP20FunctionCallInfoData'
LANGUAGE C STRICT;
--[ִ�к���]--
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
-- rows cols ��������� ����
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ���ղ����Ĺ������һ��ȫ0�ľ���
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_ones(rows INT, cols INT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z16outer_db4ai_onesP20FunctionCallInfoData' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_ones(INT)
RETURNS float8[]
AS 'db4ai_funcs','_Z16inner_db4ai_onesP20FunctionCallInfoData'
LANGUAGE C STRICT;

------------------------------------------------------------
------------------------------------------------------------
-- db4ai_reshape(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT)
-- input_table_name ����ľ������
-- dim1 �µ�����
-- dim2 �µ����� Ҫ��dim1*dim2 = old_dim1 * old_dim2 
-- output_table_name �������������
-- ���� ִ��״̬��
-- Ч�� ������ľ�������飬�γ�������������״̬��
-- ���� select db4ai_reshape('ones', 2, 6, 'reshape');
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
db4ai_reshape(input_table_name TEXT,dim1 INT, dim2 INT,output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z19outer_db4ai_reshapeP20FunctionCallInfoData'
LANGUAGE C STRICT;



------------------------------------------------------------
------------------------------------------------------------
-- db4ai_zeros(rows INT, cols INT, output_table_name TEXT)
-- rows cols ��������� ����
-- output_table_name ����ľ������
-- ���� ִ��״̬��
-- Ч�� ���ղ����Ĺ������һ��ȫ0�ľ���
------------------------------------------------------------
--[�ͻ��˽ӿ�]--
CREATE OR REPLACE FUNCTION
db4ai_zeros(rows INT, cols INT, output_table_name TEXT)
RETURNS INT
AS 'db4ai_funcs','_Z17outer_db4ai_zerosP20FunctionCallInfoData' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_zeros(INT)
RETURNS float8[]
AS 'db4ai_funcs','_Z17inner_db4ai_zerosP20FunctionCallInfoData'
LANGUAGE C STRICT;