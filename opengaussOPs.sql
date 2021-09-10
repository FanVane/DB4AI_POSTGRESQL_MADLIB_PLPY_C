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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_abs' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_abs(float8[])
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_abs'
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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_add'
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_add(float8[], float8[])
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_add'
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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_div'
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_div(float8[], float8[])
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_div'
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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_exp' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_exp(float8[])
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_exp'
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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_mul'
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_mul(float8[], float8[])
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_mul'
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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_ones' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_ones(INT)
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_ones'
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
AS '/home/postgres/OPs/opengaussOPs','outer_db4ai_zeros' -- ǰ���ֶ���Ϊ����·�� �����ֶ��ĳ�.so�еķ�����
LANGUAGE C STRICT;
--[ִ�к���]--
CREATE OR REPLACE FUNCTION
inner_db4ai_zeros(INT)
RETURNS float8[]
AS '/home/postgres/OPs/opengaussOPs','inner_db4ai_zeros'
LANGUAGE C STRICT;