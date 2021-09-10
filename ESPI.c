// SPI��չ����������SQL���ԡ�
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#define MAX_SQL_LEN 64
/**
 * ��������ʾ�ı��Ƿ���ڡ�
 * @arg table_name ���ı���
 * @return ��Ӧ��SQL���
 */
char* ESQL_table_exists(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "select count(*) from pg_class where relname = '");
    strcat(esql, table_name);
    strcat(esql, "';");
    return esql;
}

/**
 * ɾ��һ����
 * @arg table_name ɾ���ı���
 * @return ��Ӧ��SQL���
 */
char* ESQL_drop_table_if_exists(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "DROP TABLE IF EXISTS ");
    strcat(esql, table_name);
    strcat(esql, ";");
    return esql;
}

/**
 * ����һ����ͷ
 */ 
// CREATE TABLE <table_name>(rows INT, cols INT, trans INT, data float8[]);
char* ESQL_create_table_head(char* table_name){
    char* esql = malloc(MAX_SQL_LEN*sizeof(char));
    strcpy(esql, "CREATE TABLE ");
    strcat(esql, table_name);
    strcat(esql, "(rows INT, cols INT, trans INT, data float8[]);");
    return esql;
}


int main(){
    int rows = 10;
    int cols = 10;
    char* output_table_name = "dasda";
    char sql[MAX_SQL_LEN];
    sprintf(sql,"SELECT %d as rows, %d as cols, 0 as trans, inner_db4ai_zeros(%d) as data into %s;",
        rows, cols, rows * cols, output_table_name);
    printf(sql);
}