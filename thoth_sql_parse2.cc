#define MYSQL_SERVER

#include <string>
#include <stdlib.h>
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/partition_info.h"
#include "sql/sql_parse.h"
#include "include/cJSON.h"
#include "include/extern_define.h"

//#pragma comment(lib,"/data/sqlparser/lib/libsqlparser-debug.a")

using namespace std;

THD *thoth_parser(char *query, char *default_db) {
//    THD *hca_thd;
    thd->reset_db(default_db, strlen(default_db));
    if (dispatch_command(COM_QUERY, thd, query, (uint) strlen(query))) {
        return NULL;
    }
    return thd;
}


char *thoth_sql_dql(char * query, char * o_dbname) {
    LEX *sql_lex, *sql_lex_check;
    THD *hca_thd, *hca_thd_check;


    mysqld_init("");

    hca_thd = thoth_parser(query, o_dbname);
    sql_lex = hca_thd->lex;

    if (sql_lex == NULL) {
        sql_print_error("parse error \n");
        exit(-1);
    }

    SELECT_LEX *select_lex = &sql_lex->select_lex;

    const Item::Type  item_where_type = select_lex->where->type();

    sql_print_information("%d", item_where_type);


}



char *thoth_sql(char *query, char *o_dbname) {
    LEX *sql_lex, *sql_lex_check;
    THD *hca_thd, *hca_thd_check;

    cJSON *table_def = cJSON_CreateObject();
    cJSON *properties = cJSON_CreateObject();
    cJSON *fields = cJSON_CreateArray();
    cJSON *fields_def = 0;
    cJSON *keys = cJSON_CreateArray();
    cJSON *key_def = 0;
    cJSON *key_column = 0;
    cJSON *column_item = 0;

    char *query_check = add_check_flag(query, strlen(query));

    printf("%s\n", query_check);

    if (strlen(o_dbname) == 0) {
        o_dbname = const_cast<char *>("UNKNOW");
    }

//    char *query_check = trimwhitespace(query, strlen(query));
//    return query_check;

//    if (mysqld_init("")) {
//        return NULL;
//    }
    mysqld_init("");
//    cJSON_AddStringToObject(table_def, "sql_parse_error_by_check", query_check);
//    return cJSON_Print(table_def);

//
//    sql_print_information("%s", query);
//    sql_print_information("%s", query_check);

    hca_thd_check = thoth_parser(query_check, o_dbname);
    sql_lex_check = hca_thd_check->lex;

    if(sql_lex_check->sql_command == SQLCOM_END){
        cJSON_AddStringToObject(table_def, "sql_parse_error", SYNTAX_ERROR);
        return cJSON_Print(table_def);
    }
    if (sql_lex_check->sql_command == SQLCOM_CREATE_INDEX ||
        sql_lex_check->sql_command == SQLCOM_RENAME_TABLE ||
        sql_lex_check->sql_command == SQLCOM_DROP_INDEX) {
        cJSON_AddStringToObject(table_def, "sql_parse_error", DO_NOT_SUGGEST_STMT);
        return cJSON_Print(table_def);
    }
    else if (sql_lex_check->sql_command == SQLCOM_ALTER_TABLE ||
             sql_lex_check->sql_command == SQLCOM_CREATE_TABLE) {
        HA_CREATE_INFO create_info_check(sql_lex_check->create_info);
        HA_CREATE_INFO *create_info_check_ptr = &create_info_check;

//        if (create_info_check_ptr->stats_sample_pages == NULL ||
//            strcmp((char *)create_info_check_ptr->db_type, "stmt_check") != 0 ) {
//            cJSON_AddStringToObject(table_def, "sql_parse_error", SYNTAX_ERROR);
//            return cJSON_Print(table_def);
//        }
        if (create_info_check_ptr->stats_sample_pages != 1989 ) {
            cJSON_AddStringToObject(table_def, "sql_parse_error", SYNTAX_ERROR);
            return cJSON_Print(table_def);
        }
    }
    else {
        cJSON_AddStringToObject(table_def, "sql_parse_error", DO_NOT_SUPPORT_STMT);
        return cJSON_Print(table_def);
    }

//    sql_parser_cleanup();
//
//    hca_thd_check->end_statement();
//    hca_thd_check->cleanup_after_query();
//    free_root(hca_thd_check->mem_root,MYF(MY_KEEP_PREALLOC));
//    lex_free();
//    lex_end(sql_lex_check);
//
//
    mysqld_cleanup();
//
//    if (mysqld_init("")) {
//        return NULL;
//    }
    mysqld_init("");

    hca_thd = thoth_parser(query, o_dbname);
    sql_lex = hca_thd->lex;

    if (sql_lex == NULL) {
        sql_print_error("parse error \n");
        exit(-1);
    }


    SELECT_LEX *select_lex = &sql_lex->select_lex;

    TABLE_LIST *create_table = select_lex->table_list.first;
    HA_CREATE_INFO create_info(sql_lex->create_info);
    HA_CREATE_INFO *create_info_ptr = &create_info;
    Alter_info alter_info(hca_thd->lex->alter_info, hca_thd->mem_root);
    Alter_info *alter_info_ptr = &alter_info;
    List_iterator<Key> key_iterator(alter_info_ptr->key_list);
    List_iterator<Create_field> it(alter_info_ptr->create_list);
    Create_field *sql_field;
    Key *key;


//    if (create_table == NULL) {
//        cJSON_AddStringToObject(table_def, "sql_parse_error", CREATE_TABLE_ERROR);
//        return cJSON_Print(table_def);
//    }
//
//    if (create_info_ptr->db_type == NULL ||
//        create_info_ptr->default_table_charset == NULL ||
//        create_info_ptr->comment.length == 0) {
//        cJSON_AddStringToObject(table_def, "sql_parse_error", TABLE_PROPERTIES_ERROR);
//        return cJSON_Print(table_def);
//    }

    if (sql_lex->sql_command == SQLCOM_CREATE_TABLE)
        cJSON_AddStringToObject(table_def, "sql_type", "SQLCOM_CREATE_TABLE");
    if (sql_lex->sql_command == SQLCOM_ALTER_TABLE)
        cJSON_AddStringToObject(table_def, "sql_type", "SQLCOM_ALTER_TABLE");


    if (strcmp(create_table->db, "UNKNOW") == 0) {
        cJSON_AddStringToObject(table_def, "db_name", "");
    }
    else {
        cJSON_AddStringToObject(table_def, "db_name", create_table->db);
    }
//    cJSON_AddStringToObject(table_def, "db_name", create_table->db);

    if (hca_thd->lex->name.str && alter_info.flags == Alter_info::ALTER_RENAME) {
        cJSON_AddStringToObject(table_def, "table_name_str", hca_thd->lex->name.str);
        cJSON_AddNumberToObject(table_def, "table_name_length", strlen(hca_thd->lex->name.str));
        cJSON_AddNumberToObject(table_def, "table_rename_flag", 1);
    }
    else{
        cJSON_AddStringToObject(table_def, "table_name_str", create_table->table_name);
        cJSON_AddNumberToObject(table_def, "table_name_length", strlen(create_table->table_name));
        cJSON_AddNumberToObject(table_def, "table_rename_flag", 0);
    }


//    cJSON_AddStringToObject(table_def, "table_name_str", create_table->table_name);
//    cJSON_AddNumberToObject(table_def, "table_name_length", strlen(create_table->table_name));

    cJSON_AddItemToObject(table_def, "ct_properties", properties);
    cJSON_AddItemToObject(table_def, "ct_fields", fields);
    cJSON_AddItemToObject(table_def, "ct_keys", keys);

    if (create_info_ptr->db_type) {
        cJSON_AddStringToObject(properties, "engine", (char *) create_info_ptr->db_type);
    } else {
        cJSON_AddStringToObject(properties, "engine", "");
    }
    cJSON_AddNumberToObject(properties, "auto_increment", create_info_ptr->auto_increment_value);

    if (create_info_ptr->default_table_charset) {
        cJSON_AddStringToObject(properties, "charset", create_info_ptr->default_table_charset->csname);
        cJSON_AddStringToObject(properties, "collate", create_info_ptr->default_table_charset->name);
    } else {
        cJSON_AddStringToObject(properties, "charset", "");
        cJSON_AddStringToObject(properties, "collate", "");
    }

    cJSON_AddNumberToObject(properties, "comment_length", create_info_ptr->comment.length);


    cJSON_AddNumberToObject(properties, "primary_key_flag", 0);
    if (sql_lex->part_info) {
        cJSON_AddNumberToObject(properties, "partition_flag", 1);
    } else {
        cJSON_AddNumberToObject(properties, "partition_flag", 0);
    }


    char *sql_field_charset_csname;
    while ((sql_field = it++)) {
        sql_field_charset_csname = "";

        fields_def = cJSON_CreateObject();
        cJSON_AddItemToArray(fields, fields_def);

        cJSON_AddStringToObject(fields_def, "field_name_str", sql_field->field_name);
        cJSON_AddNumberToObject(fields_def, "field_name_length", strlen(sql_field->field_name));

        if (sql_field->charset) {
            sql_field_charset_csname = (char *) sql_field->charset->csname;
            cJSON_AddStringToObject(fields_def, "field_charset", sql_field->charset->csname);
            cJSON_AddStringToObject(fields_def, "field_collate", sql_field->charset->name);
        } else {
            cJSON_AddStringToObject(fields_def, "field_charset", "");
            cJSON_AddStringToObject(fields_def, "field_collate", "");
        }

        FIELD_BASIC_INFO field_basic_info_ptr = get_field_basic_info(sql_field->sql_type, sql_field_charset_csname,
                                                                     sql_field->length, sql_field->decimals);
        cJSON_AddStringToObject(fields_def, "field_type", field_basic_info_ptr.fbi_field_type_str);
        cJSON_AddNumberToObject(fields_def, "field_length", field_basic_info_ptr.fbi_field_length);
        cJSON_AddNumberToObject(fields_def, "field_decimal", field_basic_info_ptr.fbi_field_decimal);

        cJSON_AddNumberToObject(fields_def, "field_comment_length", sql_field->comment.length);

        if (sql_field->flags & AUTO_INCREMENT_FLAG) {
            cJSON_AddNumberToObject(fields_def, "field_is_auto_increment", 1);
        } else {
            cJSON_AddNumberToObject(fields_def, "field_is_auto_increment", 0);
        }

        if (sql_field->flags & NOT_NULL_FLAG) {
            cJSON_AddNumberToObject(fields_def, "field_is_nullable", 0);
        } else {
            cJSON_AddNumberToObject(fields_def, "field_is_nullable", 1);
        }

        if (sql_field->flags & NO_DEFAULT_VALUE_FLAG) {
            cJSON_AddNumberToObject(fields_def, "field_has_default", 0);
        } else {
            cJSON_AddNumberToObject(fields_def, "field_has_default", 1);
        }

        if (sql_field->flags & ON_UPDATE_NOW_FLAG) {
            cJSON_AddNumberToObject(fields_def, "field_is_on_update_now", 1);
        } else {
            cJSON_AddNumberToObject(fields_def, "field_is_on_update_now", 0);
        }
    }

    while ((key = key_iterator++)) {
        List_iterator<Key_part_spec> k_it(key->columns);
        Key_part_spec *kk;
        key_def = cJSON_CreateObject();
        cJSON_AddItemToArray(keys, key_def);
        long key_length = 0;
        if (key->name.str) {
            cJSON_AddStringToObject(key_def, "key_name_str", key->name.str);
            cJSON_AddNumberToObject(key_def, "key_name_length", strlen(key->name.str));
        } else {
            cJSON_AddStringToObject(key_def, "key_name_str", "");
            cJSON_AddNumberToObject(key_def, "key_name_length", 0);
        }
        cJSON_AddNumberToObject(key_def, "key_field_count", key->columns.elements);

        if (key->type == Key::PRIMARY) {
            cJSON_DeleteItemFromObject(properties, "primary_key_flag");
            cJSON_AddNumberToObject(properties, "primary_key_flag", 1);
            cJSON_AddStringToObject(key_def, "key_type", "PRIMARY");
        }

        if (key->type == Key::FOREIGN_KEY) {
            cJSON_AddStringToObject(key_def, "key_type", "FOREIGN_KEY");
        }

        if (key->type == Key::UNIQUE) {
            cJSON_AddStringToObject(key_def, "key_type", "UNIQUE");
        }

        if (key->type == Key::MULTIPLE) {
            cJSON_AddStringToObject(key_def, "key_type", "MULTIPLE");
        }

        if (key->type == Key::SPATIAL) {
            cJSON_AddStringToObject(key_def, "key_type", "SPATIAL");
        }

        if (key->type == Key::FULLTEXT) {
            cJSON_AddStringToObject(key_def, "key_type", "FULLTEXT");
        }

        key_column = cJSON_CreateArray();
        cJSON_AddItemToObject(key_def, "key_fields", key_column);
        for (int i = 1; (kk = k_it++); i++) {
            char *key_col_name = kk->field_name.str;
            column_item = cJSON_CreateString(key_col_name);
            cJSON_AddItemToArray(key_column, column_item);

            cJSON *field_info = cJSON_GetObjectItem(table_def, "ct_fields");
            for (int i = 0; i < cJSON_GetArraySize(field_info); i++) {
                cJSON *c_field = cJSON_GetArrayItem(field_info, i);
                char *c_field_name = cJSON_GetObjectItem(c_field, "field_name_str")->valuestring;
                if (strcmp(key_col_name, c_field_name) == 0) {
                    char *c_field_type = cJSON_GetObjectItem(c_field, "field_type")->valuestring;
                    long c_field_length = cJSON_GetObjectItem(c_field, "field_length")->valueint;
                    if (strcmp(c_field_type, "int") == 0) {
                        c_field_length = 4;
                    }
                    if (strcmp(c_field_type, "tinyint") == 0) {
                        c_field_length = 1;
                    }
                    if (strcmp(c_field_type, "smallint") == 0) {
                        c_field_length = 2;
                    }
                    if (strcmp(c_field_type, "mediumint") == 0) {
                        c_field_length = 3;
                    }
                    if (strcmp(c_field_type, "bigint") == 0) {
                        c_field_length = 8;
                    }
                    key_length += c_field_length;
                }
            }
        }
        cJSON_AddNumberToObject(key_def, "key_length", key_length);
        /*  #define HA_LEX_CREATE_TMP_TABLE	1
           #define HA_LEX_CREATE_IF_NOT_EXISTS 2
           #define HA_LEX_CREATE_TABLE_LIKE 4
           #define HA_OPTION_NO_CHECKSUM	(1L << 17)
           #define HA_OPTION_NO_DELAY_KEY_WRITE (1L << 18)
           #define HA_MAX_REC_LENGTH	65535U
        */
    }

    sql_parser_cleanup();

    return cJSON_Print(table_def);
}

FIELD_BASIC_INFO get_field_basic_info(
        int field_type,
        char *csname,
        long field_length,
        int field_decimal) {
    FIELD_BASIC_INFO i_field_basic_info_ptr;

    switch (field_type) {
        case 3: {
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("int");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 1: {
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("tinyint");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 9: {
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("mediumint");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 2: {
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("smallint");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 8: {
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("bigint");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 246: {
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("decimal");
            i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            if (field_decimal == 0) {
                i_field_basic_info_ptr.fbi_field_length = field_length - 1;
            } else {
                i_field_basic_info_ptr.fbi_field_length = field_length - 2;
            }
            break;
        }
        case 4:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("float");
            if (field_decimal == 31) {
                i_field_basic_info_ptr.fbi_field_length = NULL;
                i_field_basic_info_ptr.fbi_field_decimal = NULL;
            } else {
                i_field_basic_info_ptr.fbi_field_length = field_length;
                i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            }
            break;
        case 5:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("double");
            if (field_decimal == 31) {
                i_field_basic_info_ptr.fbi_field_length = NULL;
                i_field_basic_info_ptr.fbi_field_decimal = NULL;
            } else {
                i_field_basic_info_ptr.fbi_field_length = field_length;
                i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            }
            break;
        case 16:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("bit");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        case 14:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("date");
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        case 19:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("time");
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            break;
        case 18:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("datetime");
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            break;
        case 17:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("timestamp");
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            break;
        case 13:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("year");
            i_field_basic_info_ptr.fbi_field_length = 4;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        case 254: {
            if (strcmp(csname, "binary") == 0) {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("binary");
            } else {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("char");
            }
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 15: {
            if (strcmp(csname, "binary") == 0) {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("varbinary");
            } else {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("varchar");
            }
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 249: {
            if (strcmp(csname, "binary") == 0) {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("tinyblob");
            } else {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("tinytext");
            }
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 252: {
            if (strcmp(csname, "binary") == 0) {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("blob");
            } else {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("text");
            }
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 250: {
            if (strcmp(csname, "binary") == 0) {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("mediumblob");
            } else {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("mediumtext");
            }
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 251: {
            if (strcmp(csname, "binary") == 0) {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("longblob");
            } else {
                i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("longtext");
            }
            i_field_basic_info_ptr.fbi_field_length = NULL;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        }
        case 247:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("enum");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        case 248:
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("set");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = NULL;
            break;
        default:
//            i_field_basic_info_ptr->fbi_field_type_str = const_cast<char *>(std::to_string(field_type).c_str());
            i_field_basic_info_ptr.fbi_field_type_str = const_cast<char *>("NONE_TYPE");
            i_field_basic_info_ptr.fbi_field_length = field_length;
            i_field_basic_info_ptr.fbi_field_decimal = field_decimal;
            break;
    }
    return i_field_basic_info_ptr;
}

char *add_check_flag(const char *str, size_t len) {

    if (len == 0) return NULL;

//    char *pre = " , engine=stmt_check";
    char *pre;

    const char *str_without_space = trimwhitespace(str, len);

    char *dest = (char *)malloc(sizeof(char) * 5);

    strncpy(dest, str_without_space, 5);

    for(int i = 0; i < 5; i++) {
        if(*(dest + i)>=65 && *(dest+i)<=92) *(dest+i)+=32;
        else if (*(dest+i)>=97 && *(dest+i)<=122) *(dest+i)-=32;
    }

    if (strncmp(dest, "CREAT", 5) == 0 || strncmp(dest, "creat", 5)==0 ) {
        pre = " STATS_SAMPLE_PAGES=1989";
    }
    else {
        pre = " , STATS_SAMPLE_PAGES=1989";
    }


//    char * out = (char *) malloc((strlen(str_without_space) + strlen(pre)) * sizeof(char));
    char * out;
    size_t out_size = strlen(str_without_space);

    const char *end = str_without_space + out_size - 1;

    if (strcmp(end, ";") == 0) {
        out = (char *) malloc((strlen(str_without_space) + strlen(pre)) * sizeof(char));
        memcpy(out, str_without_space, strlen(str_without_space) - 1);
        out[strlen(str_without_space) - 1] = 0;
        strcat(out, pre);
    } else {
        out = (char *) malloc((strlen(str_without_space + 1) + strlen(pre)) * sizeof(char));
        memcpy(out, str_without_space, strlen(str_without_space));
        out[strlen(str_without_space)] = 0;
        strcat(out, pre);
    }
    return out;

//    printf("%s", str_without_space);
}

char *trimwhitespace(const char *str, size_t len) {
    if (len == 0)
        return 0;

    const char *end;
    size_t out_size;

    // Trim leading space
    while (isspace((unsigned char) *str)) str++;

    if (*str == 0)  // All spaces?
    {
        return NULL;
    }

    // Trim trailing space
    end = str + strlen(str) - 1;

    while (end > str && isspace((unsigned char) *end)) end--;
    end++;
//    printf("%ld\n", end-str);
//    printf("%ld\n", len - 1);
    out_size = (end - str) <= len - 1 ? (end - str) : len;
//    printf("%ld\n", out_size);
    char *out = (char *) malloc((out_size + 1) * sizeof(char));
    // Copy trimmed string and add null terminator
    memcpy(out, str, out_size);
    out[out_size] = 0;
    return out;
}

int main() {
    char *query = "create table a.td1\n"
                  "(\n"
                  "c1_int311 int(10) NOT NULL,"
                  "c2_int int default 11 comment 'test int without length',\n"
                  "c3_tinyint tinyint(2) default NULL comment 'test tinyint with length',\n"
                  "c4_tinyint tinyint not NULL comment 'tinyint comment with not null flag',\n"
                  "c5_mediumint mediumint(10) comment 'mediumint with length ',\n"
                  "c6_mediumint mediumint comment 'mediumint without length',\n"
                  "c7_smallint smallint default 123 comment 'smallint test',\n"
                  "c8_bigint bigint(19) default 10241024 comment 'bigint test',\n"
                  "c9_decimal decimal(11, 3) default 132 comment 'decimal with ',\n"
                  "c10_decimal decimal not null  comment 'decimal without precision and not null',\n"
                  "c11_decimal decimal(15, 13) comment 'decimal with default none',\n"
                  "c12_numeric numeric(10, 7) default 15.2 comment 'numeric with precision',\n"
                  "c13_numeric numeric default null comment 'numeric without precision and',\n"
                  "c14_float float(7, 4) default 18.65 comment 'float with precision ',\n"
                  "c15_float float not null comment 'float without precision and not null',\n"
                  "c16_double double(12, 3) not null comment 'double with precision',\n"
                  "c17_double double default 19.38 comment 'double without precision',\n"
                  "c18_bit bit default 0 comment 'bit with default value',\n"
                  "c19_bit bit(6) comment 'bit with precision and without default value',\n"
                  "c20_date date default 0 comment 'date type with 0 default',\n"
                  "c21_date date default '0000-00-00' comment 'date type with 6*0',\n"
                  "c22_date date default null comment 'date with null',\n"
                  "c23_date date not null comment 'date with not null',\n"
                  "c24_time time(3) default 0 comment 'time with default 0',\n"
                  "c25_time time null comment 'time with defualt null with nut null',\n"
                  "c26_datetime datetime default 0 comment 'datetime with default 0',\n"
                  "c27_datetime datetime(6) not null comment 'datetime with precision and not null',\n"
                  "c28_timestamp timestamp(6) default 0 comment 'datetime whti precision and default 0',\n"
                  "c29_timestamp timestamp not null comment 'datetime without precision and not null',\n"
                  "c30_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'datetime without precision and default ct',\n"
                  "c31_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'on update',\n"
                  "c32_year year not null default 0 comment 'year with default 0',\n"
                  "c33_year year(4) not null default 1991 comment 'year with precision 4',\n"
                  "c34_year year(5) not null default 1920 comment 'year whth precision > 4',\n"
                  "c35_year year(3) not null default 0 comment 'year with precision < 4',\n"
                  "c36_char char not null default 'a'  comment 'char with out length default a',\n"
                  "c37_char char(3) CHARACTER SET utf8 comment 'char with length without default value',\n"
                  "c38_varchar varchar(12) CHARACTER SET utf8 COLLATE utf8_bin default 'c38varchar'  comment 'varchar with default',\n"
                  "c39_varchar varchar(20) not null comment 'varchar with not',\n"
                  "c40_binary binary(3) not null comment 'binary with no default',\n"
                  "c41_binary binary default 0 comment 'binary with default',\n"
                  "c42_varbinary varbinary(10) default 152 comment 'varbinary with default',\n"
                  "c43_tinyblob tinyblob comment 'tinyblob with length',\n"
                  "c44_tinyblob tinyblob comment 'tinyblob without length',\n"
                  "c45_blob blob comment 'blob regular',\n"
                  "c46_mediumblob mediumblob comment 'regular medium blob',\n"
                  "c47_longblob longblob comment 'regular long blob',\n"
                  "c48_tinytext tinytext not null comment 'tinytext with notnull',\n"
                  "c49_text text not null comment 'text with not null',\n"
                  "c50_mediumtext mediumtext comment 'mediumtext without not null',\n"
                  "c51_longtext longtext comment 'longtext',\n"
                  "c52_enum enum('a','b','cccc','dddsss') DEFAULT 'a' comment 'enum with default',\n"
                  "c53_enum enum('a','b','cccc','dddsss') not null comment 'enum with not null',\n"
                  "c53_set set('a','b','c','d') default 'a' comment 'set with default',\n"
                  "c54_set set('a', 'b', 'c', 'd') not null comment 'set without default ',\n"
                  "PRIMARY KEY (`c1_int`,`c2_int`),\n"
                  "UNIQUE KEY `id` (`c8_bigint`),\n"
                  "key `i_c5_mediuminta` (c5_mediumint),\n"
                  "key `i_789_mediumint` (c39_varchar, c8_bigint, c9_decimal),\n"
                  "foreign key (c6_mediumint) references td1(c5_mediumint),\n"
                  "key `i_c9_decimal` (c9_decimal)\n"
                  ") engine=innodb  DEFAULT CHARSET=utf8 COLLATE=utf8_bin  auto_increment=10 comment='asdfasdfs'  ";

    char *query2 = const_cast<char *>("ALTER TABLE t1 RENAME t221231231231231231;");
    char *query3 = const_cast<char *>("ALTER TABLE t2 MODIFY a TINYINT NOT NULL comment 'asdfasd', CHANGE b c CHAR(20);");
    char *query4 = "   ALTER TABLE t2 MODIFY a TINYINT NOT NULL, add c CHAR(20), add a int ; ";
    char *query5 = const_cast<char *>("ALTER TABLE t2 modify a TIMESTAMP;");
    char *query6 = const_cast<char *>("ALTER TABLE t2 ADD INDEX (d), ADD UNIQUE (a);");
    char *query7 = const_cast<char *>("ALTER TABLE t2 DROP COLUMN c;");
    char *query8 = const_cast<char *>("ALTER TABLE t2 ADD c INT NOT NULL AUTO_INCREMENT, ADD INDEX i_c1 (d);");
    char *query9 = const_cast<char *>("create index i_c1 on t1(c1);");
    char *query10 = const_cast<char *>("create table c1 (C1 int)engine=innodb default charset=utf8 comment='select';  ");
    char * query11 =  const_cast<char *>("create table test(id int comment '123456')");
    char * query12 = "alter table `t1` rename `table_123_123456789_123456789_`;";

    char * query13 = const_cast<char *>("create table t1(a INT,b char(20)) engine=innodb ");
    char * query14 = "select sno, name from t alias_t \n"
                     "        where sno=(\n"
                     "                select sno+1 from my \n"
                     "                where \n"
                     "                name like \"%zhufeng%\" and \n"
                     "                sno > '10010' and \n"
                     "                name=alias_t.name\n"
                     "        ) \n"
                     "        order by name \n"
                     "        limit 100, 10;";


//
//    for(int i = 0; i< 250; i++)
//    {
//        char * result = thoth_sql(query10, "");
////        if (i % 10 == 0)
////        sleep(1);
//
//    }
    char * result = thoth_sql(query13, "");
//
    printf("%s\n", result);
//    thoth_sql_parse_alter(query4, const_cast<char *>("mysql"));
//    printf("%s", add_check_flag(query4, strlen(query4)));
//    char * query9 = (char *)malloc(strlen(query4));
//    rTrim("select ");'
//    trimwhitespace()
//    printf("%s\n", query4);
//    char * query_10 = trimwhitespace(query4, strlen(query4));
//    printf("%s", query_10);
    return 0;
}

