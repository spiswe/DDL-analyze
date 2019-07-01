#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Author : spisw
    Date   : 2018/12/12
    File   : ddl_audit
"""
from src.engines.base import BaseEngine
from src.lib.static import ErrorCode
import os


class DDLAudit(BaseEngine):
    """
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        # self.program = "/data/Documents/git/HCA_SQL_Parser/SQLParser/cmake-build-debug/libHCA_SQL_PARSER.so"
        # self.program = "/".join(os.path.dirname(os.path.abspath(__file__)).split("/")[:-1]) + "/engines/program/libHCA_SQL_PARSER.so"
        # print(self.program)
        super(DDLAudit, self).__init__()

    def _moudle_register(self):
        return [ErrorCode, BaseEngine]

    def run(self, *args, **kwargs):
        import json
        import subprocess
        """
        func: thoth_sql_parse_create_table
        :param args:
        :param kwargs:
        :return:
        """
        # symbol
        gt = lambda x, y: 1 if float(x) > float(y) else 0
        lt = lambda x, y: 1 if float(x) < float(y) else 0
        eq = lambda x, y: 1 if str(x).upper() == str(y).upper() else 0
        neq = lambda x, y: 1 if str(x).upper() != str(y).upper() else 0
        FAILURE = -1
        WARNING = 0
        PASS = 1

        ct_rules = dict(
            r_statistics=[dict(r_subject_key="alter_table_once", r_object="1", r_predicate="gt", rr=FAILURE, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="You should merge the alter statement to one from same table first.", )],
            r_table=[dict(r_subject_key="table_name_length", r_object=30, r_predicate="lt", rr=PASS, switch=1,
                          premise_key="", premise_predicate="eq", premise_value="",
                          desc="Table name length must be less than 30 characters"), ],

            r_properties=[dict(r_subject_key="engine", r_object="InnoDB", r_predicate="eq", rr=PASS, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Table engine must be InnoDB"),
                          dict(r_subject_key="charset", r_object="utf8", r_predicate="eq", rr=PASS, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Table charset must be utf8"),
                          dict(r_subject_key="collate", r_object="utf8_bin", r_predicate="eq", rr=PASS, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Table collate must be utf8_bin"),
                          dict(r_subject_key="auto_increment", r_object=1, r_predicate="eq", rr=PASS, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Auto increment must be 1"),
                          dict(r_subject_key="comment_length", r_object=0, r_predicate="eq", rr=FAILURE, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Table must have comment"),
                          dict(r_subject_key="primary_key_flag", r_object="1", r_predicate="eq", rr=PASS, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Table must have primary key"),
                          dict(r_subject_key="partition_flag", r_object="0", r_predicate="eq", rr=PASS, switch=1,
                               premise_key="", premise_predicate="eq", premise_value="",
                               desc="Table can't be partitioned"),
                          ],
            r_key=[
                dict(r_subject_key="key_name_length", r_object=30, r_predicate="lt", rr=PASS, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="Key name length must be less than 20 characters"),
                dict(r_subject_key="key_field_count", r_object=4, r_predicate="gt", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="key field count must be less than 4"),
                dict(r_subject_key="key_length", r_object=766, r_predicate="gt", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="key field length must be less than 766"),
                dict(r_subject_key="key_type", r_object="FOREIGN_KEY", r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="Do not use foreign key"),
                dict(r_subject_key="key_name_length", r_object=0, r_predicate="gt", rr=PASS, switch=1,
                     premise_key="key_type", premise_predicate="eq", premise_value="MULTIPLE",
                     desc="MULTIPLE index must set key name"),
                dict(r_subject_key="key_name_length", r_object=0, r_predicate="gt", rr=PASS, switch=1,
                     premise_key="key_type", premise_predicate="eq", premise_value="FOREIGN_KEY",
                     desc="Foreign key must set key name"),
            ],
            r_field=[
                # field name length check
                dict(r_subject_key="field_name_length", r_object=30, r_predicate="lt", rr=PASS, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="Field name length must be less than 20 characters"),
                # field comment check
                dict(r_subject_key="field_comment_length", r_object=0, r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="Field must have comment"),
                # char length check
                dict(r_subject_key="field_length", r_object=4, r_predicate="gt", rr=WARNING, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="char",
                     desc="If field type is Char and length is greater than 4, you'd better use varchar"),
                # text not null flag check
                dict(r_subject_key="field_is_nullable", r_object=0, r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="text",
                     desc="If field type is text, NOT_NULL flag is not permitted"),
                dict(r_subject_key="field_is_nullable", r_object=0, r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="tinytext",
                     desc="If field type is tinytext, NOT_NULL flag is not permitted"),
                dict(r_subject_key="field_is_nullable", r_object=0, r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="mediumtext",
                     desc="If field type is mediumtext, NOT_NULL flag is not permitted"),
                dict(r_subject_key="field_is_nullable", r_object=0, r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="longtext",
                     desc="If field type is longtext, NOT_NULL flag is not permitted"),
                # field must have no charset or collate
                dict(r_subject_key="field_charset", r_object="", r_predicate="neq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="char",
                     desc="If field type is char, field charset is not permitted"),
                dict(r_subject_key="field_collate", r_object="", r_predicate="neq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="char",
                     desc="If field type is char, field collate is not permitted"),
                dict(r_subject_key="field_charset", r_object="", r_predicate="neq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="varchar",
                     desc="If field type is varchar, field charset is not permitted"),
                dict(r_subject_key="field_collate", r_object="", r_predicate="neq", rr=FAILURE, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="varchar",
                     desc="If field type is varchar, field collate is not permitted"),
                # must use decimal
                dict(r_subject_key="field_type", r_object="decimal", r_predicate="eq", rr=PASS, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="float",
                     desc="Float/double type must use decimal instead"),
                dict(r_subject_key="field_type", r_object="decimal", r_predicate="eq", rr=PASS, switch=1,
                     premise_key="field_type", premise_predicate="eq", premise_value="double",
                     desc="Float/double type must use decimal instead"),
                # must use char(1) for flag field
                dict(r_subject_key="field_type", r_object="char", r_predicate="neq", rr=WARNING, switch=1,
                     premise_key="field_length", premise_predicate="eq", premise_value=1,
                     desc="Using Char type is better for flag field"
                     ),
                # must not use bool, set, enum
                dict(r_subject_key="field_type", r_object="bool", r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="Bool type is not allowed"
                     ),
                dict(r_subject_key="field_type", r_object="set", r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="set type is not allowed"
                     ),
                dict(r_subject_key="field_type", r_object="enum", r_predicate="eq", rr=FAILURE, switch=1,
                     premise_key="", premise_predicate="eq", premise_value="",
                     desc="enum type is not allowed"
                     ),
            ]
        )

        result = []
        alter_source = []

        if kwargs.get("ct_rules"):
            ct_rules = kwargs.get("ct_rules")

        # _query_list = kwargs.get("sql")
        # print(_query)
        # _dbname = kwargs.get("dbname")
        addinfo = kwargs.get("addinfo")

        if os.environ.get("_"):
            env_python = os.environ.get("_")
        elif os.environ.get("__PYVENV_LAUNCHER__"):
            env_python = os.environ.get("__PYVENV_LAUNCHER__")
        else:
            env_python = "python3"

        # p = subprocess.Popen(
        #     ["python3", '/data/Documents/git/HotDB_Cloud_Analysis/src/packages/thoth_parser_dll.py',
        #      addinfo.get("stype"), addinfo.get("shost"), str(addinfo.get("sport")), addinfo.get("spwd"),
        #      str(addinfo.get("sdb")), addinfo.get("taskid")],
        #     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        p = subprocess.Popen(
            [env_python, "/".join(os.path.dirname(os.path.abspath(__file__)).split("/")[:-1]) + '/thoth_parser_dll.py',
             addinfo.get("stype"), addinfo.get("shost"), str(addinfo.get("sport")), addinfo.get("spwd"),
             str(addinfo.get("sdb")), addinfo.get("taskid")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        ddl_result_list = p.stdout.read()
        ddl_result_list = ddl_result_list.decode("utf8")
        # raise Exception(ddl_result_list)
        ddl_result_list = json.loads(ddl_result_list)
        # raise Exception(ddl_result_list)

        for ddl_result in ddl_result_list:
            _result = []

            if ddl_result.get("sql_parse_error"):
                result.append(ddl_result)
                # raise Exception(result)
                continue

            # print(ddl_result)
            if ddl_result.get("sql_type") == "SQLCOM_ALTER_TABLE":
                alter_source.append(
                    ddl_result.get("db_name") + "." + ddl_result.get("table_name_str") if ddl_result.get(
                        "db_name") else ddl_result.get("table_name_str"))

            if "r_table" in ct_rules and \
                    (ddl_result.get("sql_type") == "SQLCOM_CREATE_TABLE" or ddl_result.get("table_rename_flag") == 1):
                for rule in ct_rules.get("r_table"):
                    # switch is 1, the rule is enabled
                    if not rule.get("switch") == 1:
                        continue
                    # first get and cal the premise condition
                    if rule.get("premise_key"):
                        premise_predicate = locals()[rule.get("premise_predicate")]
                        premise_subject = ddl_result.get(rule.get("premise_key"))
                        begin_flag = premise_predicate(premise_subject, rule.get("premise_value"))
                        if not begin_flag:
                            continue
                    predicate = locals()[rule.get("r_predicate")]
                    r_subject = ddl_result.get(rule.get("r_subject_key"))
                    re = predicate(r_subject, rule.get("r_object"))
                    _desc = dict(desc=rule.get("desc"),
                                 reality=r_subject,
                                 table_source=ddl_result.get("db_name") + "." + ddl_result.get(
                                     "table_name_str") if ddl_result.get("db_name") else ddl_result.get(
                                     "table_name_str"))
                    if rule.get("rr") == PASS:
                        if re:
                            continue
                        else:
                            _desc["level"] = "FAILURE"
                            _result.append(_desc)
                    elif rule.get("rr") == WARNING:
                        if re:
                            _desc["level"] = "WARNING"
                            _result.append(_desc)
                        else:
                            continue
                    else:
                        if re:
                            _desc["level"] = "FAILURE"
                            _result.append(_desc)

            # if "r_properties" in ct_rules and ddl_result.get("sql_type") == "SQLCOM_CREATE_TABLE":
            if "r_properties" in ct_rules:
                for rule in ct_rules.get("r_properties"):
                    # switch is 1, the rule is enabled
                    if not rule.get("switch") == 1:
                        continue
                    if rule.get("premise_key"):
                        premise_predicate = locals()[rule.get("premise_predicate")]
                        premise_subject = ddl_result.get("ct_properties").get(rule.get("premise_key"))
                        begin_flag = premise_predicate(premise_subject, rule.get("premise_value"))
                        if not begin_flag:
                            continue
                    if ddl_result.get("sql_type") == "SQLCOM_ALTER_TABLE" and \
                            not ddl_result.get("ct_properties").get(rule.get("r_subject_key")):
                        continue
                    predicate = locals()[rule.get("r_predicate")]
                    r_subject = ddl_result.get("ct_properties").get(rule.get("r_subject_key"))
                    re = predicate(r_subject, rule.get("r_object"))
                    _desc = dict(desc=rule.get("desc"),
                                 reality=r_subject,
                                 table_source=ddl_result.get("db_name") + "." + ddl_result.get(
                                     "table_name_str") if ddl_result.get("db_name") else ddl_result.get(
                                     "table_name_str"))
                    if rule.get("rr") == PASS:
                        if re:
                            continue
                        else:
                            _desc["level"] = "FAILURE"
                            _result.append(_desc)
                    elif rule.get("rr") == WARNING:
                        if re:
                            _desc["level"] = "WARNING"
                            _result.append(_desc)
                        else:
                            continue
                    else:
                        if re:
                            _desc["level"] = "FAILURE"
                            _result.append(_desc)

            if "r_field" in ct_rules:
                for field in ddl_result.get("ct_fields"):
                    for rule in ct_rules.get("r_field"):
                        # switch
                        if not rule.get("switch") == 1:
                            continue
                        if rule.get("premise_key"):
                            premise_predicate = locals()[rule.get("premise_predicate")]
                            premise_subject = field.get(rule.get("premise_key"))
                            begin_flag = premise_predicate(premise_subject, rule.get("premise_value"))
                            if not begin_flag:
                                continue
                        predicate = locals()[rule.get("r_predicate")]
                        r_subject = field.get(rule.get("r_subject_key"))
                        re = predicate(r_subject, rule.get("r_object"))
                        _desc = dict(desc=rule.get("desc"),
                                     reality=r_subject,
                                     table_source=ddl_result.get("db_name") + "." + ddl_result.get(
                                         "table_name_str") if ddl_result.get("db_name") else ddl_result.get(
                                         "table_name_str"),
                                     field_name=field.get("field_name_str"))
                        if rule.get("rr") == PASS:
                            if re:
                                continue
                            else:
                                _desc["level"] = "FAILURE"
                                _result.append(_desc)
                        elif rule.get("rr") == WARNING:
                            if re:
                                _desc["level"] = "WARNING"
                                _result.append(_desc)
                            else:
                                continue
                        else:
                            if re:
                                _desc["level"] = "FAILURE"
                                _result.append(_desc)

            if "r_key" in ct_rules:
                for ckey in ddl_result.get("ct_keys"):
                    for rule in ct_rules.get("r_key"):
                        # switch
                        if not rule.get("switch") == 1:
                            continue
                        if rule.get("premise_key"):
                            premise_predicate = locals()[rule.get("premise_predicate")]
                            premise_subject = ckey.get(rule.get("premise_key"))
                            begin_flag = premise_predicate(premise_subject, rule.get("premise_value"))
                            if not begin_flag:
                                continue
                        predicate = locals()[rule.get("r_predicate")]
                        r_subject = ckey.get(rule.get("r_subject_key"))
                        re = predicate(r_subject, rule.get("r_object"))
                        _desc = dict(desc=rule.get("desc"),
                                     reality=r_subject,
                                     table_source=ddl_result.get("db_name") + "." + ddl_result.get(
                                         "table_name_str") if ddl_result.get("db_name") else ddl_result.get(
                                         "table_name_str"),
                                     )
                        if rule.get("rr") == PASS:
                            if re:
                                continue
                            else:
                                _desc["level"] = "FAILURE"
                                _result.append(_desc)
                        elif rule.get("rr") == WARNING:
                            if re:
                                _desc["level"] = "WARNING"
                                _result.append(_desc)
                            else:
                                continue
                        else:
                            if re:
                                _desc["level"] = "FAILURE"
                                _result.append(_desc)
            result.append(_result)
            # for re in result:
            #     print(re)
        if "r_statistics" in ct_rules:
            _result = []
            for rule in ct_rules.get("r_statistics"):
                if not rule.get("switch") == 1:
                    continue
                if rule.get("r_subject_key") == "alter_table_once":
                    alter_source_set = set(alter_source)
                    for ass in alter_source_set:
                        if alter_source.count(ass) > 1:
                            _desc = dict(desc=rule.get("desc"),
                                         reality=alter_source.count(ass),
                                         table_source=ass)
                            _result.append(_desc)
                            # raise Exception("You should merge the alter statement to one from same table first.")
            result.append(_result)
        return ErrorCode.success, result


def main():
    pass


if __name__ == "__main__":
    main()
