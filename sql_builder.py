#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 22 19:36:38 2025

@author: zxj
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
import json
import sqlparse
from sqlparse import sql, tokens
# ============= 第一部分：核心数据结构 =============

@dataclass
class TableConfig:
    """表配置"""
    table_name: str
    alias: str
    selected_fields: List[str] = field(default_factory=list)
    
    def add_field(self, field_name: str):
        """添加字段"""
        if field_name not in self.selected_fields:
            self.selected_fields.append(field_name)
    
    def get_qualified_fields(self) -> List[str]:
        """获取带表别名的字段列表"""
        return [f"{self.alias}.{f}" for f in self.selected_fields]

@dataclass
class JoinConfig:
    """JOIN配置"""
    left_table_alias: str
    right_table: TableConfig
    join_type: str  # "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"
    on_left_field: str
    on_right_field: str
    
    def to_sql(self) -> str:
        return (f"{self.join_type} {self.right_table.table_name} AS {self.right_table.alias} "
                f"ON {self.left_table_alias}.{self.on_left_field} = "
                f"{self.right_table.alias}.{self.on_right_field}")

class FilterOperator(Enum):
    """筛选操作符"""
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER = ">"
    LESS = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    REGEXP = "REGEXP"

@dataclass
class FilterCondition:
    """筛选条件"""
    table_alias: str
    field: str
    operator: FilterOperator
    value: Any = None
    logic_operator: str = "AND"  # "AND" or "OR"
    
    def to_sql(self) -> str:
        full_field = f"{self.table_alias}.{self.field}"
        
        if self.operator in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
            return f"{full_field} {self.operator.value}"
        
        if self.operator in [FilterOperator.IN, FilterOperator.NOT_IN]:
            if isinstance(self.value, (list, tuple)):
                values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in self.value])
            else:
                values = self.value
            return f"{full_field} {self.operator.value} ({values})"
        
        if self.operator == FilterOperator.BETWEEN:
            return f"{full_field} BETWEEN {self.value[0]} AND {self.value[1]}"
        
        if self.operator == FilterOperator.REGEXP:
            return f"{full_field} REGEXP '{self.value}'"
        
        # 默认情况
        value_str = f"'{self.value}'" if isinstance(self.value, str) else str(self.value)
        return f"{full_field} {self.operator.value} {value_str}"

@dataclass
class SortConfig:
    """排序配置"""
    table_alias: str
    field: str
    direction: str = "ASC"  # "ASC" or "DESC"
    
    def to_sql(self) -> str:
        return f"{self.table_alias}.{self.field} {self.direction}"

@dataclass
class WindowFunctionConfig:
    """窗口函数配置"""
    function_name: str  # "ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG", etc.
    table_alias: str
    field: str  # 要计算的字段（对于ROW_NUMBER等可以为空）
    partition_by: List[str] = field(default_factory=list)  # PARTITION BY字段
    order_by: List[SortConfig] = field(default_factory=list)  # ORDER BY配置
    alias: str = ""  # 结果列的别名
    
    def to_sql(self) -> str:
        func_expr = f"{self.function_name}("
        
        if self.field:
            func_expr += f"{self.table_alias}.{self.field}"
        
        func_expr += ")"
        
        window_clause = " OVER ("
        
        if self.partition_by:
            partition_fields = ", ".join(self.partition_by)
            window_clause += f"PARTITION BY {partition_fields} "
        
        if self.order_by:
            order_clauses = [sort.to_sql() for sort in self.order_by]
            window_clause += f"ORDER BY {', '.join(order_clauses)}"
        
        window_clause += ")"
        
        result = func_expr + window_clause
        
        if self.alias:
            result += f" AS {self.alias}"
        
        return result

@dataclass  
class CaseWhenConfig:
    """CASE WHEN配置"""
    alias: str
    conditions: List[tuple]  # [(FilterCondition, then_value), ...]
    else_value: Any = None
    
    def to_sql(self, indent: int = 2) -> str:
        spaces = " " * indent
        lines = [f"{spaces}CASE"]
        
        for condition, then_value in self.conditions:
            then_str = f"'{then_value}'" if isinstance(then_value, str) else str(then_value)
            lines.append(f"{spaces}  WHEN {condition.to_sql()} THEN {then_str}")
        
        if self.else_value is not None:
            else_str = f"'{self.else_value}'" if isinstance(self.else_value, str) else str(self.else_value)
            lines.append(f"{spaces}  ELSE {else_str}")
        
        lines.append(f"{spaces}END AS {self.alias}")
        return "\n".join(lines)

@dataclass
class GroupByConfig:
    """GROUP BY配置"""
    fields: List[str] = field(default_factory=list)  # 格式："table_alias.field"
    having_conditions: List[FilterCondition] = field(default_factory=list)

# ============= 第二部分：通用查询构建器 =============

class UniversalQueryBuilder:
    """通用SQL查询构建器 - 支持所有常见SQL操作"""
    
    def __init__(self):
        self.tables: List[TableConfig] = []
        self.joins: List[JoinConfig] = []
        self.filters: List[FilterCondition] = []
        self.case_when: List[CaseWhenConfig] = []
        self.window_functions: List[WindowFunctionConfig] = []
        self.group_by: Optional[GroupByConfig] = None
        self.order_by: List[SortConfig] = []
        self.limit: Optional[int] = None
        self.offset: Optional[int] = None
        self.distinct: bool = False
    
    def add_table(self, table_name: str, alias: str, fields: List[str] = None) -> TableConfig:
        """添加表"""
        table = TableConfig(table_name, alias, fields or [])
        self.tables.append(table)
        return table
    
    def add_join(self, left_alias: str, right_table: str, right_alias: str,
                 on_left: str, on_right: str, join_type: str = "LEFT JOIN",
                 right_fields: List[str] = None) -> JoinConfig:
        """添加JOIN"""
        right_table_config = TableConfig(right_table, right_alias, right_fields or [])
        self.tables.append(right_table_config)
        
        join = JoinConfig(left_alias, right_table_config, join_type, on_left, on_right)
        self.joins.append(join)
        return join
    
    def add_filter(self, table_alias: str, field: str, operator: FilterOperator,
                   value: Any = None, logic: str = "AND") -> FilterCondition:
        """添加筛选条件"""
        filter_cond = FilterCondition(table_alias, field, operator, value, logic)
        self.filters.append(filter_cond)
        return filter_cond
    
    def add_case_when(self, alias: str, conditions: List[tuple], else_value: Any = None):
        """添加CASE WHEN表达式"""
        case = CaseWhenConfig(alias, conditions, else_value)
        self.case_when.append(case)
        return case
    
    def add_window_function(self, function: str, table_alias: str, field: str,
                           partition_by: List[str] = None, order_by: List[SortConfig] = None,
                           alias: str = ""):
        """添加窗口函数"""
        window = WindowFunctionConfig(
            function, table_alias, field,
            partition_by or [], order_by or [], alias
        )
        self.window_functions.append(window)
        return window
    
    def set_group_by(self, fields: List[str], having: List[FilterCondition] = None):
        """设置GROUP BY"""
        self.group_by = GroupByConfig(fields, having or [])
    
    def add_order_by(self, table_alias: str, field: str, direction: str = "ASC"):
        """添加ORDER BY"""
        sort = SortConfig(table_alias, field, direction)
        self.order_by.append(sort)
        return sort
    
    def set_limit(self, limit: int, offset: int = None):
        """设置LIMIT"""
        self.limit = limit
        self.offset = offset
    
    def to_sql(self) -> str:
        """生成完整SQL"""
        lines = []
        
        # SELECT子句
        select_keyword = "SELECT DISTINCT" if self.distinct else "SELECT"
        lines.append(select_keyword)
        
        # 收集所有SELECT项
        select_items = []
        
        # 普通字段
        for table in self.tables:
            select_items.extend(table.get_qualified_fields())
        
        # CASE WHEN
        for case in self.case_when:
            select_items.append(case.to_sql())
        
        # 窗口函数
        for window in self.window_functions:
            select_items.append("  " + window.to_sql())
        
        lines.append("  " + ",\n  ".join(select_items))
        
        # FROM子句
        if self.tables:
            main_table = self.tables[0]
            lines.append(f"FROM {main_table.table_name} AS {main_table.alias}")
        
        # JOIN子句
        for join in self.joins:
            lines.append(join.to_sql())
        
        # WHERE子句
        if self.filters:
            lines.append("WHERE")
            filter_sqls = []
            for i, f in enumerate(self.filters):
                if i == 0:
                    filter_sqls.append(f"  {f.to_sql()}")
                else:
                    filter_sqls.append(f"  {f.logic_operator} {f.to_sql()}")
            lines.append("\n".join(filter_sqls))
        
        # GROUP BY子句
        if self.group_by:
            lines.append(f"GROUP BY {', '.join(self.group_by.fields)}")
            
            if self.group_by.having_conditions:
                having_sqls = [h.to_sql() for h in self.group_by.having_conditions]
                lines.append(f"HAVING {' AND '.join(having_sqls)}")
        
        # ORDER BY子句
        if self.order_by:
            order_sqls = [sort.to_sql() for sort in self.order_by]
            lines.append(f"ORDER BY {', '.join(order_sqls)}")
        
        # LIMIT子句
        if self.limit:
            limit_clause = f"LIMIT {self.limit}"
            if self.offset:
                limit_clause += f" OFFSET {self.offset}"
            lines.append(limit_clause)
        
        return "\n".join(lines) + ";"

    def validate_sql(self, sql_text: str = None) -> dict:
            """
            验证SQL语法
            返回: {
                "valid": bool,
                "formatted": str,  # 格式化后的SQL
                "errors": list,    # 错误列表（如果有）
                "warnings": list   # 警告列表
            }
            """
            if sql_text is None:
                sql_text = self.to_sql()
            
            result = {
                "valid": True,
                "formatted": "",
                "errors": [],
                "warnings": []
            }
            
            try:
                # 解析SQL
                parsed = sqlparse.parse(sql_text)
                
                if not parsed:
                    result["valid"] = False
                    result["errors"].append("无法解析SQL语句")
                    return result
                
                # 格式化SQL（美化输出）
                result["formatted"] = sqlparse.format(
                    sql_text,
                    reindent=True,
                    keyword_case='upper',
                    indent_width=2
                )
                
                # 基本语法检查
                statement = parsed[0]
                
                # 检查是否是SELECT语句
                if statement.get_type() != 'SELECT':
                    result["warnings"].append(f"检测到非SELECT语句: {statement.get_type()}")
                
                # 检查括号匹配
                token_list = list(statement.flatten())
                paren_count = 0
                for token in token_list:
                    if token.match(tokens.Punctuation, '('):
                        paren_count += 1
                    elif token.match(tokens.Punctuation, ')'):
                        paren_count -= 1
                    if paren_count < 0:
                        result["valid"] = False
                        result["errors"].append("括号不匹配")
                        break
                
                if paren_count != 0:
                    result["valid"] = False
                    result["errors"].append("括号不匹配")
                
                # 检查常见错误
                sql_lower = sql_text.lower()
                
                # 检查是否有未闭合的引号
                single_quotes = sql_text.count("'")
                if single_quotes % 2 != 0:
                    result["warnings"].append("可能存在未闭合的单引号")
                
                # 检查SELECT *（可选的代码规范检查）
                if "select *" in sql_lower or "select  *" in sql_lower:
                    result["warnings"].append("使用了SELECT *，建议明确指定字段")
                
            except Exception as e:
                result["valid"] = False
                result["errors"].append(f"解析错误: {str(e)}")
            
            return result
   def to_natural_language(self) -> str:
        """将SQL配置转换为自然语言描述"""
        parts = []
        
        # 1. 基本查询意图
        if self.distinct:
            parts.append("查询去重后的数据")
        else:
            parts.append("查询数据")
        
        # 2. 主表
        if self.tables:
            main_table = self.tables[0]
            parts.append(f"，从 **{main_table.table_name}** 表")
            if main_table.selected_fields:
                fields_str = "、".join(main_table.selected_fields)
                parts.append(f"（字段：{fields_str}）")
        
        # 3. JOIN关系
        if self.joins:
            join_parts = []
            for join in self.joins:
                join_type_cn = {
                    "LEFT JOIN": "左连接",
                    "INNER JOIN": "内连接",
                    "RIGHT JOIN": "右连接",
                    "FULL OUTER JOIN": "全外连接"
                }.get(join.join_type, join.join_type)
                
                join_parts.append(
                    f"{join_type_cn} **{join.right_table.table_name}** 表"
                    f"（ON {join.left_table_alias}.{join.on_left_field} = {join.right_table.alias}.{join.on_right_field}）"
                )
            parts.append("，" + "，".join(join_parts))
        
        # 4. 筛选条件
        if self.filters:
            parts.append("。\n\n**筛选条件**：")
            filter_parts = []
            for i, f in enumerate(self.filters):
                op_cn = {
                    "=": "等于",
                    "!=": "不等于",
                    ">": "大于",
                    "<": "小于",
                    ">=": "大于等于",
                    "<=": "小于等于",
                    "IN": "在...之中",
                    "NOT IN": "不在...之中",
                    "LIKE": "包含",
                    "NOT LIKE": "不包含",
                    "IS NULL": "为空",
                    "IS NOT NULL": "不为空",
                    "BETWEEN": "在...之间",
                    "REGEXP": "匹配正则"
                }.get(f.operator.value, f.operator.value)
                
                logic = "" if i == 0 else f" **{f.logic_operator}** "
                
                # 格式化值
                if isinstance(f.value, list):
                    value_str = f"[{', '.join(map(str, f.value))}]"
                elif f.value is None:
                    value_str = ""
                else:
                    value_str = f" {f.value}"
                
                filter_parts.append(f"{logic}{f.table_alias}.{f.field} {op_cn}{value_str}")
            
            parts.append("\n- " + "\n- ".join(filter_parts))
        
        # 5. GROUP BY
        if self.group_by:
            parts.append(f"\n\n**分组**：按 {', '.join(self.group_by)} 分组")
            if hasattr(self, 'having_conditions') and self.having_conditions:
                parts.append("，并应用HAVING条件")
        
        # 6. CASE WHEN
        if self.case_when:
            parts.append("\n\n**条件字段**：")
            for case in self.case_when:
                parts.append(f"\n- {case.alias}（{len(case.conditions)}个条件分支）")
        
        # 7. 窗口函数
        if self.window_functions:
            parts.append("\n\n**窗口函数**：")
            for wf in self.window_functions:
                parts.append(f"\n- {wf.alias}：{wf.function_name}")
                if hasattr(wf, 'partition_by') and wf.partition_by:
                    parts.append(f" PARTITION BY {', '.join(wf.partition_by)}")
        
        # 8. 排序
        if self.order_by:
            order_parts = []
            for sort in self.order_by:
                direction_cn = "升序" if sort.direction == "ASC" else "降序"
                order_parts.append(f"{sort.table_alias}.{sort.field} {direction_cn}")
            parts.append(f"\n\n**排序**：按 {', '.join(order_parts)}")
        
        # 9. LIMIT
        if self.limit:
            limit_text = f"\n\n**限制**：返回 {self.limit} 条记录"
            if self.offset:
                limit_text += f"（跳过前 {self.offset} 条）"
            parts.append(limit_text)
        
        result = "".join(parts)
        if not result.endswith("。"):
            result += "。"
        
        return result
# ============= 第四部分：配置序列化（可以保存/加载配置）=============

def save_query_config(builder: UniversalQueryBuilder, filename: str):
    """将查询配置保存为JSON"""
    config = {
        "tables": [
            {"table_name": t.table_name, "alias": t.alias, "fields": t.selected_fields}
            for t in builder.tables
        ],
        "joins": [
            {
                "left_alias": j.left_table_alias,
                "right_table": j.right_table.table_name,
                "right_alias": j.right_table.alias,
                "join_type": j.join_type,
                "on_left": j.on_left_field,
                "on_right": j.on_right_field
            }
            for j in builder.joins
        ],
        # 可以继续添加其他配置...
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
