#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 22 19:40:24 2025

@author: zxj
"""

# app.py - SQL查询构建器GUI界面

import streamlit as st
import sqlparse
from typing import List, Dict
import json
import os
from pathlib import Path
# 导入之前写的UniversalQueryBuilder和相关类
# （把之前的代码保存为 sql_builder.py，然后导入）
from sql_builder import (
    UniversalQueryBuilder, 
    FilterOperator, 
    SortConfig,
    FilterCondition
)

# ============= 页面配置 =============
st.set_page_config(
    page_title="SQL查询构建器",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============= Session State初始化 =============
if 'builder' not in st.session_state:
    st.session_state.builder = UniversalQueryBuilder()

if 'table_counter' not in st.session_state:
    st.session_state.table_counter = 0

if 'tables' not in st.session_state:
    st.session_state.tables = []

if 'joins' not in st.session_state:
    st.session_state.joins = []

if 'filters' not in st.session_state:
    st.session_state.filters = []

if 'case_whens' not in st.session_state:
    st.session_state.case_whens = []

if 'order_bys' not in st.session_state:
    st.session_state.order_bys = []

if 'distinct' not in st.session_state:
    st.session_state.distinct = False

if 'limit_config' not in st.session_state:
    st.session_state.limit_config = {'limit': 0, 'offset': 0}

if 'config_backup' not in st.session_state:
    st.session_state.config_backup = None

if 'has_loaded_example' not in st.session_state:
    st.session_state.has_loaded_example = False
# ============= 辅助函数 =============
def rebuild_query():
    """根据session state重建查询"""
    builder = UniversalQueryBuilder()
    
    # 添加表
    for table_data in st.session_state.tables:
        builder.add_table(
            table_data['name'],
            table_data['alias'],
            table_data['fields']
        )
    
    # 添加JOIN
    for join_data in st.session_state.joins:
        builder.add_join(
            join_data['left_alias'],
            join_data['right_table'],
            join_data['right_alias'],
            join_data['on_left'],
            join_data['on_right'],
            join_data['join_type'],
            join_data['right_fields']
        )
    
    # 添加筛选条件
    for filter_data in st.session_state.filters:
        builder.add_filter(
            filter_data['table_alias'],
            filter_data['field'],
            FilterOperator[filter_data['operator']],
            filter_data['value'],
            filter_data['logic']
        )
    # 添加GROUP BY
    if 'group_by' in st.session_state and st.session_state.group_by:
        from sql_builder import GroupByConfig
        group_data = st.session_state.group_by
        
        having_conditions = []
        if group_data.get('having'):
            having = group_data['having']
            having_cond = FilterCondition(
                having['table'],
                having['field'],
                FilterOperator[having['operator']],
                having['value']
            )
            having_conditions.append(having_cond)
        
        builder.set_group_by(group_data['fields'], having_conditions)
    
    # 添加窗口函数
    if 'window_functions' in st.session_state:
        from sql_builder import WindowFunctionConfig
        for wf_data in st.session_state.window_functions:
            order_by_list = []
            for order in wf_data['order_by']:
                order_by_list.append(SortConfig(
                    order['table'],
                    order['field'],
                    order['direction']
                ))
            
            builder.add_window_function(
                wf_data['function'],
                wf_data['table'],
                wf_data['field'],
                wf_data['partition_by'],
                order_by_list,
                wf_data['alias']
            )
    # 添加CASE WHEN
    for case_data in st.session_state.case_whens:
        from sql_builder import CaseWhenConfig
        case_when = CaseWhenConfig(
            case_data['alias'],
            case_data['conditions'],
            case_data['else_value']
        )
        builder.case_when.append(case_when)       
    # 添加排序
    for order_data in st.session_state.order_bys:
        builder.add_order_by(
            order_data['table_alias'],
            order_data['field'],
            order_data['direction']
        )
    
    # 添加DISTINCT
    if st.session_state.distinct:
        builder.distinct = True
    
    # 添加LIMIT
    if st.session_state.limit_config['limit'] > 0:
        builder.set_limit(
            st.session_state.limit_config['limit'],
            st.session_state.limit_config['offset'] if st.session_state.limit_config['offset'] > 0 else None
        )
    
    st.session_state.builder = builder
    return builder
# ============= 模板管理函数 =============

def get_templates_dir():
    """获取模板存储目录"""
    templates_dir = Path.home() / ".sql_builder_templates"
    templates_dir.mkdir(exist_ok=True)
    return templates_dir

def save_template(name: str) -> bool:
    """保存当前配置为模板"""
    try:
        template = {
            'name': name,
            'tables': st.session_state.tables.copy(),
            'joins': st.session_state.joins.copy(),
            'filters': st.session_state.filters.copy(),
            'case_whens': st.session_state.case_whens.copy(),
            'order_bys': st.session_state.order_bys.copy(),
            'distinct': st.session_state.distinct,
            'limit_config': st.session_state.limit_config.copy(),
            'group_by': st.session_state.get('group_by', {}).copy() if st.session_state.get('group_by') else {},
            'window_functions': st.session_state.get('window_functions', []).copy() if st.session_state.get('window_functions') else []
        }
        
        # 保存到文件
        templates_dir = get_templates_dir()
        # 使用安全的文件名
        safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).strip()
        template_file = templates_dir / f"{safe_name}.json"
        
        with open(template_file, 'w', encoding='utf-8') as f:
            json.dump(template, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        st.error(f"保存模板失败: {str(e)}")
        return False

def load_template(template_name: str) -> bool:
    """加载模板"""
    try:
        templates_dir = get_templates_dir()
        safe_name = "".join(c for c in template_name if c.isalnum() or c in (' ', '-', '_')).strip()
        template_file = templates_dir / f"{safe_name}.json"
        
        if not template_file.exists():
            st.error("模板文件不存在")
            return False
        
        with open(template_file, 'r', encoding='utf-8') as f:
            template = json.load(f)
        
        # 恢复配置
        st.session_state.tables = template.get('tables', [])
        st.session_state.joins = template.get('joins', [])
        st.session_state.filters = template.get('filters', [])
        st.session_state.case_whens = template.get('case_whens', [])
        st.session_state.order_bys = template.get('order_bys', [])
        st.session_state.distinct = template.get('distinct', False)
        st.session_state.limit_config = template.get('limit_config', {'limit': 0, 'offset': 0})
        st.session_state.group_by = template.get('group_by', {})
        st.session_state.window_functions = template.get('window_functions', [])
        
        rebuild_query()
        return True
    except Exception as e:
        st.error(f"加载模板失败: {str(e)}")
        return False

def get_all_templates() -> list:
    """获取所有已保存的模板"""
    templates_dir = get_templates_dir()
    templates = []
    
    for template_file in templates_dir.glob("*.json"):
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                template = json.load(f)
                templates.append({
                    'name': template.get('name', template_file.stem),
                    'file': template_file
                })
        except:
            continue
    
    return templates

def delete_template(template_name: str) -> bool:
    """删除模板"""
    try:
        templates_dir = get_templates_dir()
        safe_name = "".join(c for c in template_name if c.isalnum() or c in (' ', '-', '_')).strip()
        template_file = templates_dir / f"{safe_name}.json"
        
        if template_file.exists():
            template_file.unlink()
            return True
        return False
    except Exception as e:
        st.error(f"删除模板失败: {str(e)}")
        return False
# ============= 主界面 =============
st.title("🔍 SQL查询构建器")
st.markdown("---")

# 创建两列布局
col_config, col_preview = st.columns([1, 1])

# ============= 左侧：配置区 =============
with col_config:
    st.header("📝 查询配置")
    
    # Tab布局
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 表和字段", 
        "🔗 JOIN关系", 
        "🔍 筛选条件",
        "📈 CASE WHEN",
        "📊 GROUP BY",
        "📈 窗口函数",
        "⚙️ 其他选项"
    ])
    
   # ===== Tab 1: 表和字段 =====
    with tab1:
        st.subheader("添加表")
        
        # 使用counter确保表单唯一
        if 'form_counter' not in st.session_state:
            st.session_state.form_counter = 0
        
        form_key = st.session_state.form_counter
        
        with st.form(f"add_table_form_{form_key}"):
            col1, col2 = st.columns(2)
            with col1:
                table_name = st.text_input("表名", placeholder="例如: products", key=f"table_name_{form_key}")
            with col2:
                table_alias = st.text_input("别名", placeholder="例如: p", key=f"table_alias_{form_key}")
            
            fields_input = st.text_area(
                "字段（每行一个）",
                placeholder="product_id\nproduct_name\nprice",
                height=100,
                key=f"fields_input_{form_key}"
            )
            
            if st.form_submit_button("➕ 添加表", use_container_width=True):
                if table_name and table_alias:
                    fields = [f.strip() for f in fields_input.split('\n') if f.strip()]
                    st.session_state.tables.append({
                        'name': table_name,
                        'alias': table_alias,
                        'fields': fields
                    })
                    st.session_state.form_counter += 1
                    rebuild_query()
                    st.success(f"✓ 已添加表 {table_name} ({table_alias})")
                    st.rerun()
        
        # 显示已添加的表
        if st.session_state.tables:
            st.markdown("---")
            st.subheader("已添加的表")
            for i, table in enumerate(st.session_state.tables):
                with st.expander(f"{table['name']} (别名: {table['alias']})"):
                    st.write(f"**字段**: {', '.join(table['fields']) if table['fields'] else '无'}")
                    if st.button(f"🗑️ 删除", key=f"del_table_{i}"):
                        st.session_state.tables.pop(i)
                        rebuild_query()
                        st.rerun()
    
    # ===== Tab 2: JOIN关系 =====
    with tab2:
        if len(st.session_state.tables) < 2:
            st.info("💡 至少需要2个表才能添加JOIN")
        else:
            st.subheader("添加JOIN")
            
            with st.form("add_join_form"):
                # 选择左表
                left_aliases = [t['alias'] for t in st.session_state.tables]
                left_alias = st.selectbox("左表别名", left_aliases)
                
                # 输入右表信息
                col1, col2 = st.columns(2)
                with col1:
                    right_table = st.text_input("右表名", placeholder="例如: categories")
                    right_alias = st.text_input("右表别名", placeholder="例如: c")
                with col2:
                    join_type = st.selectbox(
                        "JOIN类型",
                        ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN"]
                    )
                
                # ON条件
                col3, col4 = st.columns(2)
                with col3:
                    on_left = st.text_input("左表字段", placeholder="例如: category_id")
                with col4:
                    on_right = st.text_input("右表字段", placeholder="例如: category_id")
                
                # 右表字段
                right_fields_input = st.text_area(
                    "右表选择字段（每行一个）",
                    placeholder="category_name\nparent_category",
                    height=80
                )
                
                if st.form_submit_button("➕ 添加JOIN", use_container_width=True):
                    if all([right_table, right_alias, on_left, on_right]):
                        right_fields = [f.strip() for f in right_fields_input.split('\n') if f.strip()]
                        st.session_state.joins.append({
                            'left_alias': left_alias,
                            'right_table': right_table,
                            'right_alias': right_alias,
                            'join_type': join_type,
                            'on_left': on_left,
                            'on_right': on_right,
                            'right_fields': right_fields
                        })
                        # 同时添加到tables列表
                        st.session_state.tables.append({
                            'name': right_table,
                            'alias': right_alias,
                            'fields': right_fields
                        })
                        rebuild_query()
                        st.success(f"✓ 已添加JOIN: {left_alias} → {right_alias}")
                        st.rerun()
            
            # 显示已添加的JOIN
            if st.session_state.joins:
                st.markdown("---")
                st.subheader("已添加的JOIN")
                for i, join in enumerate(st.session_state.joins):
                    with st.expander(f"{join['left_alias']} → {join['right_alias']}"):
                        st.write(f"**类型**: {join['join_type']}")
                        st.write(f"**条件**: {join['left_alias']}.{join['on_left']} = {join['right_alias']}.{join['on_right']}")
                        if st.button(f"🗑️ 删除", key=f"del_join_{i}"):
                            st.session_state.joins.pop(i)
                            rebuild_query()
                            st.rerun()
    
    # ===== Tab 3: 筛选条件 =====
    with tab3:
        if not st.session_state.tables:
            st.info("💡 请先添加表")
        else:
            st.subheader("添加筛选条件")
            
            with st.form("add_filter_form"):
                # 选择表和字段
                col1, col2 = st.columns(2)
                with col1:
                    table_aliases = [t['alias'] for t in st.session_state.tables]
                    filter_table = st.selectbox("表别名", table_aliases, key="filter_table")
                with col2:
                    filter_field = st.text_input("字段名", placeholder="例如: price")
                
                # 选择操作符
                operator_names = [op.name for op in FilterOperator]
                filter_operator = st.selectbox("操作符", operator_names)
                
                # 值输入（根据操作符类型调整）
                if filter_operator in ["IS_NULL", "IS_NOT_NULL"]:
                    filter_value = None
                    st.info("该操作符不需要输入值")
                elif filter_operator == "BETWEEN":
                    col3, col4 = st.columns(2)
                    with col3:
                        val1 = st.text_input("起始值", key="between_start")
                    with col4:
                        val2 = st.text_input("结束值", key="between_end")
                    filter_value = [val1, val2]
                elif filter_operator in ["IN", "NOT_IN"]:
                    filter_value_input = st.text_input(
                        "值（逗号分隔）",
                        placeholder="例如: 电子产品,服装,食品"
                    )
                    filter_value = [v.strip() for v in filter_value_input.split(',') if v.strip()]
                else:
                    filter_value = st.text_input("值", placeholder="例如: 100")
                
                # 逻辑操作符
                logic_op = st.radio("与前一个条件的关系", ["AND", "OR"], horizontal=True)
                
                if st.form_submit_button("➕ 添加筛选条件", use_container_width=True):
                    if filter_field:
                        st.session_state.filters.append({
                            'table_alias': filter_table,
                            'field': filter_field,
                            'operator': filter_operator,
                            'value': filter_value,
                            'logic': logic_op
                        })
                        rebuild_query()
                        st.success(f"✓ 已添加筛选: {filter_table}.{filter_field}")
                        st.rerun()
            
            # 显示已添加的筛选
            if st.session_state.filters:
                st.markdown("---")
                st.subheader("已添加的筛选条件")
                for i, flt in enumerate(st.session_state.filters):
                    logic_prefix = "" if i == 0 else f"{flt['logic']} "
                    with st.expander(f"{logic_prefix}{flt['table_alias']}.{flt['field']} {flt['operator']}"):
                        st.write(f"**值**: {flt['value']}")
                        if st.button(f"🗑️ 删除", key=f"del_filter_{i}"):
                            st.session_state.filters.pop(i)
                            rebuild_query()
                            st.rerun()
    
        # ===== Tab 4: CASE WHEN =====
    with tab4:
        if not st.session_state.tables:
            st.info("💡 请先添加表")
        else:
            st.subheader("添加CASE WHEN表达式")
            
            st.markdown("⚠️ 简化版本：目前支持基于已有筛选条件创建CASE WHEN")
            
            with st.form("add_case_when_form"):
                # 别名
                case_alias = st.text_input("结果字段别名", placeholder="例如: price_level")
                
                # ELSE值
                else_value = st.text_input("ELSE值（默认值）", placeholder="例如: 其他")
                
                st.markdown("**WHEN条件配置**")
                st.info("💡 提示：先在'筛选条件'tab添加条件，这里可以引用它们")
                
                # 简化版：手动输入条件和结果
                num_conditions = st.number_input("条件数量", min_value=1, max_value=10, value=2)
                
                conditions_input = []
                for i in range(int(num_conditions)):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        cond_table = st.selectbox(f"条件{i+1}-表", 
                                                [t['alias'] for t in st.session_state.tables],
                                                key=f"case_table_{i}")
                    with col2:
                        cond_field = st.text_input(f"字段", key=f"case_field_{i}")
                    with col3:
                        cond_op = st.selectbox(f"操作符",
                                              ["EQUALS", "GREATER", "LESS", "IN"],
                                              key=f"case_op_{i}")
                    with col4:
                        cond_value = st.text_input(f"值", key=f"case_value_{i}")
                    
                    then_value = st.text_input(f"条件{i+1} THEN值", 
                                              placeholder=f"例如: 高价",
                                              key=f"case_then_{i}")
                    
                    conditions_input.append({
                        'table': cond_table,
                        'field': cond_field,
                        'operator': cond_op,
                        'value': cond_value,
                        'then': then_value
                    })
                
                if st.form_submit_button("➕ 添加CASE WHEN", use_container_width=True):
                    if case_alias:
                        # 构建条件列表
                        case_conditions = []
                        for cond_input in conditions_input:
                            if cond_input['field'] and cond_input['then']:
                                # 创建FilterCondition
                                filter_cond = FilterCondition(
                                    cond_input['table'],
                                    cond_input['field'],
                                    FilterOperator[cond_input['operator']],
                                    cond_input['value']
                                )
                                case_conditions.append((filter_cond, cond_input['then']))
                        
                        st.session_state.case_whens.append({
                            'alias': case_alias,
                            'conditions': case_conditions,
                            'else_value': else_value if else_value else None
                        })
                        rebuild_query()
                        st.success(f"✓ 已添加CASE WHEN: {case_alias}")
                        st.rerun()
            
            # 显示已添加的CASE WHEN
            if st.session_state.case_whens:
                st.markdown("---")
                st.subheader("已添加的CASE WHEN")
                for i, case in enumerate(st.session_state.case_whens):
                    with st.expander(f"{case['alias']} ({len(case['conditions'])}个条件)"):
                        for j, (cond, then_val) in enumerate(case['conditions']):
                            st.write(f"WHEN 条件{j+1} THEN {then_val}")
                        if case['else_value']:
                            st.write(f"ELSE {case['else_value']}")
                        if st.button(f"🗑️ 删除", key=f"del_case_{i}"):
                            st.session_state.case_whens.pop(i)
                            rebuild_query()
                            st.rerun()
    
    # ===== Tab 7: 其他选项 =====
    with tab7:
        st.subheader("排序")
        
        if st.session_state.tables:
            with st.form("add_order_form"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    order_table = st.selectbox(
                        "表别名",
                        [t['alias'] for t in st.session_state.tables],
                        key="order_table"
                    )
                with col2:
                    order_field = st.text_input("字段名", key="order_field")
                with col3:
                    order_dir = st.selectbox("方向", ["ASC", "DESC"])
                
                if st.form_submit_button("➕ 添加排序", use_container_width=True):
                    if order_field:
                        st.session_state.order_bys.append({
                            'table_alias': order_table,
                            'field': order_field,
                            'direction': order_dir
                        })
                        rebuild_query()
                        st.success(f"✓ 已添加排序: {order_table}.{order_field}")
                        st.rerun()
            
            if st.session_state.order_bys:
                st.markdown("**已添加的排序**:")
                for i, order in enumerate(st.session_state.order_bys):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.text(f"{order['table_alias']}.{order['field']} {order['direction']}")
                    with col2:
                        if st.button("🗑️", key=f"del_order_{i}"):
                            st.session_state.order_bys.pop(i)
                            rebuild_query()
                            st.rerun()
        
        st.markdown("---")
        st.subheader("DISTINCT")
        distinct_enabled = st.checkbox("去重（SELECT DISTINCT）", value=st.session_state.distinct, key="distinct_checkbox")
        if distinct_enabled != st.session_state.distinct:
            st.session_state.distinct = distinct_enabled
            rebuild_query()
        
        st.markdown("---")
        st.subheader("LIMIT")
        col1, col2 = st.columns(2)
        with col1:
            limit_value = st.number_input("限制行数", min_value=0, value=st.session_state.limit_config['limit'], step=100, key="limit_input")
        with col2:
            offset_value = st.number_input("偏移量", min_value=0, value=st.session_state.limit_config['offset'], step=100, key="offset_input")
        
        if limit_value != st.session_state.limit_config['limit'] or offset_value != st.session_state.limit_config['offset']:
            st.session_state.limit_config = {'limit': limit_value, 'offset': offset_value}
            rebuild_query()
    
    # ===== Tab 5: GROUP BY =====
    with tab5:
        if not st.session_state.tables:
            st.info("💡 请先添加表")
        else:
            st.subheader("GROUP BY配置")
            
            with st.form("add_group_by_form"):
                st.markdown("**选择分组字段**")
                
                group_fields_input = st.text_area(
                    "分组字段（每行一个，格式：表别名.字段名）",
                    placeholder="p.category\np.brand",
                    height=100
                )
                
                st.markdown("**HAVING条件（可选）**")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    having_table = st.selectbox(
                        "表别名",
                        [t['alias'] for t in st.session_state.tables],
                        key="having_table"
                    )
                with col2:
                    having_field = st.text_input("聚合字段", placeholder="COUNT(*) 或 SUM(amount)")
                with col3:
                    having_op = st.selectbox("操作符", ["GREATER", "LESS", "EQUALS"])
                
                having_value = st.text_input("HAVING值", placeholder="例如: 100")
                
                if st.form_submit_button("✓ 设置GROUP BY", use_container_width=True):
                    if group_fields_input:
                        group_fields = [f.strip() for f in group_fields_input.split('\n') if f.strip()]
                        
                        if 'group_by' not in st.session_state:
                            st.session_state.group_by = {}
                        
                        st.session_state.group_by = {
                            'fields': group_fields,
                            'having': None
                        }
                        
                        if having_field and having_value:
                            st.session_state.group_by['having'] = {
                                'table': having_table,
                                'field': having_field,
                                'operator': having_op,
                                'value': having_value
                            }
                        
                        rebuild_query()
                        st.success("✓ 已设置GROUP BY")
                        st.rerun()
            
            # 显示当前GROUP BY
            if 'group_by' in st.session_state and st.session_state.group_by:
                st.markdown("---")
                st.subheader("当前GROUP BY配置")
                st.write(f"**分组字段**: {', '.join(st.session_state.group_by['fields'])}")
                if st.session_state.group_by.get('having'):
                    having = st.session_state.group_by['having']
                    st.write(f"**HAVING**: {having['field']} {having['operator']} {having['value']}")
                
                if st.button("🗑️ 清除GROUP BY", key="clear_group_by"):
                    st.session_state.group_by = {}
                    rebuild_query()
                    st.rerun()
    # ===== Tab 6: 窗口函数 =====
    with tab6:
        if not st.session_state.tables:
            st.info("💡 请先添加表")
        else:
            st.subheader("添加窗口函数")
            
            with st.form("add_window_function_form"):
                col1, col2 = st.columns(2)
                with col1:
                    window_func = st.selectbox(
                        "窗口函数",
                        ["ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG", "COUNT", "MIN", "MAX"]
                    )
                with col2:
                    window_alias = st.text_input("结果别名", placeholder="例如: row_num")
                
                col3, col4 = st.columns(2)
                with col3:
                    window_table = st.selectbox(
                        "表别名",
                        [t['alias'] for t in st.session_state.tables],
                        key="window_table"
                    )
                with col4:
                    window_field = st.text_input(
                        "字段名（聚合函数需要）",
                        placeholder="例如: amount"
                    )
                
                st.markdown("**PARTITION BY（可选）**")
                partition_by_input = st.text_input(
                    "分区字段（逗号分隔）",
                    placeholder="例如: p.category, p.brand"
                )
                
                st.markdown("**ORDER BY（可选）**")
                col5, col6 = st.columns(2)
                with col5:
                    order_field = st.text_input("排序字段", placeholder="例如: p.price")
                with col6:
                    order_dir = st.selectbox("方向", ["ASC", "DESC"], key="window_order_dir")
                
                if st.form_submit_button("➕ 添加窗口函数", use_container_width=True):
                    if window_alias:
                        partition_by = [f.strip() for f in partition_by_input.split(',') if f.strip()]
                        
                        order_by_configs = []
                        if order_field:
                            # 简化：假设格式是 table.field
                            if '.' in order_field:
                                table_part, field_part = order_field.split('.', 1)
                                order_by_configs.append({
                                    'table': table_part,
                                    'field': field_part,
                                    'direction': order_dir
                                })
                        
                        if 'window_functions' not in st.session_state:
                            st.session_state.window_functions = []
                        
                        st.session_state.window_functions.append({
                            'function': window_func,
                            'table': window_table,
                            'field': window_field,
                            'partition_by': partition_by,
                            'order_by': order_by_configs,
                            'alias': window_alias
                        })
                        
                        rebuild_query()
                        st.success(f"✓ 已添加窗口函数: {window_alias}")
                        st.rerun()
            
            # 显示已添加的窗口函数
            if 'window_functions' in st.session_state and st.session_state.window_functions:
                st.markdown("---")
                st.subheader("已添加的窗口函数")
                for i, wf in enumerate(st.session_state.window_functions):
                    with st.expander(f"{wf['alias']} - {wf['function']}"):
                        st.write(f"**字段**: {wf['field'] or '无'}")
                        if wf['partition_by']:
                            st.write(f"**PARTITION BY**: {', '.join(wf['partition_by'])}")
                        if wf['order_by']:
                            st.write(f"**ORDER BY**: {wf['order_by']}")
                        if st.button(f"🗑️ 删除", key=f"del_window_{i}"):
                            st.session_state.window_functions.pop(i)
                            rebuild_query()
                            st.rerun()
# ============= 右侧：预览区 =============
with col_preview:
    st.header("👁️ SQL预览")
    
    # 生成SQL按钮
    if st.button("🔄 生成/刷新SQL", use_container_width=True, type="primary"):
        rebuild_query()
    
    # 生成并显示SQL
    try:
        builder = rebuild_query()
        sql = builder.to_sql()
        
        # 验证SQL
        validation = builder.validate_sql(sql)
        
        # 显示验证状态
        if validation['valid']:
            st.success("✓ SQL语法验证通过")
        else:
            st.error("✗ SQL语法验证失败")
            for error in validation['errors']:
                st.error(f"错误: {error}")
        
        # 显示警告
        if validation['warnings']:
            with st.expander("⚠️ 警告信息", expanded=False):
                for warning in validation['warnings']:
                    st.warning(warning)
        # 显示自然语言描述
        st.markdown("---")
        st.subheader("📝 查询说明")
        
        try:
            description = builder.to_natural_language()
            st.markdown(description)
            
            # AI提示词 - 纯需求描述
            with st.expander("💡 生成SQL需求描述（可直接发给AI）", expanded=False):
                # 生成详细的需求描述
                prompt_parts = []
                
                # 标题
                prompt_parts.append("我需要生成一个SQL查询，具体需求如下：\n")
                
                # 1. 查询的表和字段
                prompt_parts.append("**数据来源**：")
                if builder.tables:
                    for i, table in enumerate(builder.tables):
                        if i == 0:
                            prompt_parts.append(f"\n- 主表：{table.table_name}（别名：{table.alias}）")
                        else:
                            prompt_parts.append(f"\n- 关联表：{table.table_name}（别名：{table.alias}）")
                        
                        if table.selected_fields:
                            fields_str = "、".join(table.selected_fields)
                            prompt_parts.append(f"\n  需要的字段：{fields_str}")
                
                # 2. JOIN关系
                if builder.joins:
                    prompt_parts.append("\n\n**表关联方式**：")
                    for join in builder.joins:
                        join_type_cn = {
                            "LEFT JOIN": "左连接",
                            "INNER JOIN": "内连接",
                            "RIGHT JOIN": "右连接",
                            "FULL OUTER JOIN": "全外连接"
                        }.get(join.join_type, join.join_type)
                        
                        prompt_parts.append(
                            f"\n- {join.left_table_alias} 表 {join_type_cn} {join.right_table.alias} 表"
                            f"\n  连接条件：{join.left_table_alias}.{join.on_left_field} = {join.right_table.alias}.{join.on_right_field}"
                        )
                
                # 3. 筛选条件
                if builder.filters:
                    prompt_parts.append("\n\n**筛选条件**：")
                    for i, f in enumerate(builder.filters):
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
                            "BETWEEN": "介于...之间",
                            "REGEXP": "匹配正则表达式"
                        }.get(f.operator.value, f.operator.value)
                        
                        logic = "" if i == 0 else f"{f.logic_operator} "
                        
                        # 格式化值
                        if isinstance(f.value, list):
                            if f.operator.value == "BETWEEN":
                                value_str = f"{f.value[0]} 和 {f.value[1]}"
                            else:
                                value_str = "、".join(map(str, f.value))
                        elif f.value is None:
                            value_str = ""
                        else:
                            value_str = f" {f.value}"
                        
                        prompt_parts.append(f"\n- {logic}{f.table_alias}.{f.field} {op_cn} {value_str}")
                
                # 4. GROUP BY
                if hasattr(builder, 'group_by') and builder.group_by:
                    prompt_parts.append(f"\n\n**分组统计**：按 {', '.join(builder.group_by)} 分组")
                
                # 5. CASE WHEN
                if builder.case_when:
                    prompt_parts.append("\n\n**条件字段**：")
                    for case in builder.case_when:
                        prompt_parts.append(f"\n- 创建字段 {case.alias}，根据以下条件赋值：")
                        for j, (cond, then_val) in enumerate(case.conditions, 1):
                            prompt_parts.append(f"\n  条件{j}：如果 {cond.to_sql()}，则值为 {then_val}")
                        if case.else_value:
                            prompt_parts.append(f"\n  否则值为 {case.else_value}")
                
                # 6. 窗口函数
                if builder.window_functions:
                    prompt_parts.append("\n\n**窗口函数计算**：")
                    for wf in builder.window_functions:
                        wf_desc = f"\n- 计算 {wf.function_name}"
                        if hasattr(wf, 'field') and wf.field:
                            wf_desc += f"({wf.field})"
                        wf_desc += f"，结果命名为 {wf.alias}"
                        if hasattr(wf, 'partition_by') and wf.partition_by:
                            wf_desc += f"\n  按 {', '.join(wf.partition_by)} 分区"
                        if hasattr(wf, 'order_by') and wf.order_by:
                            order_strs = [f"{o.field} {'升序' if o.direction == 'ASC' else '降序'}" for o in wf.order_by]
                            wf_desc += f"\n  按 {', '.join(order_strs)} 排序"
                        prompt_parts.append(wf_desc)
                
                # 7. 排序
                if builder.order_by:
                    prompt_parts.append("\n\n**结果排序**：")
                    order_strs = []
                    for sort in builder.order_by:
                        direction = "升序" if sort.direction == "ASC" else "降序"
                        order_strs.append(f"{sort.table_alias}.{sort.field} {direction}")
                    prompt_parts.append(f"\n- 按 {', '.join(order_strs)}")
                
                # 8. 去重
                if builder.distinct:
                    prompt_parts.append("\n\n**去重**：需要对结果进行去重")
                
                # 9. LIMIT
                if builder.limit:
                    limit_text = f"\n\n**返回限制**：只返回 {builder.limit} 条记录"
                    if builder.offset:
                        limit_text += f"，跳过前 {builder.offset} 条"
                    prompt_parts.append(limit_text)
                
                # 结尾
                prompt_parts.append("\n\n请根据以上需求生成对应的SQL查询语句。")
                
                prompt = "".join(prompt_parts)
                
                st.code(prompt, language="text")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        "📋 下载需求描述",
                        prompt,
                        file_name="sql_requirements.txt",
                        mime="text/plain",
                        use_container_width=True
                    )
                with col2:
                    if st.button("📋 复制到剪贴板", key="copy_prompt", use_container_width=True):
                        st.info("💡 请手动选择并复制上方文本")
        except Exception as e:
            st.error(f"生成描述时出错: {str(e)}")
        # 显示格式化的SQL
        st.markdown("---")
        st.subheader("生成的SQL")
        st.code(validation['formatted'], language='sql')
        
        # 统计信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("总行数", len(sql.splitlines()))
        with col2:
            st.metric("表数量", len(builder.tables))
        with col3:
            st.metric("JOIN数量", len(builder.joins))
        
        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric("筛选条件", len(builder.filters))
        with col5:
            st.metric("CASE WHEN", len(builder.case_when))
        with col6:
            st.metric("排序字段", len(builder.order_by))
        
        # 下载按钮
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="📥 下载SQL文件",
                data=validation['formatted'],
                file_name="generated_query.sql",
                mime="text/plain",
                use_container_width=True
            )
        with col2:
            # 导出配置为JSON
            config = {
                'tables': st.session_state.tables,
                'joins': st.session_state.joins,
                'filters': st.session_state.filters,
                'order_bys': st.session_state.order_bys
            }
            st.download_button(
                label="📥 下载配置JSON",
                data=json.dumps(config, ensure_ascii=False, indent=2),
                file_name="query_config.json",
                mime="application/json",
                use_container_width=True
            )
        
    except Exception as e:
        st.error(f"生成SQL时出错: {str(e)}")
        st.exception(e)

# ============= 底部：快捷操作 =============
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🗑️ 清空所有配置", use_container_width=True):
        st.session_state.tables = []
        st.session_state.joins = []
        st.session_state.filters = []
        st.session_state.case_whens = []
        st.session_state.order_bys = []
        st.session_state.distinct = False
        st.session_state.limit_config = {'limit': 0, 'offset': 0}
        if 'group_by' in st.session_state:
            st.session_state.group_by = {}
        if 'window_functions' in st.session_state:
            st.session_state.window_functions = []
        st.session_state.builder = UniversalQueryBuilder()
        st.rerun()

with col2:
    # 加载示例
    if st.button("📋 加载示例查询", use_container_width=True):
        # 保存当前配置作为备份
        st.session_state.config_backup = {
            'tables': st.session_state.tables.copy(),
            'joins': st.session_state.joins.copy(),
            'filters': st.session_state.filters.copy(),
            'case_whens': st.session_state.case_whens.copy(),
            'order_bys': st.session_state.order_bys.copy(),
            'distinct': st.session_state.distinct,
            'limit_config': st.session_state.limit_config.copy(),
            'group_by': st.session_state.get('group_by', {}).copy() if st.session_state.get('group_by') else {},
            'window_functions': st.session_state.get('window_functions', []).copy() if st.session_state.get('window_functions') else []
        }
        
        # 清空现有配置
        st.session_state.tables = []
        st.session_state.joins = []
        st.session_state.filters = []
        st.session_state.order_bys = []
        st.session_state.case_whens = []
        st.session_state.distinct = False
        st.session_state.limit_config = {'limit': 0, 'offset': 0}
        st.session_state.group_by = {}
        st.session_state.window_functions = []
        
        # 添加示例配置
        st.session_state.tables = [
            {'name': 'products', 'alias': 'p', 'fields': ['product_id', 'product_name', 'price']},
        ]
        st.session_state.joins = [
            {
                'left_alias': 'p',
                'right_table': 'categories',
                'right_alias': 'c',
                'join_type': 'LEFT JOIN',
                'on_left': 'category_id',
                'on_right': 'category_id',
                'right_fields': ['category_name']
            }
        ]
        st.session_state.filters = [
            {
                'table_alias': 'p',
                'field': 'price',
                'operator': 'GREATER',
                'value': '100',
                'logic': 'AND'
            }
        ]
        st.session_state.order_bys = [
            {'table_alias': 'p', 'field': 'price', 'direction': 'DESC'}
        ]
        
        st.session_state.has_loaded_example = True
        rebuild_query()
        st.success("✓ 已加载示例查询（可点击'撤销示例'恢复之前的配置）")
        st.rerun()

with col3:
    # 撤销示例按钮或保存模板按钮
    if st.session_state.has_loaded_example and st.session_state.config_backup is not None:
        if st.button("↩️ 撤销示例", use_container_width=True, type="secondary"):
            # 恢复备份
            backup = st.session_state.config_backup
            st.session_state.tables = backup['tables']
            st.session_state.joins = backup['joins']
            st.session_state.filters = backup['filters']
            st.session_state.case_whens = backup['case_whens']
            st.session_state.order_bys = backup['order_bys']
            st.session_state.distinct = backup['distinct']
            st.session_state.limit_config = backup['limit_config']
            st.session_state.group_by = backup['group_by']
            st.session_state.window_functions = backup['window_functions']
            
            # 清除备份和标记
            st.session_state.config_backup = None
            st.session_state.has_loaded_example = False
            
            rebuild_query()
            st.success("✓ 已恢复到加载示例前的配置")
            st.rerun()
    else:
        if st.button("💾 保存到模板库", use_container_width=True):
            st.session_state.show_save_template_dialog = True
            st.rerun()

# ============= 模板管理区域 =============
if 'show_save_template_dialog' not in st.session_state:
    st.session_state.show_save_template_dialog = False

if 'show_template_manager' not in st.session_state:
    st.session_state.show_template_manager = False

# 保存模板对话框
if st.session_state.show_save_template_dialog:
    st.markdown("---")
    st.subheader("💾 保存为模板")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        template_name = st.text_input("模板名称", placeholder="例如: 电商销售分析", key="new_template_name")
    with col2:
        st.write("")
        st.write("")
        if st.button("保存", type="primary", key="save_template_btn"):
            if template_name:
                if save_template(template_name):
                    st.success(f"✓ 模板 '{template_name}' 已保存")
                    st.session_state.show_save_template_dialog = False
                    st.rerun()
            else:
                st.error("请输入模板名称")
    
    if st.button("取消", key="cancel_save_template"):
        st.session_state.show_save_template_dialog = False
        st.rerun()

# 模板管理按钮
st.markdown("---")
if st.button("📚 管理模板库", use_container_width=True):
    st.session_state.show_template_manager = not st.session_state.show_template_manager
    st.rerun()

# 模板管理界面
if st.session_state.show_template_manager:
    st.markdown("---")
    st.subheader("📚 模板库")
    
    templates = get_all_templates()
    
    if not templates:
        st.info("暂无保存的模板，点击上方'保存到模板库'创建第一个模板")
    else:
        st.write(f"共有 {len(templates)} 个模板")
        
        for i, template in enumerate(templates):
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.write(f"**{template['name']}**")
            with col2:
                if st.button("📂 加载", key=f"load_template_{i}"):
                    if load_template(template['name']):
                        st.success(f"✓ 已加载模板 '{template['name']}'")
                        st.session_state.show_template_manager = False
                        st.rerun()
            with col3:
                if st.button("🗑️ 删除", key=f"delete_template_{i}"):
                    if delete_template(template['name']):
                        st.success(f"✓ 已删除模板 '{template['name']}'")
                        st.rerun()
            
            st.markdown("---")
# ============= 侧边栏：帮助和说明 =============
with st.sidebar:
    st.header("📖 使用说明")
    
    with st.expander("🚀 快速开始"):
        st.markdown("""
        1. **添加表**: 在"表和字段"标签中添加主表
        2. **添加JOIN**: 如需连接其他表，在"JOIN关系"中配置
        3. **添加筛选**: 在"筛选条件"中设置WHERE条件
        4. **添加排序**: 在"其他选项"中配置ORDER BY
        5. **生成SQL**: 点击"生成/刷新SQL"查看结果
        """)
    
    with st.expander("💡 使用技巧"):
        st.markdown("""
        - **字段输入**: 每行一个字段名，不需要加逗号
        - **IN操作符**: 多个值用逗号分隔
        - **正则表达式**: 使用REGEXP操作符进行模式匹配
        - **下载**: 可以下载SQL文件和配置JSON
        """)
    
    with st.expander("🔧 常见问题"):
        st.markdown("""
        **Q: 如何删除已添加的配置?**  
        A: 在对应的展开区域中点击"删除"按钮
        
        **Q: 支持哪些数据库?**  
        A: 生成的是标准SQL，兼容MySQL、PostgreSQL等主流数据库
        
        **Q: 可以保存配置吗?**  
        A: 可以下载配置JSON文件，后续版本将支持直接加载
        """)
    
    st.markdown("---")
    st.markdown("**版本**: v1.0.0")
    st.markdown("**作者**: Zxj")
