# tests/test_static_analysis.py
import ast
import pytest
import importlib.util
from pathlib import Path
from unittest.mock import Mock, patch

def test_syntax():
    """Test that all Python files have valid syntax"""
    python_files = Path("src").rglob("*.py")
    
    for file_path in python_files:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                ast.parse(file.read())
        except SyntaxError as e:
            pytest.fail(f"Syntax error in {file_path}: {e}")

def test_imports():
    """Test that all modules can be imported without Databricks"""
    python_files = Path("src").rglob("*.py")
    
    for file_path in python_files:
        module_name = file_path.stem
        try:
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            
            # Mock Databricks-specific imports for import test
            with patch.dict('sys.modules', {
                'pyspark': Mock(),
                'pyspark.sql': Mock(),
                'dbruntime': Mock(),
                'dbruntime.dbutils': Mock()
            }):
                spec.loader.exec_module(module)
                
        except Exception as e:
            pytest.fail(f"Failed to import {file_path}: {e}")