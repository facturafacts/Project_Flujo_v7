#!/usr/bin/env python3
"""Redirects to scripts/excel/export.py"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.excel import export
export.run_export()
