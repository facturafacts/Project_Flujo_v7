#!/usr/bin/env python3
"""Redirects to scripts/excel/import.py"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.excel import import_labels
import_labels.run_import()
