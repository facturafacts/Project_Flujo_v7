#!/usr/bin/env python3
"""Redirects to scripts/ledger/merge.py"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.ledger import merge
merge.run_merge(full="--full" in sys.argv)
