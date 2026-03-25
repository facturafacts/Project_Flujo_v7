#!/usr/bin/env python3
"""Sync Account B — incremental (default)"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.sync import api_sync
api_sync.ACCOUNT = "B"
date_arg = None
if "--full" in sys.argv:
    idx = sys.argv.index("--full")
    date_arg = sys.argv[idx + 1] if idx + 1 < len(sys.argv) and not sys.argv[idx + 1].startswith("--") else None
api_sync.run_sync(start_date_str=date_arg, full="--full" in sys.argv)
