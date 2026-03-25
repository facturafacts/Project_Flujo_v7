#!/usr/bin/env python3
"""Sync Account A — incremental (default)"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.sync import api_sync
api_sync.ACCOUNT = "A"
api_sync.run_sync(full="--full" in sys.argv)
