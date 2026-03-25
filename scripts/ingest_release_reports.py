#!/usr/bin/env python3
"""Redirects to scripts/sync/ingest_releases.py"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.sync import ingest_releases
ingest_releases.run()
