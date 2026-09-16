"""Loads .env once, on first import of any src.common.* module, so every
entry point (demo scripts, Glue/EMR job scripts, Lambda handlers run
locally, pytest) sees a consistent local environment without each one
having to remember to call load_dotenv() itself.
"""
from dotenv import load_dotenv

load_dotenv()
