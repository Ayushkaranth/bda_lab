@echo off
REM One-click: install, generate data, run the pipeline, open the dashboard.
cd /d %~dp0
set PYTHONIOENCODING=utf-8
if not exist .venv (
  echo Creating virtual environment...
  python -m venv .venv
)
call .venv\Scripts\activate
pip install -q -r requirements.txt
python -m src.generate_data
python -m src.pipeline
streamlit run app.py
