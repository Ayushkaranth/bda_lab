@echo off
REM Open the dashboard (data and pipeline already built).
cd /d %~dp0
call .venv\Scripts\activate
streamlit run app.py
