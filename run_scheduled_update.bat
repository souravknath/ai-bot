@echo off
echo Starting scheduled stock data update system...
cd /d D:\AI\AI-Bot
python update_daily_data.py
pause