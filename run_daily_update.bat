@echo off
echo Starting daily stock data update...
cd /d D:\AI\AI-Bot
python update_daily_data.py
echo Update completed.
pause