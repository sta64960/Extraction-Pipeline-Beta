@echo off
echo ============================================
echo   FFIEC Data Update
echo   This may take 30-60 min per new quarter
echo ============================================
echo.
python pipeline.py
echo.
echo ============================================
echo   Done! Press any key to close.
echo ============================================
pause
