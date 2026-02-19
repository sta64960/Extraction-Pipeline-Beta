#!/bin/bash
echo "============================================"
echo "  FFIEC Data Update"
echo "  This may take 30-60 min per new quarter"
echo "============================================"
echo ""
python3 pipeline.py
echo ""
echo "============================================"
echo "  Done!"
echo "============================================"
