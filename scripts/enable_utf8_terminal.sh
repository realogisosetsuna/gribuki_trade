#!/usr/bin/env bash
# 为 Git Bash 设置 UTF-8 环境，避免 Python/rg 输出中文乱码。
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8
export LANG="${LANG:-C.UTF-8}"
export LC_ALL="${LC_ALL:-C.UTF-8}"
printf 'UTF-8 terminal encoding enabled for this Bash session.\n'
