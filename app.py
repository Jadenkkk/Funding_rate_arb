from flask import Flask, render_template, jsonify
import pandas as pd
import subprocess
import json
import os
import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

app = Flask(__name__)

# 데이터 저장 경로
DATA_FILE = "arbitrage_data.json"
LAST_UPDATE_FILE = "last_update.txt"

def run_scanner():
    """차익거래 스캐너 실행 및 결과 저장"""
    try:
        # 스캐너 스크립트 실행
        result = subprocess.run(["python", "main.py", "--output_json", "true"], 
                               capture_output=True, text=True)
        
        # 현재 시간 저장
        with open(LAST_UPDATE_FILE, "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
        print("Data updated successfully")
    except Exception as e:
        print(f"Error updating data: {str(e)}")

@app.route('/')
def index():
    """메인 페이지 렌더링"""
    # 마지막 업데이트 시간 가져오기
    last_update = "Unknown"
    if os.path.exists(LAST_UPDATE_FILE):
        with open(LAST_UPDATE_FILE, "r") as f:
            last_update = f.read().strip()
    
    # 저장된 데이터 가져오기
    data = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
    
    return render_template('index.html', 
                          opportunities=data, 
                          last_update=last_update)

@app.route('/api/data')
def get_data():
    """API 엔드포인트: 최신 데이터 반환"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        return jsonify(data)
    return jsonify([])

def initialize():
    """초기 데이터 로드 및 스케줄러 설정"""
    # 처음 시작할 때 데이터 생성
    if not os.path.exists(DATA_FILE):
        run_scanner()
    
    # 스케줄러 설정 (5분마다 실행)
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_scanner, 'interval', minutes=5)
    scheduler.start()

if __name__ == '__main__':
    initialize()
    app.run(host='0.0.0.0', port=5000)