import json
import logging
from datetime import datetime
# Message Q
from redis import Redis
from rq import Queue
# Flask
from flask import request, jsonify
import requests
# Modules
from config import *
from app.main import bp


r = Redis()
q = Queue(connection=r)
print("작업 대기열 설정 완료")


@bp.route('/')
def index():
    print('접근함!!')
    return jsonify(
        message="You're connected to the Data Manage Server API !!",
        contant={
            "key":"Make Somthing",
            "value":"I have No Idea :["
        }
        )
    # return "You're connected to the Main Server API !!"


@bp.route('/job_result/<job_id>', methods=['POST'])
def job_arrt(job_id):
    job = q.fetch_job(job_id)
    if job is not None:
        if job.is_finished:
            result = job.result
            # 작업이 완료되면 Response 객체에서 데이터를 추출하여 저장
            # Response 객체에서 텍스트 또는 JSON 데이터 추출
            try:
                result_data = result.json()  # JSON 데이터 추출
            except ValueError:
                print('에로')
                result_data = result.text  # 텍스트 데이터 추출
            # 추출된 데이터를 JSON으로 직렬화하여 Redis에 저장 -> 전부 str로 바꿔서 저장해버림
            serialized_result = json.dumps(result_data, ensure_ascii=False)
            # 작업이 완료되면 Redis에 result 저장하고 q에서 job 삭제
            r.set(f"job_id: {job_id}", serialized_result)
            job.cleanup()
            return jsonify({'status': 'success', 'message': '작업 성공', 'result': serialized_result})
        elif job.is_failed:
            job.cleanup()
            return jsonify({'status': 'error', 'message': '작업 실패'})
        elif job.is_started: # 현재 작업 중
            return jsonify({'status': 'progress', 'message': f'작업이 처리 중입니다. 예상 처리 시간: 33초'})
        elif job.is_queued: # 큐에서 현재 작업 위치
            queued_jobs = q.job_ids
            position = queued_jobs.index(job_id) + 1
            return jsonify({'status': 'waiting', 'message': f'작업이 대기 중입니다. 대기 순번: {position}/{len(queued_jobs)}'})
    else:
        return jsonify({'status': 'error', 'message': 'Job not found'})


@bp.route('/model_broker', methods=['POST'])  
def enqueue_job():
    jobs = q.jobs
    message = None

    if request.form:
        data = request.form
        try:
            task = q.enqueue(inference_process, data)
            jobs = q.jobs 
            q_len = len(q)
            reaponse = {
                "status": "success",
                "task_id": task.id,
                "q_len": q_len
            }

            return jsonify(reaponse)
        except:
            reaponse = {
                "status": "fail",
                # "id": task.id,
                # "q_len": q_len
            }

            return jsonify(reaponse)


def inference_process(data):
    response = requests.post(MODEL_API_ADDRESS, data=data)
    return response