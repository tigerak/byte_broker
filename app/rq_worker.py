import sys
import os
# 프로젝트 루트 경로를 sys.path에 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from rq import Worker, Queue, Connection
from redis import Redis
from config import *



# Redis 연결 설정
redis_conn = Redis()

# 처리할 큐 리스트 설정
listen = ['default']

if __name__ == '__main__':

    # RQ 워커 실행
    with Connection(redis_conn):
        worker = Worker(map(Queue, listen))
        worker.work()