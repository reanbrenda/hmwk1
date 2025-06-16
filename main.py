from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import requests
import json
import asyncio
import uvicorn
from time import sleep
from typing import List, Optional
import asyncpg
import uuid
from datetime import datetime
import os
from contextlib import asynccontextmanager


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5434/shifts_db")


db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    await init_database()
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(lifespan=lifespan)

BASE_URL = "http://localhost:8181/shift"
GET_SHIFTS_URL = "http://localhost:8181/shifts"
MAX_RETRIES = 6
RETRY_DELAY = 0.5

class Shift(BaseModel):
    companyId: str
    userId: str
    startTime: str
    endTime: str
    action: str = "add"

class ShiftRequest(BaseModel):
    shifts: List[Shift]

class RequestStatus(BaseModel):
    request_id: str
    status: str
    total_shifts: int
    processed: int
    successful: int
    failed: int
    created_at: datetime
    completed_at: Optional[datetime] = None

sample_shifts = [
    {"companyId": "acme-corp", "userId": "user001", "startTime": "2025-06-15T08:00:00", "endTime": "2025-06-15T16:00:00", "action": "add"},
    {"companyId": "tech-corp", "userId": "user002", "startTime": "2025-06-15T09:00:00", "endTime": "2025-06-15T17:00:00", "action": "add"},
    {"companyId": "work-corp", "userId": "user003", "startTime": "2025-06-15T10:00:00", "endTime": "2025-06-15T18:00:00", "action": "add"},
    {"companyId": "name-corp", "userId": "user004", "startTime": "2025-06-15T11:00:00", "endTime": "2025-06-15T19:00:00", "action": "add"},
    {"companyId": "juice-corp", "userId": "user005", "startTime": "2025-06-15T12:00:00", "endTime": "2025-06-15T20:00:00", "action": "add"},
    {"companyId": "bree-corp", "userId": "user006", "startTime": "2025-06-15T13:00:00", "endTime": "2025-06-15T21:00:00", "action": "add"},
    {"companyId": "acme-corp", "userId": "user007", "startTime": "2025-06-15T14:00:00", "endTime": "2025-06-15T22:00:00", "action": "add"},
    {"companyId": "acme-corp", "userId": "user008", "startTime": "2025-06-15T15:00:00", "endTime": "2025-06-15T23:00:00", "action": "add"},
    {"companyId": "acme-corp", "userId": "user009", "startTime": "2025-06-16T08:00:00", "endTime": "2025-06-16T16:00:00", "action": "add"},
    {"companyId": "acme-corp", "userId": "user010", "startTime": "2025-06-16T09:00:00", "endTime": "2025-06-16T17:00:00", "action": "add"}
]

headers = {'Content-Type': 'application/json'}

async def init_database():
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS shift_requests (
                id UUID PRIMARY KEY,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                total_shifts INTEGER NOT NULL,
                processed INTEGER DEFAULT 0,
                successful INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP NULL
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS individual_shifts (
                id SERIAL PRIMARY KEY,
                request_id UUID REFERENCES shift_requests(id),
                company_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                start_time VARCHAR(255) NOT NULL,
                end_time VARCHAR(255) NOT NULL,
                action VARCHAR(20) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                error_message TEXT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                processed_at TIMESTAMP NULL
            )
        ''')

async def get_existing_shifts():
    try:
        response = requests.get(GET_SHIFTS_URL)
        return response.json().get('shifts', [])
    except requests.exceptions.RequestException as e:
        print(f"fetching existing shifts is  error: {e}")
        return []

def shift_exists(shift, existing_shifts):
    for existing_shift in existing_shifts:
        if (existing_shift['companyId'] == shift['companyId'] and
            existing_shift['userId'] == shift['userId'] and
            existing_shift['startTime'] == shift['startTime'] and
            existing_shift['endTime'] == shift['endTime']):
            return True
    return False

async def update_shift_status(shift_id: int, status: str, attempts: int = 0, error_message: str = None):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE individual_shifts 
            SET status = $1, attempts = $2, error_message = $3, processed_at = NOW()
            WHERE id = $4
        ''', status, attempts, error_message, shift_id)

async def update_request_progress(request_id: str):
    async with db_pool.acquire() as conn:
        counts = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status != 'pending') as processed,
                COUNT(*) FILTER (WHERE status = 'success') as successful,
                COUNT(*) FILTER (WHERE status = 'failed') as failed
            FROM individual_shifts 
            WHERE request_id = $1
        ''', uuid.UUID(request_id))
        
        if counts['processed'] == counts['total']:
            overall_status = 'completed'
            completed_at = datetime.now()
        else:
            overall_status = 'processing'
            completed_at = None
        
        await conn.execute('''
            UPDATE shift_requests 
            SET processed = $1, successful = $2, failed = $3, status = $4, completed_at = $5
            WHERE id = $6
        ''', counts['processed'], counts['successful'], counts['failed'], 
             overall_status, completed_at, uuid.UUID(request_id))

async def process_single_shift(shift_id: int, shift_data: dict, request_id: str):
    print(f"processing shift: {shift_data.get('userId')}")
    
    existing_shifts = await get_existing_shifts()
    if shift_exists(shift_data, existing_shifts):
        print(f"Shift for user {shift_data['userId']} already exists")
        await update_shift_status(shift_id, 'skipped', 0, 'Shift already exists')
        await update_request_progress(request_id)
        return
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES} for user {shift_data['userId']}")
            
            response = requests.post(BASE_URL, headers=headers, json=shift_data)
            
            if response.status_code in [200, 201]:
                print(f"Successfully booked shift for user {shift_data['userId']}")
                await update_shift_status(shift_id, 'success', attempt + 1)
                await update_request_progress(request_id)
                return
            else:
                print(f"HTTP {response.status_code} for user {shift_data['userId']}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)
                
        except requests.exceptions.RequestException as e:
            print(f"Request error for user {shift_data['userId']}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
    
    print(f"failed to book shift for user {shift_data['userId']} after {MAX_RETRIES} attempts")
    await update_shift_status(shift_id, 'failed', MAX_RETRIES, 'Max retries exceeded')
    await update_request_progress(request_id)

async def process_shift_request_background(request_id: str):
    print(f"starting background processing for request {request_id}")
    
    async with db_pool.acquire() as conn:
        shifts = await conn.fetch('''
            SELECT id, company_id, user_id, start_time, end_time, action
            FROM individual_shifts 
            WHERE request_id = $1 AND status = 'pending'
        ''', uuid.UUID(request_id))
    
    tasks = []
    for shift_row in shifts:
        shift_data = {
            'companyId': shift_row['company_id'],
            'userId': shift_row['user_id'],
            'startTime': shift_row['start_time'],
            'endTime': shift_row['end_time'],
            'action': shift_row['action']
        }
        task = process_single_shift(shift_row['id'], shift_data, request_id)
        tasks.append(task)
    
    semaphore = asyncio.Semaphore(5)  
    
    async def limited_process(task):
        async with semaphore:
            await task
    
    await asyncio.gather(*[limited_process(task) for task in tasks])

@app.post("/book-shifts")
async def book_shifts(request: ShiftRequest, background_tasks: BackgroundTasks):
    shifts = [shift.dict() for shift in request.shifts]
    
    if len(shifts) < 10:
        raise HTTPException(status_code=400, detail="at least 10 shifts required")
    
    request_id = str(uuid.uuid4())
    
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO shift_requests (id, total_shifts, status)
            VALUES ($1, $2, $3)
        ''', uuid.UUID(request_id), len(shifts), 'pending')
        
        for shift in shifts:
            await conn.execute('''
                INSERT INTO individual_shifts 
                (request_id, company_id, user_id, start_time, end_time, action)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', uuid.UUID(request_id), shift['companyId'], shift['userId'], 
                 shift['startTime'], shift['endTime'], shift['action'])
    
    background_tasks.add_task(process_shift_request_background, request_id)
    
    return {
        'request_id': request_id,
        'status': 'accepted',
        'message': 'Request accepted for processing',
        'total_shifts': len(shifts)
    }

@app.get("/request-status/{request_id}")
async def get_request_status(request_id: str):
    """Get status of a shift booking request"""
    try:
        request_uuid = uuid.UUID(request_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid request ID format")
    
    async with db_pool.acquire() as conn:
        request_info = await conn.fetchrow('''
            SELECT * FROM shift_requests WHERE id = $1
        ''', request_uuid)
        
        if not request_info:
            raise HTTPException(status_code=404, detail="Request not found")
        
        shift_details = await conn.fetch('''
            SELECT company_id, user_id, status, attempts, error_message, processed_at
            FROM individual_shifts 
            WHERE request_id = $1
            ORDER BY id
        ''', request_uuid)
    
    return {
        'request_id': request_id,
        'status': request_info['status'],
        'total_shifts': request_info['total_shifts'],
        'processed': request_info['processed'],
        'successful': request_info['successful'],
        'failed': request_info['failed'],
        'skipped': request_info['processed'] - request_info['successful'] - request_info['failed'],
        'created_at': request_info['created_at'],
        'completed_at': request_info['completed_at'],
        'shifts': [
            {
                'company_id': shift['company_id'],
                'user_id': shift['user_id'],
                'status': shift['status'],
                'attempts': shift['attempts'],
                'error_message': shift['error_message'],
                'processed_at': shift['processed_at']
            } for shift in shift_details
        ]
    }

@app.get("/test-book")
async def test_book(confirm: bool = False, background_tasks: BackgroundTasks = None):
    if not confirm:
        return {"error": "pass confirm=true to execute test booking"}
    
    shift_objects = [Shift(**shift) for shift in sample_shifts]
    request_obj = ShiftRequest(shifts=shift_objects)
    
    return await book_shifts(request_obj, background_tasks)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)