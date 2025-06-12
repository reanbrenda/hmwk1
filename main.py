from fastapi import FastAPI
from pydantic import BaseModel
import requests
import json
import asyncio
import uvicorn
from time import sleep
from typing import List
app = FastAPI()

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

def get_existing_shifts():
    try:
        response = requests.get(GET_SHIFTS_URL)
        return response.json().get('shifts', [])
    except requests.exceptions.RequestException as e:
        print(f"error fetching existing shifts: {e}")
        return []

def shift_exists(shift, existing_shifts):
    for existing_shift in existing_shifts:
        if (existing_shift['companyId'] == shift['companyId'] and
            existing_shift['userId'] == shift['userId'] and
            existing_shift['startTime'] == shift['startTime'] and
            existing_shift['endTime'] == shift['endTime']):
            return True
    return False

def book_single_shift_with_retry(shift):
    print(f"processing  for user: {shift.get('userId')}")
    existing_shifts = get_existing_shifts()
    print(f"existing shifts: {len(existing_shifts)}")

    if shift_exists(shift, existing_shifts):
        print(f"shift for user {shift['userId']} already exists")
        return {
            'success': True,
            'skipped': True,
            'shift': shift,
            'message': 'Shift already exists'
        }

    for attempt in range(MAX_RETRIES):
        try:
            print(f"attempt {attempt + 1}/{MAX_RETRIES} for user {shift['userId']}")

            response = requests.post(BASE_URL, headers=headers, json=shift)

            if response.status_code in [200, 201]:
                print(f"successfully booked shift for user {shift['userId']}")
                return {
                    'success': True,
                    'skipped': False,
                    'shift': shift,
                    'attempts': attempt + 1,
                    'response': response.json() if response.content else {}
                }
            else:
                print(f"HTTP {response.status_code} for user {shift['userId']}")
                if attempt < MAX_RETRIES - 1:
                    sleep(RETRY_DELAY)

        except requests.exceptions.RequestException as e:
            print(f"request error for user {shift['userId']}: {e}")
            if attempt < MAX_RETRIES - 1:
                sleep(RETRY_DELAY)

    print(f"failed to book shift for user {shift['userId']} after {MAX_RETRIES} attempts")
    return {
        'success': False,
        'skipped': False,
        'shift': shift,
        'attempts': MAX_RETRIES,
        'error': 'Max retries exceeded'
    }

@app.post("/book-shifts")
async def book_shifts(request: ShiftRequest):
    """Book multiple shifts with retry logic"""
    shifts = [shift.dict() for shift in request.shifts]

    if len(shifts) < 10:
        return {'error': 'At least 10 shifts required'}

    results = []
    successful = 0
    skipped = 0
    failed = 0

    for shift in shifts:
        result = book_single_shift_with_retry(shift)
        results.append(result)

        if result['success']:
            if result['skipped']:
                skipped += 1
            else:
                successful += 1
        else:
            failed += 1

    return {
        'results': results,
        'summary': {
            'total': len(shifts),
            'successful': successful,
            'skipped': skipped,
            'failed': failed
        }
    }

@app.get("/test-book")
async def test_book(confirm: bool = False):
    if not confirm:
        return {"error": "Pass confirm=true to execute test booking"}

    results = []
    successful = 0
    skipped = 0
    failed = 0

    for shift in sample_shifts:
        result = book_single_shift_with_retry(shift)
        results.append(result)

        if result['success']:
            if result['skipped']:
                skipped += 1
            else:
                successful += 1
        else:
            failed += 1

    return {
        'results': results,
        'summary': {
            'total': len(sample_shifts),
            'successful': successful,
            'skipped': skipped,
            'failed': failed
        }
    }

if __name__ == "__main__": 
    uvicorn.run(app, host="0.0.0.0", port=8000)
