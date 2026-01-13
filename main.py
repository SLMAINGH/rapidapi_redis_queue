import asyncio
import json
import uuid
import time
import httpx
import logging
import os
import redis.asyncio as redis
from fastapi import FastAPI, Response, Request
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("api_proxy")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SECONDS_BETWEEN_REQUESTS = float(os.getenv("RATE_LIMIT_DELAY", "3.5"))
REDIS_QUEUE_KEY = "api_request_queue"
REDIS_JOB_PREFIX = "job:"
REDIS_EXPIRATION = 3600
GLOBAL_HEADERS = json.loads(os.getenv("GLOBAL_HEADERS", "{}"))

redis_client: Optional[redis.Redis] = None
MAX_WEBHOOK_SIZE_BYTES = 100 * 1024  # 100 KB


# ==================== TRANSFORMATIONS ====================

def transform_linkedin_reactions_text_only(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only the 'text' field from LinkedIn reactions posts and concatenate them."""
    if not isinstance(response_data, dict):
        return response_data

    # Create a copy of the response to modify
    result = response_data.copy()

    # Check if 'data' array exists
    if 'data' in result and isinstance(result['data'], list):
        texts = []
        for item in result['data']:
            if isinstance(item, dict) and 'post' in item and isinstance(item['post'], dict):
                text = item['post'].get('text', '')
                if text:
                    texts.append(text)

        # Concatenate all texts with double newline separator
        result['data'] = '\n\n'.join(texts)

    return result


def apply_endpoint_transformation(url: str, response_data: Dict[str, Any], opcja: Optional[str] = None) -> Dict[str, Any]:
    """Apply endpoint-specific transformations based on URL and opcja parameter."""
    if opcja == "oryginal":
        logger.info("opcja=oryginal - skipping transformations")
        return response_data

    if "fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/reactions" in url:
        logger.info("Applying LinkedIn reactions text-only transformation")
        return transform_linkedin_reactions_text_only(response_data)

    return response_data


def truncate_to_size_limit(payload: Dict[str, Any], max_bytes: int = MAX_WEBHOOK_SIZE_BYTES) -> Dict[str, Any]:
    """Truncate webhook payload to fit within size limit, keeping newest data."""
    payload_json = json.dumps(payload)

    if len(payload_json.encode('utf-8')) <= max_bytes:
        return payload

    logger.warning(f"Payload size {len(payload_json.encode('utf-8'))} bytes exceeds {max_bytes} bytes limit")

    if 'api_response' in payload and isinstance(payload['api_response'], dict):
        api_response = payload['api_response']

        # Handle string data
        if 'data' in api_response and isinstance(api_response['data'], str):
            truncated_payload = payload.copy()
            truncated_payload['api_response'] = api_response.copy()
            data_str = api_response['data']
            original_length = len(data_str)

            while data_str and len(json.dumps(truncated_payload).encode('utf-8')) > max_bytes:
                data_str = data_str[:-100]
                truncated_payload['api_response']['data'] = data_str

            if data_str:
                truncated_payload['_truncated'] = True
                truncated_payload['_original_length'] = original_length
                truncated_payload['_truncated_length'] = len(data_str)
                return truncated_payload

        # Handle array data
        elif 'data' in api_response and isinstance(api_response['data'], list):
            truncated_payload = payload.copy()
            truncated_payload['api_response'] = api_response.copy()
            data_items = api_response['data'].copy()

            while data_items and len(json.dumps(truncated_payload).encode('utf-8')) > max_bytes:
                data_items.pop()
                truncated_payload['api_response']['data'] = data_items

            if data_items:
                truncated_payload['_truncated'] = True
                truncated_payload['_original_item_count'] = len(api_response['data'])
                truncated_payload['_truncated_item_count'] = len(data_items)
                return truncated_payload

    logger.error("Unable to truncate payload to size limit")
    return payload


# ==================== CORE LOGIC ====================

async def update_job_status(job_id: str, status: str, result=None, error=None):
    payload = {
        "status": status,
        "updated_at": time.time(),
        "result": result,
        "error": error
    }
    try:
        if redis_client:
            await redis_client.set(
                f"{REDIS_JOB_PREFIX}{job_id}",
                json.dumps(payload),
                ex=REDIS_EXPIRATION
            )
    except Exception as e:
        logger.error(f"Redis status update failed for {job_id}: {e}")


async def process_job(job: Dict[str, Any]):
    """Process a single job and ensure webhook is sent even on failure."""
    job_id = job['job_id']
    target_url = job['target_api_url']
    method = job.get('request_method', 'GET').upper()
    request_body = job.get('request_body')
    opcja = job.get('opcja')
    
    # We normalized this in queue_job, so it should be correct now
    webhook_url = job.get('callback_webhook_url')

    headers = GLOBAL_HEADERS.copy()
    if job.get('request_headers'):
        headers.update(job['request_headers'])

    logger.info(f"Processing {job_id} -> {method} {target_url} (opcja={opcja})")

    async with httpx.AsyncClient() as client:
        try:
            # 1. Perform the API Request
            if method == 'POST':
                response = await client.post(target_url, headers=headers, json=request_body, timeout=30.0)
            elif method == 'PUT':
                response = await client.put(target_url, headers=headers, json=request_body, timeout=30.0)
            elif method == 'DELETE':
                response = await client.delete(target_url, headers=headers, timeout=30.0)
            else:
                response = await client.get(target_url, headers=headers, timeout=30.0)

            # 2. Parse Response
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {"raw_text": response.text}

            # 3. Apply Transformations
            response_data = apply_endpoint_transformation(target_url, response_data, opcja)
            logger.info(f"Success {job_id}: {response.status_code}")

            # 4. Prepare Success Webhook
            webhook_payload = job.copy()
            webhook_payload.pop("request_headers", None)
            webhook_payload.pop("request_body", None)
            webhook_payload["api_status_code"] = response.status_code
            webhook_payload["api_response"] = response_data
            webhook_payload["processed_at"] = time.time()
            webhook_payload["status"] = "success"

            webhook_payload = truncate_to_size_limit(webhook_payload)
            
            # 5. Send Webhook
            if webhook_url:
                try:
                    wb_resp = await client.post(webhook_url, json=webhook_payload, timeout=10.0)
                    logger.info(f"Webhook sent for {job_id}: {wb_resp.status_code}")
                except Exception as e:
                    logger.error(f"Webhook delivery failed for {job_id}: {e}")

            await update_job_status(job_id, "completed", result=response_data)

        except Exception as e:
            # === CRITICAL FIX: Send Error Webhook ===
            logger.error(f"Job {job_id} failed: {e}")
            
            if webhook_url:
                error_payload = {
                    "job_id": job_id,
                    "status": "error",
                    "error_message": str(e),
                    "target_url": target_url,
                    "processed_at": time.time()
                }
                try:
                    await client.post(webhook_url, json=error_payload, timeout=10.0)
                    logger.info(f"Error webhook sent for {job_id}")
                except Exception as we:
                    logger.error(f"Failed to send error webhook for {job_id}: {we}")

            await update_job_status(job_id, "failed", error=str(e))


async def queue_worker():
    rate_per_minute = 60 / SECONDS_BETWEEN_REQUESTS
    logger.info(f"Worker started. Rate limit: {rate_per_minute:.0f} requests/minute")

    while True:
        try:
            if redis_client:
                result = await redis_client.blpop(REDIS_QUEUE_KEY, timeout=0)

                if result:
                    _, raw_data = result
                    job = json.loads(raw_data)
                    start_time = time.monotonic()

                    try:
                        await asyncio.wait_for(process_job(job), timeout=45.0)
                    except asyncio.TimeoutError:
                        logger.error(f"Job {job.get('job_id')} timed out")
                        # Note: process_job has internal timeout handling, this catches worker hangs
                        await update_job_status(job['job_id'], "timeout")

                    elapsed = time.monotonic() - start_time
                    sleep_time = max(0, SECONDS_BETWEEN_REQUESTS - elapsed)
                    await asyncio.sleep(sleep_time)
            else:
                await asyncio.sleep(1)

        except Exception as e:
            logger.critical(f"Worker error: {e}")
            await asyncio.sleep(1)


# ==================== LIFESPAN & API ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    try:
        logger.info(f"Connecting to Redis: {REDIS_URL}")
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("Redis connected")
        asyncio.create_task(queue_worker())
    except Exception as e:
        logger.critical(f"Redis connection failed: {e}")
    
    yield
    
    if redis_client:
        await redis_client.close()
        logger.info("Redis disconnected")


app = FastAPI(lifespan=lifespan)


@app.post("/process")
async def queue_job(request: Request, response: Response):
    try:
        body = await request.json()
    except Exception:
        response.status_code = 400
        return {"error": "Invalid JSON"}

    # === FIX 1: Normalize 'target_api' -> 'target_api_url' ===
    if "target_api_url" not in body:
        if "target_api" in body:
            body["target_api_url"] = body.pop("target_api")
        else:
            response.status_code = 400
            return {"error": "Missing required field: target_api_url"}

    # === FIX 2: Normalize 'callback_webhook' -> 'callback_webhook_url' ===
    if "callback_webhook_url" not in body and "callback_webhook" in body:
        body["callback_webhook_url"] = body.pop("callback_webhook")

    # === FIX 3: Normalize 'headers' -> 'request_headers' ===
    if "request_headers" not in body and "headers" in body:
        body["request_headers"] = body.pop("headers")

    # === FIX 4: Clean the URL (Remove newlines and whitespace) ===
    if body.get("target_api_url"):
        body["target_api_url"] = str(body["target_api_url"]).strip()

    job_id = str(uuid.uuid4())
    body['job_id'] = job_id
    body['queued_at'] = time.time()

    queue_position = -1
    if redis_client:
        await update_job_status(job_id, "queued")
        queue_position = await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(body))

    logger.info(f"Queued {job_id} at position {queue_position}")

    response.status_code = 202
    return {
        "job_id": job_id,
        "status": "queued",
        "queue_position": queue_position
    }


@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    if redis_client:
        data = await redis_client.get(f"{REDIS_JOB_PREFIX}{job_id}")
        if data:
            return json.loads(data)
    return {"status": "not_found"}


@app.get("/health")
async def health():
    redis_ok = False
    queue_depth = 0
    
    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
            queue_depth = await redis_client.llen(REDIS_QUEUE_KEY)
        except:
            pass

    return {
        "status": "ok" if redis_ok else "degraded",
        "redis_connected": redis_ok,
        "queue_depth": queue_depth,
        "rate_limit": f"{60/SECONDS_BETWEEN_REQUESTS:.0f}/min"
    }


@app.get("/")
async def root():
    return {
        "service": "Rate-Limited API Proxy",
        "endpoints": {
            "POST /process": "Queue a new API request",
            "GET /status/{job_id}": "Check job status",
            "GET /health": "Service health check"
        }
    }
