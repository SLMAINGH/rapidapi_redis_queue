import asyncio
import json
import uuid
import time
import httpx
import logging
import os
import redis.asyncio as redis
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone

# ==================== CONFIGURATION ====================

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("worker")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_QUEUE_KEY = "api_request_queue"  # Restored original key name
REDIS_JOB_PREFIX = "job:"
REDIS_EXPIRATION = 3600
SECONDS_BETWEEN_REQUESTS = float(os.getenv("RATE_LIMIT_DELAY", "3.0"))
MAX_WEBHOOK_SIZE_BYTES = 100 * 1024

# LinkedIn Specific Constants
PROFILE_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/profile"
REACTIONS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/reactions"
POSTS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/posts"
PROFILE_OPTIONAL_PARAMS = [
    'include_follower_and_connection', 'include_experiences', 'include_skills',
    'include_certifications', 'include_publications', 'include_educations',
    'include_volunteers', 'include_honors', 'include_interests', 'include_bio'
]

redis_client: Optional[redis.Redis] = None

# ==================== OLD HELPERS (RESTORED) ====================

def transform_linkedin_reactions_text_only(response_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(response_data, dict): return response_data
    result = response_data.copy()
    if 'data' in result and isinstance(result['data'], list):
        texts = [item['post'].get('text', '') for item in result['data'] if isinstance(item, dict) and 'post' in item and item['post'].get('text')]
        result['data'] = '\n\n'.join(texts)
    return result

def apply_endpoint_transformation(url: str, response_data: Dict[str, Any], opcja: Optional[str] = None) -> Dict[str, Any]:
    if opcja == "oryginal": return response_data
    if "fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/reactions" in url:
        return transform_linkedin_reactions_text_only(response_data)
    return response_data

def truncate_to_size_limit(payload: Dict[str, Any], max_bytes: int = MAX_WEBHOOK_SIZE_BYTES) -> Dict[str, Any]:
    payload_json = json.dumps(payload)
    if len(payload_json.encode('utf-8')) <= max_bytes: return payload
    
    # Simple truncation strategy
    if 'api_response' in payload and isinstance(payload['api_response'], dict):
        # Allow truncate
        payload['api_response']['data'] = "...TRUNCATED..."
    return payload

async def update_job_status(job_id: str, status: str, result=None, error=None):
    payload = {"status": status, "updated_at": time.time(), "result": result, "error": error}
    try:
        if redis_client:
            await redis_client.set(f"{REDIS_JOB_PREFIX}{job_id}", json.dumps(payload), ex=REDIS_EXPIRATION)
    except Exception as e:
        logger.error(f"Redis status update failed: {e}")

# ==================== NEW LINKEDIN HELPERS ====================

def is_within_last_n_days(timestamp_str, days=30):
    try:
        if not timestamp_str: return False
        if timestamp_str.endswith('Z'): timestamp_str = timestamp_str.replace('Z', '+00:00')
        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp.tzinfo is None: timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp >= (datetime.now(timezone.utc) - timedelta(days=days))
    except: return False

def normalize_text(text):
    return text.strip().lower() if text else ""

async def make_api_call(client, url, headers, params):
    for attempt in range(3):
        try:
            resp = await client.get(url, headers=headers, params=params, timeout=15.0)
            if resp.status_code == 200: return resp.json()
            if resp.status_code == 429:
                await asyncio.sleep(5)
                continue
            if resp.status_code == 404: return None
        except:
            if attempt < 2: await asyncio.sleep(2)
    return None

# ==================== LOGIC BRANCHING ====================

async def process_generic_proxy_job(client, job):
    """Restored logic for generic API proxying"""
    job_id = job['job_id']
    target_url = job['target_api_url']
    method = job.get('request_method', 'GET').upper()
    headers = job.get('request_headers', {})
    webhook_url = job.get('callback_webhook_url') or job.get('webhook_url') # Support both naming conventions
    
    logger.info(f"🔄 Processing GENERIC Job {job_id} -> {target_url}")

    try:
        if method == 'POST':
            response = await client.post(target_url, headers=headers, json=job.get('request_body'), timeout=30.0)
        else:
            response = await client.get(target_url, headers=headers, timeout=30.0)

        try: response_data = response.json()
        except: response_data = {"raw_text": response.text}

        response_data = apply_endpoint_transformation(target_url, response_data, job.get('opcja'))

        # Prepare Webhook
        if webhook_url:
            payload = {
                "job_id": job_id,
                "status": "success",
                "api_status_code": response.status_code,
                "api_response": response_data,
                "processed_at": time.time()
            }
            payload = truncate_to_size_limit(payload)
            await client.post(webhook_url, json=payload, timeout=10.0)
            logger.info(f"✓ Generic Webhook sent for {job_id}")

        await update_job_status(job_id, "completed", result=response_data)

    except Exception as e:
        logger.error(f"Generic Job {job_id} failed: {e}")
        if webhook_url:
            await client.post(webhook_url, json={"job_id": job_id, "status": "error", "error": str(e)}, timeout=10.0)
        await update_job_status(job_id, "failed", error=str(e))


async def process_linkedin_scraper_job(client, job):
    """New logic for specific LinkedIn scraping"""
    job_id = job.get('job_id')
    username = job.get('username')
    urn = job.get('urn')
    webhook_url = job.get('webhook_url') or job.get('callback_webhook_url')
    
    logger.info(f"🚀 Processing LINKEDIN Job {job_id} | User: {username or urn}")

    results = {
        'username': username,
        'experiences': '', 'profile': None, 'urn': urn,
        'verified_full_name': None, 'reaction_count': 0, 'post_count': 0,
        'user_reshares': 0, 'posts': '', 'success': False, 'errors': []
    }

    headers = {
        'x-rapidapi-key': job.get('api_key'),
        'x-rapidapi-host': job.get('api_host', 'fresh-linkedin-scraper-api.p.rapidapi.com')
    }

    try:
        # 1. Profile / ID Resolution
        profile_data = None
        should_fetch_profile = (not urn) or any(job.get(p) for p in PROFILE_OPTIONAL_PARAMS)
        
        if should_fetch_profile and username:
            params = {'username': username}
            for p in PROFILE_OPTIONAL_PARAMS:
                if str(job.get(p, '')).lower() == 'true': params[p] = 'true'
            profile_data = await make_api_call(client, PROFILE_API, headers, params)
        elif should_fetch_profile and not username:
            results['errors'].append("Username required for profile fetch")

        if not urn and profile_data:
            urn = profile_data.get('data', {}).get('urn')
            results['urn'] = urn
            # Name resolution
            d = profile_data.get('data', {})
            results['verified_full_name'] = f"{d.get('firstName','')} {d.get('lastName','')}".strip() or d.get('full_name')

        if not results['verified_full_name']: 
            results['verified_full_name'] = job.get('full_name')

        # Experiences
        if str(job.get('include_experiences', '')).lower() == 'true' and profile_data:
            exps = profile_data.get('data', {}).get('experiences', [])
            formatted = [f"Role: {e.get('title')}\nCompany: {e.get('company',{}).get('name')}\nDate: {e.get('date',{}).get('start')} - {e.get('date',{}).get('end','Present')}" for e in exps]
            results['experiences'] = '\n\n'.join(formatted)

        # 2. Activity (Posts/Reactions)
        if urn:
            await asyncio.sleep(1) # Polite delay
            target_name_norm = normalize_text(results['verified_full_name'])
            days = int(job.get('posted_max_days_ago', 30))

            # Reactions
            r_data = await make_api_call(client, REACTIONS_API, headers, {'urn': urn, 'page': 1})
            if r_data:
                data = r_data.get('data', []) if isinstance(r_data.get('data'), list) else []
                results['reaction_count'] = len([r for r in data if r.get('post',{}).get('created_at') and is_within_last_n_days(r['post']['created_at'], 30)])

            # Posts
            await asyncio.sleep(1)
            p_data = await make_api_call(client, POSTS_API, headers, {'urn': urn, 'page': 1})
            if p_data:
                data = p_data.get('data', []) if isinstance(p_data.get('data'), list) else []
                valid_posts = []
                for p in data:
                    if not is_within_last_n_days(p.get('created_at'), days): continue
                    
                    # Reshare detection
                    author = p.get('author', {})
                    is_reshare = False
                    if author.get('account_type') == 'company': is_reshare = True
                    else:
                        aname = author.get('full_name') or f"{author.get('first_name','')} {author.get('last_name','')}"
                        if normalize_text(aname) != target_name_norm: is_reshare = True
                    
                    if is_reshare:
                        results['user_reshares'] += 1
                        continue
                        
                    valid_posts.append(f"[{p.get('created_at')[:10]}] {p.get('text','')}\n{p.get('url','')}")
                
                results['posts'] = '\n\n'.join(valid_posts)
                results['post_count'] = len(valid_posts)

        results['success'] = results['urn'] is not None and not results['errors']
        
        # Webhook
        if webhook_url:
            await client.post(webhook_url, json=results, timeout=10.0)
            logger.info(f"✓ LinkedIn Webhook sent for {job_id}")

        await update_job_status(job_id, "completed", result=results)

    except Exception as e:
        logger.error(f"LinkedIn Job {job_id} failed: {e}")
        if webhook_url:
            results['errors'].append(str(e))
            await client.post(webhook_url, json=results, timeout=10.0)
        await update_job_status(job_id, "failed", error=str(e))

# ==================== MAIN DISPATCHER ====================

async def queue_worker():
    logger.info("👷 Worker started (Hybrid Mode). Waiting for jobs...")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                if redis_client:
                    result = await redis_client.blpop(REDIS_QUEUE_KEY, timeout=0)
                    if result:
                        _, raw_data = result
                        job = json.loads(raw_data)
                        start_time = time.monotonic()
                        
                        # === DISPATCHER LOGIC ===
                        if "target_api_url" in job:
                            await process_generic_proxy_job(client, job)
                        elif "username" in job or "urn" in job:
                            await process_linkedin_scraper_job(client, job)
                        else:
                            logger.error(f"❌ Unknown job format: {job.keys()}")
                            await update_job_status(job.get('job_id'), "failed", error="Unknown job format")

                        elapsed = time.monotonic() - start_time
                        sleep_time = max(0, SECONDS_BETWEEN_REQUESTS - elapsed)
                        if sleep_time > 0: await asyncio.sleep(sleep_time)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Queue Worker error: {e}")
                await asyncio.sleep(1)

# ==================== LIFESPAN & API ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("✅ Redis connected")
        asyncio.create_task(queue_worker())
    except Exception as e: logger.critical(f"❌ Redis connection failed: {e}")
    yield
    if redis_client: await redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.post("/process")
async def queue_job(request: Request, response: Response):
    try: body = await request.json()
    except: return {"error": "Invalid JSON"}

    # Normalization for Generic Proxy (restore backwards compatibility)
    if "target_api" in body and "target_api_url" not in body: body["target_api_url"] = body.pop("target_api")
    if "callback_webhook" in body and "callback_webhook_url" not in body: body["callback_webhook_url"] = body.pop("callback_webhook")
    
    job_id = str(uuid.uuid4())
    body['job_id'] = job_id
    body['queued_at'] = time.time()

    if redis_client:
        await update_job_status(job_id, "queued")
        pos = await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(body))
        return {"job_id": job_id, "status": "queued", "position": pos}
    
    response.status_code = 503
    return {"error": "Redis not available"}

@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    if redis_client:
        data = await redis_client.get(f"{REDIS_JOB_PREFIX}{job_id}")
        if data: return json.loads(data)
    return {"status": "not_found"}

@app.get("/health")
async def health():
    return {"status": "ok", "queue_length": await redis_client.llen(REDIS_QUEUE_KEY) if redis_client else 0}
