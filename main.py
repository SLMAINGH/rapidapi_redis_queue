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
REDIS_QUEUE_KEY = "linkedin_scraper_queue"

# Rate Limit: How many seconds to wait between processing jobs?
# Set to 3.0 or 4.0 to be safe with RapidAPI limits
SECONDS_BETWEEN_JOBS = float(os.getenv("RATE_LIMIT_DELAY", "3.0"))

# Endpoints
PROFILE_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/profile"
REACTIONS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/reactions"
POSTS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/posts"

redis_client: Optional[redis.Redis] = None

# Optional Params List
PROFILE_OPTIONAL_PARAMS = [
    'include_follower_and_connection', 'include_experiences', 'include_skills',
    'include_certifications', 'include_publications', 'include_educations',
    'include_volunteers', 'include_honors', 'include_interests', 'include_bio'
]

# ==================== HELPER LOGIC ====================

def is_within_last_n_days(timestamp_str, days=30):
    try:
        if not timestamp_str: return False
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str.replace('Z', '+00:00')
        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        return timestamp >= cutoff_date
    except Exception as e:
        logger.warning(f"Error parsing timestamp {timestamp_str}: {e}")
        return False

def normalize_text(text):
    if not text: return ""
    return text.strip().lower()

async def make_api_call(client, url, headers, params):
    """Async wrapper for API calls with basic retry logic"""
    for attempt in range(3):
        try:
            resp = await client.get(url, headers=headers, params=params, timeout=15.0)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                logger.warning("Hit 429 Rate Limit. Sleeping 5s...")
                await asyncio.sleep(5)
                continue
            if resp.status_code == 404:
                return None
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                logger.error(f"API call failed after retries: {e}")
    return None

# ==================== WORKER PROCESSOR ====================

async def process_job(job: Dict[str, Any]):
    """
    Executes the main scraping logic.
    """
    job_id = job.get('job_id')
    username = job.get('username')
    api_key = job.get('api_key')
    webhook_url = job.get('webhook_url')
    provided_urn = job.get('urn')
    input_full_name = job.get('full_name')
    posted_max_days_ago = int(job.get('posted_max_days_ago') or 30)
    
    logger.info(f"🚀 Processing Job {job_id} | User: {username or provided_urn}")

    results = {
        'username': username,
        'experiences': '', 
        'profile': None,
        'urn': None,
        'verified_full_name': None,
        'reaction_count': 0,
        'post_count': 0,
        'user_reshares': 0,
        'posts': '',
        'success': False,
        'errors': []
    }

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': job.get('api_host', 'fresh-linkedin-scraper-api.p.rapidapi.com')
    }

    async with httpx.AsyncClient() as client:
        try:
            # --- PHASE 1: IDENTIFICATION & PROFILE ---
            profile_data = None
            
            # Check if we need to fetch profile (either no URN provided OR user wants extra fields)
            extra_fields_requested = any(job.get(param) for param in PROFILE_OPTIONAL_PARAMS)
            should_fetch_profile = (not provided_urn) or extra_fields_requested

            if should_fetch_profile:
                if username:
                    params = {'username': username}
                    # Add requested optional params
                    for param in PROFILE_OPTIONAL_PARAMS:
                        val = job.get(param)
                        if val is True or str(val).lower() == 'true':
                            params[param] = 'true'
                    
                    profile_data = await make_api_call(client, PROFILE_API, headers, params)
                else:
                    results['errors'].append("Username required to fetch profile details")

            # Resolve URN and Name
            urn = None
            if provided_urn:
                urn = provided_urn
                results['urn'] = urn
                results['verified_full_name'] = input_full_name
            else:
                if profile_data:
                    results['profile'] = profile_data
                    data_obj = profile_data.get('data', {})
                    urn = data_obj.get('urn')
                    
                    # Name resolution
                    first = data_obj.get('firstName', '')
                    last = data_obj.get('lastName', '')
                    profile_full_name = f"{first} {last}".strip()
                    if not profile_full_name:
                        profile_full_name = data_obj.get('full_name', '')
                    
                    results['verified_full_name'] = profile_full_name if profile_full_name else input_full_name
                    
                    if urn:
                        results['urn'] = urn
                    else:
                        results['errors'].append('URN not found in profile')
                else:
                     if not provided_urn: results['errors'].append('Failed to fetch profile and no URN provided')

            # Extract Experiences if requested
            include_experiences = job.get('include_experiences', False)
            if (include_experiences is True or str(include_experiences).lower() == 'true') and profile_data:
                data_obj = profile_data.get('data', {})
                raw_experiences = data_obj.get('experiences', [])
                formatted_list = []
                for exp in raw_experiences:
                    title = exp.get('title', 'Unknown Title')
                    company = exp.get('company', {}).get('name', 'Unknown Company')
                    start = exp.get('date', {}).get('start', 'Unknown')
                    end = exp.get('date', {}).get('end', 'Present')
                    exp_str = f"Role: {title}\nCompany: {company}\nDate: {start} - {end}"
                    formatted_list.append(exp_str)
                results['experiences'] = '\n\n'.join(formatted_list)
                logger.info(f"✓ Extracted {len(formatted_list)} experiences")

            # --- PHASE 2: POSTS & REACTIONS (Only if we have a URN) ---
            if urn and results['verified_full_name']:
                target_name_norm = normalize_text(results['verified_full_name'])
                
                # 1. Reactions
                logger.info(f"Fetching reactions for {urn}...")
                reactions_data = await make_api_call(client, REACTIONS_API, headers, {'urn': urn, 'page': 1})
                if reactions_data:
                    data_content = reactions_data.get('data')
                    if isinstance(data_content, list):
                        filtered = [
                            r for r in data_content 
                            if r.get('post', {}).get('created_at') and 
                            is_within_last_n_days(r['post']['created_at'], 30)
                        ]
                        results['reaction_count'] = len(filtered)
                    elif isinstance(data_content, dict):
                        results['reaction_count'] = data_content.get('count', 0)

                # 2. Posts
                # Small sleep to be kind to the API
                await asyncio.sleep(1)
                
                logger.info(f"Fetching posts for {urn}...")
                posts_data = await make_api_call(client, POSTS_API, headers, {'urn': urn, 'page': 1})
                if posts_data:
                    data_content = posts_data.get('data')
                    if isinstance(data_content, list):
                        filtered_posts = []
                        for p in data_content:
                            created_at = p.get('created_at')
                            if not created_at or not is_within_last_n_days(created_at, posted_max_days_ago):
                                continue
                            
                            # Author Check Logic
                            author_obj = p.get('author', {})
                            is_reshare = False
                            
                            if author_obj.get('account_type') == 'company':
                                is_reshare = True
                            elif author_obj.get('account_type') == 'user':
                                auth_name = author_obj.get('full_name') or f"{author_obj.get('first_name','')} {author_obj.get('last_name','')}".strip()
                                # Normalize both to check equality
                                if normalize_text(auth_name) != target_name_norm:
                                    is_reshare = True
                            
                            if is_reshare:
                                results['user_reshares'] += 1
                                continue
                                
                            date_only = created_at[:10] if created_at else ''
                            filtered_posts.append(f"[{date_only}] {p.get('text', '')}\n{p.get('url', '')}")
                        
                        results['posts'] = '\n\n'.join(filtered_posts)
                        results['post_count'] = len(filtered_posts)
                    elif isinstance(data_content, dict):
                        results['post_count'] = data_content.get('count', 0)
            
            results['success'] = results['urn'] is not None and len(results['errors']) == 0
            logger.info(f"✓ Job {job_id} Finished. Success: {results['success']}")

            # --- PHASE 3: WEBHOOK ---
            if webhook_url:
                logger.info(f"📤 Sending webhook to {webhook_url}")
                try:
                    await client.post(webhook_url, json=results, timeout=10.0)
                    logger.info("✓ Webhook sent successfully")
                except Exception as e:
                    logger.error(f"Failed to send webhook: {e}")

        except Exception as e:
            logger.error(f"Critical error in job {job_id}: {e}")
            if webhook_url:
                try:
                    await client.post(webhook_url, json={"error": str(e), "success": False, "username": username}, timeout=5.0)
                except: pass

# ==================== QUEUE WORKER ====================

async def queue_worker():
    logger.info("👷 Worker started. Waiting for jobs...")
    while True:
        try:
            if redis_client:
                # Pop job from Redis
                result = await redis_client.blpop(REDIS_QUEUE_KEY, timeout=0)
                if result:
                    _, raw_data = result
                    job = json.loads(raw_data)
                    
                    start_time = time.monotonic()
                    
                    # Process
                    await process_job(job)
                    
                    # Rate Limit Delay
                    elapsed = time.monotonic() - start_time
                    sleep_time = max(0, SECONDS_BETWEEN_JOBS - elapsed)
                    if sleep_time > 0:
                        logger.info(f"💤 Sleeping {sleep_time:.2f}s for rate limits...")
                        await asyncio.sleep(sleep_time)
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
    except Exception as e:
        logger.critical(f"❌ Redis connection failed: {e}")
    yield
    if redis_client:
        await redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.post("/process")
async def queue_job(request: Request, response: Response):
    try:
        body = await request.json()
    except:
        return {"error": "Invalid JSON"}

    job_id = str(uuid.uuid4())
    body['job_id'] = job_id
    body['queued_at'] = time.time()

    if redis_client:
        # Push to Redis
        pos = await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(body))
        return {
            "job_id": job_id,
            "status": "queued",
            "position": pos,
            "message": "Job added to queue. Results will be sent to webhook_url."
        }
    
    response.status_code = 503
    return {"error": "Redis not available"}

@app.get("/health")
async def health():
    q_len = await redis_client.llen(REDIS_QUEUE_KEY) if redis_client else 0
    return {"status": "ok", "queue_length": q_len}

@app.get("/")
async def root():
    return {"service": "LinkedIn Scraper Queue Worker"}
