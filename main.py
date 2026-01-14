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
from datetime import datetime, timedelta, timezone

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

# LinkedIn API endpoints
PROFILE_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/profile"
REACTIONS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/reactions"
POSTS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/posts"

PROFILE_OPTIONAL_PARAMS = [
    'include_follower_and_connection',
    'include_experiences',
    'include_skills',
    'include_certifications',
    'include_publications',
    'include_educations',
    'include_volunteers',
    'include_honors',
    'include_interests',
    'include_bio'
]


# ==================== LINKEDIN HELPERS ====================

def is_within_last_n_days(timestamp_str, days=30):
    try:
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str.replace('Z', '+00:00')
        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        return timestamp >= cutoff_date
    except Exception as e:
        logger.warning(f"Error parsing timestamp {timestamp_str}: {str(e)}")
        return False


def normalize_text(text):
    if not text:
        return ""
    return text.strip().lower()


async def make_linkedin_api_call(client: httpx.AsyncClient, url: str, headers: Dict, params: Dict, max_retries=3):
    """Make LinkedIn API call with retries"""
    for attempt in range(max_retries):
        try:
            response = await client.get(url, headers=headers, params=params, timeout=15.0)
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                return None
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            logger.error(f"API call failed after {max_retries} attempts: {e}")
            return None
    return None


async def process_linkedin_scraper_job(job: Dict[str, Any], client: httpx.AsyncClient):
    """Process LinkedIn scraper job with all the original logic"""
    
    username = job.get('username')
    api_key = job['api_key']
    api_host = job.get('api_host', 'fresh-linkedin-scraper-api.p.rapidapi.com')
    webhook_url = job.get('webhook_url')
    provided_urn = job.get('urn')
    posted_max_days_ago = int(job.get('posted_max_days_ago', 30))
    input_full_name = job.get('full_name')

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': api_host
    }

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

    # --- PHASE 1: IDENTIFICATION ---
    profile_data = None
    extra_fields_requested = any(job.get(param) for param in PROFILE_OPTIONAL_PARAMS)
    should_fetch_profile = (not provided_urn) or extra_fields_requested

    if should_fetch_profile:
        logger.info(f"📋 Step 1: Fetching profile for {username}")
        if username:
            params = {'username': username}
            
            for param in PROFILE_OPTIONAL_PARAMS:
                val = job.get(param)
                if val is True or str(val).lower() == 'true':
                    params[param] = 'true'
            
            profile_data = await make_linkedin_api_call(client, PROFILE_API, headers, params)
        else:
            results['errors'].append("Username required to fetch profile details")

    if provided_urn:
        if not input_full_name:
            results['errors'].append("If URN is provided, 'full_name' must also be supplied.")
            urn = None
        else:
            urn = provided_urn
            results['urn'] = urn
            results['verified_full_name'] = input_full_name
            logger.info(f"📋 Using provided URN: {urn}")
    else:
        if not profile_data:
            results['errors'].append('Failed to fetch profile')
            urn = None
        else:
            results['profile'] = profile_data
            data_obj = profile_data.get('data', {})
            urn = data_obj.get('urn')
            
            first = data_obj.get('firstName', '')
            last = data_obj.get('lastName', '')
            profile_full_name = f"{first} {last}".strip()
            if not profile_full_name:
                profile_full_name = data_obj.get('full_name', '')

            results['verified_full_name'] = profile_full_name if profile_full_name else input_full_name

            if not urn:
                results['errors'].append('URN not found in profile')
            else:
                results['urn'] = urn
                logger.info(f"✓ Target Identity: {results['verified_full_name']}")

    # Process experiences
    include_experiences = job.get('include_experiences', False)
    if (include_experiences is True or str(include_experiences).lower() == 'true') and profile_data:
        data_obj = profile_data.get('data', {})
        raw_experiences = data_obj.get('experiences', [])
        
        formatted_experiences_list = []
        for exp in raw_experiences:
            title = exp.get('title', 'Unknown Title')
            company_name = exp.get('company', {}).get('name', 'Unknown Company')
            location = exp.get('location', '')
            
            date_obj = exp.get('date', {})
            start_date = date_obj.get('start', 'Unknown')
            end_date = date_obj.get('end', 'Present')
            
            exp_str = f"Role: {title}\nCompany: {company_name}\nDate: {start_date} - {end_date}"
            if location:
                exp_str += f"\nLocation: {location}"
            
            formatted_experiences_list.append(exp_str)

        results['experiences'] = '\n\n'.join(formatted_experiences_list)
        logger.info(f"✓ Extracted {len(formatted_experiences_list)} experiences")

    await asyncio.sleep(2)

    # --- PHASE 2: PROCESSING ---
    if urn and results['verified_full_name']:
        target_name_norm = normalize_text(results['verified_full_name'])
        
        # Reactions
        logger.info(f"📋 Fetching reactions for URN {urn}")
        reactions_data = await make_linkedin_api_call(client, REACTIONS_API, headers, {'urn': urn, 'page': 1})

        if reactions_data:
            if isinstance(reactions_data.get('data'), list):
                all_reactions = reactions_data['data']
                filtered_reactions = [
                    r for r in all_reactions
                    if r.get('post', {}).get('created_at') and
                    is_within_last_n_days(r['post']['created_at'], 30)
                ]
                results['reaction_count'] = len(filtered_reactions)
            elif isinstance(reactions_data.get('data'), dict):
                results['reaction_count'] = reactions_data['data'].get('count', 0)

        await asyncio.sleep(2)

        # Posts
        logger.info(f"📋 Fetching posts for URN {urn}")
        posts_data = await make_linkedin_api_call(client, POSTS_API, headers, {'urn': urn, 'page': 1})

        if posts_data:
            if isinstance(posts_data.get('data'), list):
                all_posts = posts_data['data']
                filtered_posts = []

                for p in all_posts:
                    created_at = p.get('created_at')
                    if not created_at or not is_within_last_n_days(created_at, posted_max_days_ago):
                        continue

                    author_obj = p.get('author', {})
                    author_account_type = author_obj.get('account_type')
                    is_reshare = False

                    if author_account_type == 'company':
                        is_reshare = True
                    elif author_account_type == 'user':
                        post_author_name = author_obj.get('full_name')
                        if not post_author_name:
                            f_name = author_obj.get('first_name', '')
                            l_name = author_obj.get('last_name', '')
                            post_author_name = f"{f_name} {l_name}".strip()

                        if not post_author_name or normalize_text(post_author_name) != target_name_norm:
                            is_reshare = True

                    if is_reshare:
                        results['user_reshares'] += 1
                        continue

                    date_only = created_at[:10] if created_at else ''
                    post_url = p.get('url', '')
                    post_text = p.get('text', '')
                    filtered_posts.append(f"[{date_only}] {post_text}\n{post_url}")

                results['posts'] = '\n\n'.join(filtered_posts)
                results['post_count'] = len(filtered_posts)

            elif isinstance(posts_data.get('data'), dict):
                results['post_count'] = posts_data['data'].get('count', 0)
        else:
            results['errors'].append('Failed to fetch posts')

    elif urn and not results['verified_full_name']:
        results['errors'].append('Verification failed: No full name available.')

    results['success'] = results['urn'] is not None and len(results['errors']) == 0
    logger.info(f"✓ Completed LinkedIn processing for {username or urn}")

    return results


# ==================== TRANSFORMATIONS ====================

def transform_linkedin_reactions_text_only(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only the 'text' field from LinkedIn reactions posts and concatenate them."""
    if not isinstance(response_data, dict):
        return response_data

    result = response_data.copy()

    if 'data' in result and isinstance(result['data'], list):
        texts = []
        for item in result['data']:
            if isinstance(item, dict) and 'post' in item and isinstance(item['post'], dict):
                text = item['post'].get('text', '')
                if text:
                    texts.append(text)

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
    """Process a single job - handles both generic API calls and LinkedIn scraper jobs."""
    job_id = job['job_id']
    job_type = job.get('job_type', 'generic')
    webhook_url = job.get('callback_webhook_url') or job.get('webhook_url')

    logger.info(f"Processing {job_id} (type: {job_type})")

    async with httpx.AsyncClient() as client:
        try:
            # LinkedIn scraper job
            if job_type == 'linkedin_scraper':
                logger.info(f"Processing LinkedIn scraper job {job_id}")
                results = await process_linkedin_scraper_job(job, client)
                
                # Send webhook
                if webhook_url:
                    try:
                        wb_resp = await client.post(webhook_url, json=results, timeout=10.0)
                        logger.info(f"Webhook sent for {job_id}: {wb_resp.status_code}")
                    except Exception as e:
                        logger.error(f"Webhook delivery failed for {job_id}: {e}")
                
                await update_job_status(job_id, "completed", result=results)
                return

            # Generic API proxy job
            target_url = job['target_api_url']
            method = job.get('request_method', 'GET').upper()
            request_body = job.get('request_body')
            opcja = job.get('opcja')

            headers = GLOBAL_HEADERS.copy()
            if job.get('request_headers'):
                headers.update(job['request_headers'])

            logger.info(f"Processing generic API job {job_id} -> {method} {target_url}")

            if method == 'POST':
                response = await client.post(target_url, headers=headers, json=request_body, timeout=30.0)
            elif method == 'PUT':
                response = await client.put(target_url, headers=headers, json=request_body, timeout=30.0)
            elif method == 'DELETE':
                response = await client.delete(target_url, headers=headers, timeout=30.0)
            else:
                response = await client.get(target_url, headers=headers, timeout=30.0)

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {"raw_text": response.text}

            response_data = apply_endpoint_transformation(target_url, response_data, opcja)
            logger.info(f"Success {job_id}: {response.status_code}")

            webhook_payload = job.copy()
            webhook_payload.pop("request_headers", None)
            webhook_payload.pop("request_body", None)
            webhook_payload["api_status_code"] = response.status_code
            webhook_payload["api_response"] = response_data
            webhook_payload["processed_at"] = time.time()
            webhook_payload["status"] = "success"

            webhook_payload = truncate_to_size_limit(webhook_payload)
            
            if webhook_url:
                try:
                    wb_resp = await client.post(webhook_url, json=webhook_payload, timeout=10.0)
                    logger.info(f"Webhook sent for {job_id}: {wb_resp.status_code}")
                except Exception as e:
                    logger.error(f"Webhook delivery failed for {job_id}: {e}")

            await update_job_status(job_id, "completed", result=response_data)

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            
            if webhook_url:
                error_payload = {
                    "job_id": job_id,
                    "status": "error",
                    "error_message": str(e),
                    "processed_at": time.time()
                }
                if job_type == 'linkedin_scraper':
                    error_payload['username'] = job.get('username')
                    error_payload['urn'] = job.get('urn')
                else:
                    error_payload['target_url'] = job.get('target_api_url')
                
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
                        await asyncio.wait_for(process_job(job), timeout=60.0)
                    except asyncio.TimeoutError:
                        logger.error(f"Job {job.get('job_id')} timed out")
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
    """
    Queue a job. Supports two types:
    1. Generic API proxy: Requires target_api_url
    2. LinkedIn scraper: Requires username or urn + full_name
    """
    try:
        body = await request.json()
    except Exception:
        response.status_code = 400
        return {"error": "Invalid JSON"}

    # Detect job type
    is_linkedin_job = (
        ('username' in body or 'urn' in body) and
        'api_key' in body and
        'target_api_url' not in body
    )

    if is_linkedin_job:
        # LinkedIn scraper job
        if not body.get('username') and not body.get('urn'):
            response.status_code = 400
            return {"error": "username or urn is required for LinkedIn scraper"}
        
        if not body.get('api_key'):
            response.status_code = 400
            return {"error": "api_key is required"}
        
        if body.get('urn') and not body.get('full_name'):
            response.status_code = 400
            return {"error": "full_name is required when providing URN"}

        body['job_type'] = 'linkedin_scraper'
        
    else:
        # Generic API proxy job
        if "target_api_url" not in body:
            if "target_api" in body:
                body["target_api_url"] = body.pop("target_api")
            else:
                response.status_code = 400
                return {"error": "Missing required field: target_api_url"}

        if "callback_webhook_url" not in body and "callback_webhook" in body:
            body["callback_webhook_url"] = body.pop("callback_webhook")

        if "request_headers" not in body and "headers" in body:
            body["request_headers"] = body.pop("headers")

        if body.get("target_api_url"):
            body["target_api_url"] = str(body["target_api_url"]).strip()

        body['job_type'] = 'generic'

    job_id = str(uuid.uuid4())
    body['job_id'] = job_id
    body['queued_at'] = time.time()

    queue_position = -1
    if redis_client:
        await update_job_status(job_id, "queued")
        queue_position = await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(body))

    logger.info(f"Queued {job_id} (type: {body['job_type']}) at position {queue_position}")

    response.status_code = 202
    return {
        "job_id": job_id,
        "status": "queued",
        "job_type": body['job_type'],
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
        "service": "Rate-Limited API Proxy with LinkedIn Scraper",
        "endpoints": {
            "POST /process": "Queue a new job (generic API or LinkedIn scraper)",
            "GET /status/{job_id}": "Check job status",
            "GET /health": "Service health check"
        },
        "job_types": {
            "generic": "Requires target_api_url",
            "linkedin_scraper": "Requires username or urn + full_name, api_key"
        }
    }
