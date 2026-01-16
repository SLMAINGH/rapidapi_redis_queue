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

# LinkedIn API key from environment
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "fresh-linkedin-scraper-api.p.rapidapi.com")

redis_client: Optional[redis.Redis] = None
MAX_WEBHOOK_SIZE_BYTES = 100 * 1024  # 100 KB
WEBHOOK_DELAY_SECONDS = 1.0  # Delay between individual post webhooks

# LinkedIn API endpoints
PROFILE_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/profile"
REACTIONS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/reactions"
POSTS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/user/posts"
SEARCH_POSTS_API = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/search/posts"

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

def get_rapidapi_headers(api_host: Optional[str] = None) -> Dict[str, str]:
    """Get RapidAPI headers using environment variable."""
    if not RAPIDAPI_KEY:
        raise ValueError("RAPIDAPI_KEY environment variable is not set")
    return {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': api_host or RAPIDAPI_HOST
    }


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
    api_host = job.get('api_host', RAPIDAPI_HOST)
    webhook_url = job.get('webhook_url')
    provided_urn = job.get('urn')
    posted_max_days_ago = int(job.get('posted_max_days_ago', 30))
    input_full_name = job.get('full_name')

    headers = get_rapidapi_headers(api_host)

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


async def process_search_posts_job(job: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Process LinkedIn search posts job.
    Fetches one page, sends individual webhooks per post, queues next page if needed.
    """
    job_id = job['job_id']
    search_session_id = job.get('search_session_id', job_id)
    keyword = job['keyword']
    webhook_url = job.get('webhook_url')
    page = job.get('page', 1)
    
    # Optional API parameters
    date_posted = job.get('date_posted')  # past_month, past_week, past_24h
    sort_by = job.get('sort_by')  # date_posted, relevance
    content_type = job.get('content_type')  # videos, photos, jobs, live_videos, documents, collaborative_articles
    custom_days_ago = job.get('custom_days_ago')  # Custom date filter (e.g., 90)
    
    # Retry tracking
    retry_count = job.get('_retry_count', 0)
    max_retries = 3
    
    headers = get_rapidapi_headers()
    
    # Build API params
    params = {
        'keyword': keyword,
        'page': page
    }
    
    if custom_days_ago:
        # For custom date range, always sort by date to get chronological results
        params['sort_by'] = 'date_posted'
    else:
        if date_posted:
            params['date_posted'] = date_posted
        if sort_by:
            params['sort_by'] = sort_by
    
    if content_type:
        params['content_type'] = content_type
    
    logger.info(f"🔍 Search posts: keyword='{keyword}', page={page}, session={search_session_id}")
    
    # Make API call
    response_data = await make_linkedin_api_call(client, SEARCH_POSTS_API, headers, params)
    
    if not response_data:
        logger.error(f"Search posts API call failed for page {page}")
        
        # Retry logic
        if retry_count < max_retries:
            logger.info(f"Retrying page {page} (attempt {retry_count + 1}/{max_retries})")
            retry_job = job.copy()
            retry_job['_retry_count'] = retry_count + 1
            retry_job['job_id'] = f"{search_session_id}_page_{page}_retry_{retry_count + 1}"
            
            if redis_client:
                await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(retry_job))
            
            return {
                'success': False,
                'error': 'API call failed, queued for retry',
                'search_session_id': search_session_id,
                'page': page,
                'retry_count': retry_count + 1
            }
        else:
            # Send error webhook
            if webhook_url:
                error_payload = {
                    'search_session_id': search_session_id,
                    'keyword': keyword,
                    'page': page,
                    'status': 'error',
                    'error': f'API call failed after {max_retries} retries'
                }
                try:
                    await client.post(webhook_url, json=error_payload, timeout=10.0)
                except Exception as e:
                    logger.error(f"Error webhook failed: {e}")
            
            return {
                'success': False,
                'error': f'API call failed after {max_retries} retries',
                'search_session_id': search_session_id,
                'page': page
            }
    
    posts = response_data.get('data', [])
    has_more = response_data.get('has_more', False)
    total = response_data.get('total', 0)
    
    logger.info(f"✓ Fetched {len(posts)} posts from page {page} (total: {total}, has_more: {has_more})")
    
    # Track if we should continue to next page
    should_continue = has_more
    posts_sent = 0
    cutoff_reached = False
    
    # Send individual webhooks for each post
    if webhook_url and posts:
        cutoff_date = None
        if custom_days_ago:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=int(custom_days_ago))
        
        for post in posts:
            # Check custom date filter
            if cutoff_date:
                created_at = post.get('created_at')
                if created_at:
                    try:
                        if created_at.endswith('Z'):
                            created_at = created_at.replace('Z', '+00:00')
                        post_date = datetime.fromisoformat(created_at)
                        if post_date.tzinfo is None:
                            post_date = post_date.replace(tzinfo=timezone.utc)
                        
                        if post_date < cutoff_date:
                            logger.info(f"Post from {created_at} is older than {custom_days_ago} days, stopping")
                            should_continue = False
                            cutoff_reached = True
                            break
                    except Exception as e:
                        logger.warning(f"Error parsing post date {created_at}: {e}")
            
            # Build webhook payload for individual post
            post_payload = {
                'search_session_id': search_session_id,
                'keyword': keyword,
                'page': page,
                'post': post
            }
            
            try:
                await client.post(webhook_url, json=post_payload, timeout=10.0)
                posts_sent += 1
                logger.debug(f"Webhook sent for post {post.get('id', 'unknown')}")
            except Exception as e:
                logger.error(f"Webhook failed for post {post.get('id', 'unknown')}: {e}")
            
            # Rate limit between webhook calls
            await asyncio.sleep(WEBHOOK_DELAY_SECONDS)
    
    logger.info(f"✓ Sent {posts_sent} webhooks for page {page}")
    
    # Queue next page if needed
    if should_continue and not cutoff_reached:
        next_page_job = {
            'job_type': 'search_posts',
            'job_id': f"{search_session_id}_page_{page + 1}",
            'search_session_id': search_session_id,
            'keyword': keyword,
            'webhook_url': webhook_url,
            'page': page + 1,
            'queued_at': time.time()
        }
        
        # Carry over optional params
        if date_posted:
            next_page_job['date_posted'] = date_posted
        if sort_by and not custom_days_ago:
            next_page_job['sort_by'] = sort_by
        if content_type:
            next_page_job['content_type'] = content_type
        if custom_days_ago:
            next_page_job['custom_days_ago'] = custom_days_ago
        
        if redis_client:
            await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(next_page_job))
            logger.info(f"📋 Queued next page: {page + 1}")
    else:
        # Send completion webhook
        if webhook_url:
            completion_payload = {
                'search_session_id': search_session_id,
                'keyword': keyword,
                'status': 'completed',
                'final_page': page,
                'reason': 'cutoff_date_reached' if cutoff_reached else 'no_more_results'
            }
            try:
                await client.post(webhook_url, json=completion_payload, timeout=10.0)
                logger.info(f"✓ Sent completion webhook for session {search_session_id}")
            except Exception as e:
                logger.error(f"Completion webhook failed: {e}")
    
    return {
        'success': True,
        'search_session_id': search_session_id,
        'keyword': keyword,
        'page': page,
        'posts_fetched': len(posts),
        'posts_sent': posts_sent,
        'has_more': has_more,
        'continued': should_continue and not cutoff_reached
    }


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
    """Process a single job - handles generic API calls, LinkedIn scraper, and search posts jobs."""
    job_id = job['job_id']
    job_type = job.get('job_type', 'generic')
    webhook_url = job.get('callback_webhook_url') or job.get('webhook_url')

    logger.info(f"Processing {job_id} (type: {job_type})")

    async with httpx.AsyncClient() as client:
        try:
            # Search posts job
            if job_type == 'search_posts':
                logger.info(f"Processing search posts job {job_id}")
                results = await process_search_posts_job(job, client)
                await update_job_status(job_id, "completed", result=results)
                return

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
                elif job_type == 'search_posts':
                    error_payload['keyword'] = job.get('keyword')
                    error_payload['search_session_id'] = job.get('search_session_id')
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
                        await asyncio.wait_for(process_job(job), timeout=300.0)  # Increased timeout for search jobs
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
        
        # Validate RAPIDAPI_KEY
        if not RAPIDAPI_KEY:
            logger.warning("RAPIDAPI_KEY not set - LinkedIn API calls will fail")
        else:
            logger.info("RAPIDAPI_KEY configured")
        
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
    Queue a job. Supports three types:
    1. Generic API proxy: Requires target_api_url
    2. LinkedIn scraper: Requires username or urn + full_name
    3. Search posts: Requires keyword
    """
    try:
        body = await request.json()
    except Exception:
        response.status_code = 400
        return {"error": "Invalid JSON"}

    # Detect job type
    is_search_posts_job = (
        'keyword' in body and
        'target_api_url' not in body and
        'username' not in body and
        'urn' not in body
    )
    
    is_linkedin_job = (
        ('username' in body or 'urn' in body) and
        'target_api_url' not in body
    )

    if is_search_posts_job:
        # Search posts job
        if not body.get('keyword'):
            response.status_code = 400
            return {"error": "keyword is required for search posts"}
        
        if not body.get('webhook_url'):
            response.status_code = 400
            return {"error": "webhook_url is required for search posts"}
        
        # Validate optional params
        valid_date_posted = ['past_month', 'past_week', 'past_24h']
        if body.get('date_posted') and body['date_posted'] not in valid_date_posted:
            response.status_code = 400
            return {"error": f"date_posted must be one of: {valid_date_posted}"}
        
        valid_sort_by = ['date_posted', 'relevance']
        if body.get('sort_by') and body['sort_by'] not in valid_sort_by:
            response.status_code = 400
            return {"error": f"sort_by must be one of: {valid_sort_by}"}
        
        valid_content_type = ['videos', 'photos', 'jobs', 'live_videos', 'documents', 'collaborative_articles']
        if body.get('content_type') and body['content_type'] not in valid_content_type:
            response.status_code = 400
            return {"error": f"content_type must be one of: {valid_content_type}"}
        
        if body.get('custom_days_ago'):
            try:
                days = int(body['custom_days_ago'])
                if days <= 0:
                    raise ValueError()
            except (ValueError, TypeError):
                response.status_code = 400
                return {"error": "custom_days_ago must be a positive integer"}

        body['job_type'] = 'search_posts'
        body['page'] = 1  # Always start from page 1
        
    elif is_linkedin_job:
        # LinkedIn scraper job
        if not body.get('username') and not body.get('urn'):
            response.status_code = 400
            return {"error": "username or urn is required for LinkedIn scraper"}
        
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
    
    # For search_posts, set search_session_id same as initial job_id
    if body['job_type'] == 'search_posts':
        body['search_session_id'] = job_id

    queue_position = -1
    if redis_client:
        await update_job_status(job_id, "queued")
        queue_position = await redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(body))

    logger.info(f"Queued {job_id} (type: {body['job_type']}) at position {queue_position}")

    result = {
        "job_id": job_id,
        "status": "queued",
        "job_type": body['job_type'],
        "queue_position": queue_position
    }
    
    # Include search_session_id for search_posts jobs
    if body['job_type'] == 'search_posts':
        result['search_session_id'] = body['search_session_id']

    response.status_code = 202
    return result


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
        "rate_limit": f"{60/SECONDS_BETWEEN_REQUESTS:.0f}/min",
        "rapidapi_configured": bool(RAPIDAPI_KEY)
    }


@app.get("/")
async def root():
    return {
        "service": "Rate-Limited API Proxy with LinkedIn Scraper",
        "endpoints": {
            "POST /process": "Queue a new job (generic API, LinkedIn scraper, or search posts)",
            "GET /status/{job_id}": "Check job status",
            "GET /health": "Service health check"
        },
        "job_types": {
            "generic": "Requires target_api_url",
            "linkedin_scraper": "Requires username or urn + full_name",
            "search_posts": "Requires keyword + webhook_url"
        },
        "search_posts_params": {
            "keyword": "Search term (required)",
            "webhook_url": "URL to receive individual posts (required)",
            "date_posted": "past_month | past_week | past_24h (optional)",
            "sort_by": "date_posted | relevance (optional)",
            "content_type": "videos | photos | jobs | live_videos | documents | collaborative_articles (optional)",
            "custom_days_ago": "Integer - custom date filter in days (optional, overrides date_posted)"
        }
    }
