# Rate-Limited API Proxy

A simple service that queues your API requests and sends them at a controlled pace. Perfect for APIs that have rate limits (like 20 requests per minute).

---

## Why Do You Need This?

Some APIs (like LinkedIn data providers) will block you if you send too many requests too quickly. This service acts as a "traffic cop" — it holds your requests in a queue and releases them one at a time at a safe speed.

**Without this proxy:** You send 100 requests → API blocks you after 20  
**With this proxy:** You send 100 requests → They get delivered smoothly over 5 minutes → No blocking

---

## How It Works

```
┌─────────┐      ┌─────────────┐      ┌────────────┐      ┌─────────────┐
│  Clay   │ ──►  │  This Proxy │ ──►  │ Target API │ ──►  │ Your Webhook│
└─────────┘      └─────────────┘      └────────────┘      └─────────────┘
                   (queues and                              (receives 
                    rate-limits)                            results)
```

1. **Clay sends a request** to your proxy with the target API URL
2. **Proxy queues it** and returns immediately with a job ID
3. **Proxy calls the target API** at a safe pace (e.g., every 3 seconds)
4. **Proxy sends results to your webhook** (back to Clay or wherever you want)

---

## Setting Up in Railway

### Step 1: Create a New Project

1. Go to railway, please use my referal link :) https://railway.com?referralCode=HwTrPp and sign in
2. Click **"New Project"** → **"Empty Project"**

### Step 2: Add Redis

1. Click **"+ Add Service"** → **"Database"** → **"Redis"**
2. Wait for it to deploy (takes about 30 seconds)

### Step 3: Add the Proxy Service

1. Click **"+ Add Service"** → **"GitHub Repo"**
2. Connect your repository (or use "Empty Service" and deploy via CLI)
3. Once added, click on the service → **"Settings"** → **"Generate Domain"**
4. Copy your domain (e.g., `your-proxy-abc123.railway.app`)

### Step 4: Configure Environment Variables

Click on your proxy service → **"Variables"** → Add these:

| Variable | Value | Description |
|----------|-------|-------------|
| `REDIS_URL` | `${{Redis.REDIS_URL}}` | Click the dropdown, select your Redis service |
| `RATE_LIMIT_DELAY` | `3.0` | Seconds between requests (3 = 20/minute) |
| `GLOBAL_HEADERS` | `{"x-api-key": "your-key"}` | Your API credentials (see below) |

**Setting GLOBAL_HEADERS for RapidAPI:**
```json
{"x-rapidapi-key": "abc123your-key-here", "x-rapidapi-host": "linkedin-api.p.rapidapi.com"}
```

### Step 5: Deploy

Railway should auto-deploy. Check the **"Deployments"** tab to see if it's running.

**Test it:** Visit `https://your-domain.railway.app/health` — you should see:
```json
{"status": "ok", "redis_connected": true, "queue_depth": 0, "rate_limit": "20/min"}
```

---

## Using with Clay

### Basic Setup

In Clay, use an **HTTP API** action (or "Run HTTP Request") with these settings:

| Setting | Value |
|---------|-------|
| **Method** | POST |
| **URL** | `https://your-domain.railway.app/process` |
| **Headers** | `Content-Type: application/json` |

### The Request Body

```json
{
  "target_api_url": "https://api.example.com/person/123",
  "callback_webhook_url": "https://hooks.clay.com/your-webhook-id",
  "person_id": "123",
  "company": "Acme Corp"
}
```

**Required fields:**
- `target_api_url` — The actual API you want to call

**Optional fields:**
- `callback_webhook_url` — Where to send results (your Clay webhook)
- `request_method` — GET, POST, PUT, DELETE (default: GET)
- `request_body` — Data to send for POST/PUT requests
- `request_headers` — Extra headers for this specific request

**Custom fields (pass-through):**
- Add any fields you want (like `person_id`, `urn`, `row_id`)
- They'll be included in the webhook response so you can match results to rows

### Example: LinkedIn Profile Lookup

**Request to proxy:**
```json
{
  "target_api_url": "https://linkedin-api.p.rapidapi.com/get-profile?url=https://linkedin.com/in/johndoe",
  "callback_webhook_url": "https://hooks.clay.com/abc123",
  "clay_row_id": "row_456",
  "person_name": "John Doe"
}
```

**What your webhook receives:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "target_api_url": "https://linkedin-api.p.rapidapi.com/get-profile?url=...",
  "clay_row_id": "row_456",
  "person_name": "John Doe",
  "api_status_code": 200,
  "api_response": {
    "firstName": "John",
    "lastName": "Doe",
    "headline": "CEO at Acme Corp",
    ...
  },
  "processed_at": 1699999999.123
}
```

### Clay Webhook Setup

1. In your Clay table, add a **"Webhook"** source (or enrichment that accepts webhooks)
2. Copy the webhook URL Clay gives you
3. Use that URL as your `callback_webhook_url`

---

## Common Rate Limit Settings

| API Limit | RATE_LIMIT_DELAY Value |
|-----------|------------------------|
| 10/minute | `6.0` |
| 20/minute | `3.0` |
| 30/minute | `2.0` |
| 60/minute | `1.0` |
| 100/minute | `0.6` |

---

## Checking Job Status

If you want to check on a job without waiting for the webhook:

```
GET https://your-domain.railway.app/status/{job_id}
```

Response:
```json
{
  "status": "completed",
  "updated_at": 1699999999.123,
  "result": { ... }
}
```

Possible statuses: `queued`, `completed`, `completed_webhook_failed`, `failed`, `timeout`

---

## Troubleshooting

### "Redis not connected"
- Check that your `REDIS_URL` variable is set correctly
- Make sure the Redis service is running in Railway

### Jobs stuck in queue
- Check the `/health` endpoint to see queue depth
- Look at Railway logs for errors

### Webhook not receiving data
- Verify your `callback_webhook_url` is correct
- Check that Clay's webhook is active
- Look at Railway logs — webhook failures are logged

### API returning errors
- Check your `GLOBAL_HEADERS` are correct (API keys, etc.)
- Test the target API directly first to make sure it works
- Look at the `api_status_code` in webhook responses

---

## Cost

**Railway pricing (as of 2025):**
- Free tier: $5/month credit (usually enough for small projects)

**Redis:** Included in Railway

---

## Quick Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/process` | POST | Queue a new API request |
| `/status/{job_id}` | GET | Check job status |
| `/health` | GET | Check if service is running |
| `/` | GET | API documentation |

---

## Need Help?

1. Check Railway logs (click on service → "Logs")
2. Test `/health` endpoint first
3. Verify environment variables are set correctly
