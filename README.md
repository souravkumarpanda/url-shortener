# URL Shortener

Client -> nginx load balancer -> 3 stateless Express instances -> Redis (cache + rate limit),
Kafka (click events) -> worker -> MongoDB.

## Run everything (Docker)

    docker compose up -d --build
    docker compose ps                     # wait until api1-3, mongo, redis are "healthy" (~30-60 s first time)

Open http://localhost:8080 and shorten a link.

## Get a public https link (no account needed)

    TRUST_PROXY=2 docker compose --profile public up -d --build
    docker compose logs tunnel | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | head -1

(PowerShell: `$env:TRUST_PROXY=2; docker compose --profile public up -d --build`)
TRUST_PROXY=2 is needed because the tunnel adds one more proxy hop; without it everyone
shares one rate-limit bucket. The URL changes each time the tunnel container restarts.

## Check it works

    curl -i http://localhost:8080/healthz                 # run a few times: X-Served-By changes api1/api2/api3
    curl -s -X POST http://localhost:8080/url -H 'content-type: application/json' \
         -d '{"originalUrl":"https://github.com"}'        # -> {"shortUrl":"abc","shortLink":"http://localhost:8080/abc"}
    curl -I http://localhost:8080/<code>                  # 302 + X-Cache: MISS, then HIT
    curl http://localhost:8080/url/analytics/<code>       # clicks arrive via Kafka -> worker, ~1 s delay
    docker compose logs -f worker                         # see click batches being consumed

## Operate

    docker compose logs -f api1 api2 api3
    docker compose stop api2                              # site keeps working on api1/api3
    docker compose start api2
    docker compose down                                   # stop (data kept)
    docker compose down -v                                # stop and delete MongoDB data

## Development without Docker for the app

    docker compose up -d mongo redis kafka
    npm install
    npm start                                             # terminal 1 (API on :8001)
    npm run worker                                        # terminal 2

## Always-on hosting (VPS)

    curl -fsSL https://get.docker.com | sh
    # copy this folder to the server, then:
    docker compose up -d --build
    # open TCP 8080 in the server firewall -> http://<server-ip>:8080
    # for port 80 change "8080:80" to "80:80" in docker-compose.yml
    # for a domain + https, put Caddy or Cloudflare in front and set BASE_URL=https://your.domain

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| PORT | 8001 | API port inside the container |
| MONGO_URL | mongodb://localhost:27017/short-url | MongoDB |
| REDIS_URL | redis://localhost:6379 | Redis |
| KAFKA_BROKERS | localhost:9092 | Kafka (comma separated) |
| TRUST_PROXY | 0 (compose: 1) | Proxy hops in front of the app (1 = nginx, 2 = tunnel + nginx) |
| BASE_URL | derived from request | Pin the public address used in generated links |
| CACHE_TTL_SECONDS | 86400 | Redirect cache lifetime |
