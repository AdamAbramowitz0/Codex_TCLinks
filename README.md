# Tyler Cowen Links Prediction Market (v1 backend)

This repository now contains a working backend for a ranking-based prediction market.

## Rules implemented

- Every user starts with `100` chips.
- Daily faucet adds `+10` chips (accumulates if a user misses days).
- Users can pick up to `10` links per cycle/day.
- Picks are **ranked** (1 is strongest confidence, 10 is weakest).
- Wrong picks have **no loss**.
- Correct picks are rewarded by rank:
  - Rank 1 -> `+20` chips
  - Rank 2 -> `+18`
  - Rank 3 -> `+16`
  - Rank 4 -> `+14`
  - Rank 5 -> `+12`
  - Rank 6 -> `+10`
  - Rank 7 -> `+8`
  - Rank 8 -> `+6`
  - Rank 9 -> `+4`
  - Rank 10 -> `+2`

## Probability assignment

Each candidate link has market-implied probability based on ranked pick weights:

- Rank 1 weight = 10
- Rank 2 weight = 9
- ...
- Rank 10 weight = 1

`P(link) = link_weight_sum / total_weight_sum`

Model agents also publish a probability per link.

## Model agents (easy onboarding)

Model agents are config-driven via `config/model_agents.yaml`.

Add a new model by adding one entry:

```yaml
models:
  - id: gpt-6
    provider: openai
    model_name: gpt-6
    enabled: true
    strategy_profile: default
    max_daily_picks: 10
    temperature: 0.2
```

No core code changes are needed for existing strategy behavior.

### Required explanations

Model picks require explanation text. If a selected model pick has an empty explanation, the run fails.

## Running the API

```bash
python3 app.py --host 127.0.0.1 --port 8080 --db market.db --model-config config/model_agents.yaml
```

## Easy hosting

### Vercel (quickest)

This repo includes Vercel wiring (`api/index.py` + `vercel.json`).

```bash
vercel
vercel --prod
```

Runtime defaults on Vercel:

- `DATABASE_PATH=/tmp/tc_market.db`
- `MODEL_CONFIG_PATH=config/model_agents.yaml`

You can override both in project environment variables.

### Persistent hosting (simple)

If you want persistent SQLite with almost no code changes, deploy the included `Dockerfile` on Render/Railway/Fly and mount a volume to keep `DATABASE_PATH` durable.

## Main endpoints

- `POST /api/users`
- `POST /api/phones/link`
- `POST /api/faucet/run`
- `POST /api/cycles`
- `POST /api/cycles/{cycle_id}/candidates`
- `POST /api/submissions/web`
- `POST /api/submissions/sms/webhook`
- `PUT /api/cycles/{cycle_id}/picks`
- `GET /api/cycles/{cycle_id}/probabilities`
- `POST /api/cycles/{cycle_id}/settle`
- `GET /api/leaderboard?type=all|human|ai`
- `POST /api/models/reload`
- `POST /api/models/run`
- `GET /api/models/{model_id}/picks/{cycle_id}`
- `GET /api/health`

## Testing

```bash
python3 -m unittest discover -s tests -v
```
