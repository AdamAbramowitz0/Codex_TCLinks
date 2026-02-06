"""Project-wide constants."""

STARTING_CHIPS = 100
DAILY_CHIPS = 10
MAX_PICKS_PER_CYCLE = 10
PHONE_OTP_TTL_MINUTES = 10
SESSION_TTL_DAYS = 14

CURATION_RANK_REWARDS = {
    1: 40,
    2: 20,
    3: 10,
}

# Reward chips for a correct pick at rank N.
RANK_REWARDS = {
    1: 20,
    2: 18,
    3: 16,
    4: 14,
    5: 12,
    6: 10,
    7: 8,
    8: 6,
    9: 4,
    10: 2,
}

# Weights used for market-implied probabilities.
RANK_WEIGHTS = {
    1: 10,
    2: 9,
    3: 8,
    4: 7,
    5: 6,
    6: 5,
    7: 4,
    8: 3,
    9: 2,
    10: 1,
}
