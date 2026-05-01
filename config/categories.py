import html
import re

CATEGORIES = [
    "Groceries", "Food & Drink", "Shopping", "Gas", "Health",
    "Travel", "Entertainment", "Subscriptions", "Bills & Utilities",
    "Personal Care", "Home", "Transfer", "Uncategorized",
]

CHASE_CATEGORY_MAP = {
    "Food & Drink": "Food & Drink",
    "Groceries": "Groceries",
    "Gas": "Gas",
    "Travel": "Travel",
    "Entertainment": "Entertainment",
    "Health & Wellness": "Health",
    "Shopping": "Shopping",
    "Bills & Utilities": "Bills & Utilities",
    "Professional Services": "Bills & Utilities",
    "Fees & Adjustments": "Bills & Utilities",
    "Personal": "Personal Care",
}

FALLBACK_KEYWORD_MAP = [
    ("TRADER JOE", "Groceries"), ("WHOLE FOODS", "Groceries"),
    ("SAFEWAY", "Groceries"), ("BUTCHERBOX", "Groceries"),
    ("UBER EATS", "Food & Drink"), ("DOORDASH", "Food & Drink"),
    ("GRUBHUB", "Food & Drink"), ("STARBUCKS", "Food & Drink"),
    ("UBER TRIP", "Travel"), ("HOLIDAY INN", "Travel"),
    ("CHEVRON", "Gas"), ("SHELL", "Gas"), ("ARCO", "Gas"),
    ("ZOOMCARE", "Health"), ("CVS", "Health"),
    ("WALGREENS", "Health"), ("PHARMACY", "Health"),
    ("GYM", "Health"), ("PLANET FITNESS", "Health"),
    ("NETFLIX", "Subscriptions"), ("SPOTIFY", "Subscriptions"),
    ("HULU", "Subscriptions"), ("AMAZON PRIME", "Subscriptions"),
    ("APPLE.COM", "Subscriptions"), ("GOOGLE ONE", "Subscriptions"),
    ("AMAZON", "Shopping"),
    ("VENMO", "Transfer"), ("ZELLE", "Transfer"), ("PAYPAL", "Transfer"),
    ("GEICO", "Bills & Utilities"), ("PLAN FEE", "Bills & Utilities"),
]

SEED_MERCHANT_RULES = [
    ("TRADER JOE", "Groceries"), ("WHOLE FOODS", "Groceries"),
    ("AMAZON", "Shopping"), ("NETFLIX", "Subscriptions"),
    ("SPOTIFY", "Subscriptions"), ("APPLE.COM", "Subscriptions"),
    ("UBER EATS", "Food & Drink"), ("DOORDASH", "Food & Drink"),
    ("STARBUCKS", "Food & Drink"), ("CHEVRON", "Gas"),
    ("SHELL", "Gas"), ("ARCO", "Gas"), ("WALGREENS", "Health"),
    ("CVS", "Health"), ("PLANET FITNESS", "Health"),
    ("VENMO", "Transfer"), ("ZELLE", "Transfer"), ("PAYPAL", "Transfer"),
]


def _normalize_text(value: str) -> str:
    value = str(value or "").upper()
    value = re.sub(r"[\x27‘’`]", "", value)  # remove apostrophes before spacing
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()
