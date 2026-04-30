"""Generate anonymized sample Chase CSV exports for Q1 and Q2 2024."""
import csv
import random
from pathlib import Path

random.seed(42)

MERCHANTS = [
    # (description, chase_category, amount_range)
    ("TRADER JOE S #123", "Groceries", (60, 120)),
    ("WHOLE FOODS MARKET #456", "Groceries", (70, 130)),
    ("SAFEWAY #789", "Groceries", (50, 110)),
    ("BUTCHERBOX", "Groceries", (100, 160)),
    ("UBER EATS", "Food & Drink", (15, 45)),
    ("DOORDASH DASHPASS", "Food & Drink", (12, 40)),
    ("GRUBHUB", "Food & Drink", (10, 35)),
    ("STARBUCKS #3421", "Food & Drink", (5, 15)),
    ("STARBUCKS STORE 03421", "Food & Drink", (5, 15)),
    ("NETFLIX.COM", "Entertainment", (15, 23)),
    ("SPOTIFY USA", "Entertainment", (10, 11)),
    ("HULU", "Entertainment", (8, 18)),
    ("APPLE.COM/BILL", "Shopping", (3, 15)),
    ("GOOGLE ONE", "Shopping", (3, 10)),
    ("AMAZON.COM", "Shopping", (15, 150)),
    ("AMAZON MKTPLACE", "Shopping", (10, 80)),
    ("CHEVRON #98765", "Gas", (45, 85)),
    ("SHELL OIL #12345", "Gas", (40, 80)),
    ("ARCO #54321", "Gas", (35, 75)),
    ("WALGREENS #2341", "Health & Wellness", (10, 40)),
    ("CVS PHARMACY #456", "Health & Wellness", (8, 35)),
    ("PLANET FITNESS", "Health & Wellness", (10, 25)),
    ("ZOOMCARE PORTLAND", "Health & Wellness", (25, 80)),
    ("VENMO PAYMENT", "Transfer", (20, 200)),
    ("ZELLE TRANSFER", "Transfer", (50, 300)),
    ("PAYPAL", "Transfer", (10, 100)),
    ("GEICO INS PMT", "Bills & Utilities", (80, 150)),
    ("COMCAST CABLE", "Bills & Utilities", (60, 100)),
    ("UBER TRIP", "Travel", (8, 35)),
    ("HOLIDAY INN EXPRESS", "Travel", (80, 180)),
    ("TARGET #1234", "Shopping", (20, 120)),
    ("COSTCO WHSE #789", "Groceries", (80, 200)),
]

HEADER = ["Transaction Date", "Post Date", "Description", "Category", "Type", "Amount", "Memo"]


def random_amount(lo: float, hi: float) -> float:
    # Chase: charges are negative
    return -round(random.uniform(lo, hi), 2)


def gen_date(year: int, month: int, day: int) -> str:
    return f"{month:02d}/{day:02d}/{year}"


def generate_quarter(year: int, months: list, n_rows: int, output_path: Path):
    rows = []
    for _ in range(n_rows - 1):  # reserve last slot for day-29+ row
        month = random.choice(months)
        day = random.randint(1, 28)
        merchant, category, amt_range = random.choice(MERCHANTS)
        trans_date = gen_date(year, month, day)
        post_date = gen_date(year, month, min(day + random.randint(1, 3), 28))
        rows.append([trans_date, post_date, merchant, category, "Sale",
                     random_amount(*amt_range), ""])

    # One transaction on day >= 29 to exercise statement_month rollover
    rollover_month = months[0]
    merchant, category, amt_range = random.choice(MERCHANTS)
    rows.append([
        gen_date(year, rollover_month, 29),
        gen_date(year, rollover_month, 30),
        merchant, category, "Sale", random_amount(*amt_range), "",
    ])

    # Add one payment/credit row (positive) — should be filtered at bronze
    rows.append([
        gen_date(year, months[-1], 15),
        gen_date(year, months[-1], 17),
        "AUTOPAY PAYMENT THANK YOU", "Payment", "Payment", 1200.00, "",
    ])

    random.shuffle(rows)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows → {output_path}")


if __name__ == "__main__":
    out = Path(__file__).parent.parent / "data" / "sample"
    out.mkdir(parents=True, exist_ok=True)
    generate_quarter(2024, [1, 2, 3], 46, out / "chase_sample_2024_q1.csv")
    generate_quarter(2024, [4, 5, 6], 46, out / "chase_sample_2024_q2.csv")
