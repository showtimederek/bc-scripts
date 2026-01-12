import csv
import json
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

try:
    from tqdm import tqdm  # optional: pip install tqdm
except ImportError:
    tqdm = None

LIMIT = 250
OUTPUT_CSV = "bigcommerce_orders_v2.csv"

# Concurrency + rate limiting controls
MAX_WORKERS = 10
MAX_RETRIES = 8
BASE_BACKOFF = 1.0
TIMEOUT = 60


def get_headers(access_token):
    return {
        "X-Auth-Token": access_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def normalize_row(obj):
    # Flatten nested dict/list values into JSON strings for CSV compatibility
    out = {}
    for k, v in obj.items():
        out[k] = json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
    return out


def request_with_backoff(url, headers, params=None):
    """
    Handles:
      - 429 rate limiting (Retry-After if present)
      - transient 5xx errors
      - network timeouts/connection errors
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)

            # Rate limited
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = BASE_BACKOFF * (2 ** (attempt - 1))
                else:
                    sleep_s = BASE_BACKOFF * (2 ** (attempt - 1))

                sleep_s += random.uniform(0, 0.25 * sleep_s)
                time.sleep(min(sleep_s, 60))
                continue

            # transient server errors
            if resp.status_code in (500, 502, 503, 504):
                sleep_s = BASE_BACKOFF * (2 ** (attempt - 1))
                sleep_s += random.uniform(0, 0.25 * sleep_s)
                time.sleep(min(sleep_s, 60))
                continue

            resp.raise_for_status()
            return resp

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            sleep_s = BASE_BACKOFF * (2 ** (attempt - 1))
            sleep_s += random.uniform(0, 0.25 * sleep_s)
            time.sleep(min(sleep_s, 60))

    if last_exc:
        raise last_exc
    raise RuntimeError("Failed request after {} attempts: {}".format(MAX_RETRIES, url))


def fetch_all_orders(store_hash, access_token):
    """
    V2 orders pagination:
      GET /v2/orders?limit=250&page=N

    V2 typically does NOT include meta.pagination in the body.
    Stop when the returned page has < LIMIT rows (or is empty).
    """
    url = "https://api.bigcommerce.com/stores/{}/v2/orders".format(store_hash)
    headers = get_headers(access_token)

    orders = []
    page = 1

    total_hint = None
    pbar = None

    while True:
        resp = request_with_backoff(url, headers=headers, params={"limit": LIMIT, "page": page})

        if total_hint is None:
            x_total = resp.headers.get("X-Total-Count") or resp.headers.get("x-total-count")
            if x_total and str(x_total).isdigit():
                total_hint = int(x_total)

        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError("Unexpected /v2/orders response shape on page {}: {}".format(page, type(data)))

        if not data:
            break

        if pbar is None and tqdm is not None:
            pbar = tqdm(total=total_hint, desc="Exporting orders", unit="order") if total_hint else tqdm(
                desc="Exporting orders", unit="order"
            )

        for o in data:
            orders.append(o)
            if pbar is not None:
                pbar.update(1)

        if len(data) < LIMIT:
            break

        page += 1

    if pbar is not None:
        pbar.close()

    return orders


def fetch_order_products(store_hash, access_token, order_id):
    """
    GET /v2/orders/{order_id}/products
    """
    url = "https://api.bigcommerce.com/stores/{}/v2/orders/{}/products".format(store_hash, order_id)
    headers = get_headers(access_token)

    resp = request_with_backoff(url, headers=headers)
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected products response for order {}: {}".format(order_id, type(data)))
    return data


def build_rows_one_per_order(store_hash, access_token, orders):
    """
    One CSV row per order.
    Adds products_json: JSON array string of the order's products.
    """
    rows = []

    pbar = tqdm(total=len(orders), desc="Fetching order products", unit="order") if tqdm else None

    def worker(order):
        oid = int(order.get("id"))
        products = fetch_order_products(store_hash, access_token, oid)

        order_norm = normalize_row(order)
        order_norm["products_json"] = json.dumps(products, separators=(",", ":"))
        return order_norm

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, o) for o in orders]
        for fut in as_completed(futures):
            row = fut.result()
            rows.append(row)
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    # Optional: stable ordering by order id
    try:
        rows.sort(key=lambda r: int(r.get("id") or 0))
    except Exception:
        pass

    return rows


def write_csv(rows, output_file):
    if not rows:
        print("No rows found; nothing to write.")
        return

    for r in rows:
        if "products_json" not in r:
            r["products_json"] = "[]"

    # Preserve key order (first-seen order across rows)
    fieldnames = []
    seen = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("Wrote {} orders to {}".format(len(rows), output_file))


def main():
    store_hash = input("Enter your BigCommerce Store Hash: ").strip()
    access_token = input("Enter your BigCommerce Access Token: ").strip()

    if not store_hash or not access_token:
        raise SystemExit("Store hash and access token are required.")

    print("Fetching all orders (v2)...")
    orders = fetch_all_orders(store_hash, access_token)
    print("Fetched {} orders.".format(len(orders)))

    print("Fetching products for each order (v2)...")
    rows = build_rows_one_per_order(store_hash, access_token, orders)

    write_csv(rows, OUTPUT_CSV)


if __name__ == "__main__":
    main()