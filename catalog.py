import csv
import json
import requests
from tqdm import tqdm  # pip install tqdm

LIMIT = 250
OUTPUT_CSV = "bigcommerce_catalog_products.csv"


def get_headers(access_token: str) -> dict:
    return {
        "X-Auth-Token": access_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def normalize_row(obj: dict) -> dict:
    # Flatten nested dict/list values into JSON strings for CSV compatibility
    out = {}
    for k, v in obj.items():
        out[k] = json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
    return out


def fetch_all_products(store_hash: str, access_token: str) -> list[dict]:
    url = f"https://api.bigcommerce.com/stores/{store_hash}/v3/catalog/products"
    headers = get_headers(access_token)

    products: list[dict] = []
    page = 1
    pbar = None

    while True:
        resp = requests.get(url, headers=headers, params={"limit": LIMIT, "page": page}, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        data = payload.get("data") or []
        pagination = (payload.get("meta") or {}).get("pagination") or {}

        if not data:
            break

        if pbar is None:
            total = pagination.get("total")
            pbar = tqdm(total=total, desc="Exporting products", unit="product") if total else tqdm(
                desc="Exporting products", unit="product"
            )

        for product in data:
            products.append(normalize_row(product))
            pbar.update(1)

        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_pages", page)
        if current_page >= total_pages:
            break

        page += 1

    if pbar is not None:
        pbar.close()

    return products


def write_csv(rows: list[dict], output_file: str) -> None:
    if not rows:
        print("No products found; nothing to write.")
        return

    # Preserve BigCommerce JSON key order (first-seen order across rows)
    fieldnames: list[str] = []
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

    print(f"Wrote {len(rows)} products to {output_file}")


def main() -> None:
    store_hash = input("Enter your BigCommerce Store Hash: ").strip()
    access_token = input("Enter your BigCommerce Access Token: ").strip()

    if not store_hash or not access_token:
        raise SystemExit("Store hash and access token are required.")

    rows = fetch_all_products(store_hash, access_token)
    write_csv(rows, OUTPUT_CSV)


if __name__ == "__main__":
    main()
