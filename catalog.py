import os
import csv
import json
import requests
from tqdm import tqdm  # pip install tqdm

# =============================
# CONFIGURATION
# =============================

# OPTION 1: Hard-code your credentials here:
STORE_HASH = "6lmqdiy"          # e.g. "abc123"
ACCESS_TOKEN = "f0k3kmct0muqvu7dzn689byjwoi72si"      # from Advanced Settings -> API Accounts -> Store API

# OPTION 2 (recommended): use environment variables instead of editing the code:
#   export BC_STORE_HASH="abc123"
#   export BC_ACCESS_TOKEN="your_token_here"
# and then uncomment these lines:
# STORE_HASH = os.getenv("BC_STORE_HASH", STORE_HASH)
# ACCESS_TOKEN = os.getenv("BC_ACCESS_TOKEN", ACCESS_TOKEN)

# Output CSV file
OUTPUT_CSV = "bigcommerce_catalog_products.csv"

# API base
BASE_URL = f"https://api.bigcommerce.com/stores/{STORE_HASH}/v3/catalog/products"

# Default query params
LIMIT = 250  # max allowed by V3 catalog APIs for many resources


def get_headers():
    """
    Headers for BigCommerce V3 Management API.
    Using X-Auth-Token for server-to-server auth.
    """
    return {
        "X-Auth-Token": ACCESS_TOKEN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def normalize_product(product):
    """
    Convert nested fields (dicts/lists) into JSON strings
    so they can be written to a flat CSV row.

    Keeps key order the same as the incoming JSON dict, so the
    CSV column order can follow the BigCommerce JSON structure.
    """
    normalized = {}
    for key, value in product.items():
        if isinstance(value, (dict, list)):
            normalized[key] = json.dumps(value, separators=(",", ":"))
        else:
            normalized[key] = value
    return normalized


def fetch_all_products():
    """
    Page through /v3/catalog/products until all pages have been fetched.
    Uses meta.pagination to iterate, and shows a progress bar based on total products.
    """
    all_products = []
    page = 1

    total_products = None
    pbar = None

    while True:
        params = {
            "limit": LIMIT,
            "page": page,
        }

        response = requests.get(BASE_URL, headers=get_headers(), params=params)
        response.raise_for_status()
        payload = response.json()

        data = payload.get("data", [])  # list of products
        meta = payload.get("meta", {})
        pagination = meta.get("pagination", {})

        # If there is no data, we're done
        if not data:
            break

        # Initialize progress bar once we know total products
        if total_products is None:
            total_products = pagination.get("total")
            if total_products:
                pbar = tqdm(
                    total=total_products,
                    desc="Exporting products",
                    unit="product",
                )

        # Add products and update progress bar
        for product in data:
            all_products.append(normalize_product(product))
            if pbar is not None:
                pbar.update(1)

        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_pages", page)

        # Optional: log page progress
        print(
            f"Fetched page {current_page} of {total_pages} "
            f"({len(data)} products on this page)"
        )

        # Stop if we've reached the last page
        if current_page >= total_pages:
            break

        page += 1

    if pbar is not None:
        pbar.close()

    return all_products


def write_products_to_csv(products, output_file):
    """
    Write the collected products list (list of dicts) to a CSV.

    Column order follows the key order from the BigCommerce JSON:
    - We walk the products in order.
    - First time we see a key, we append it to fieldnames.
    - This preserves the order from the first product's JSON,
      with any "new" keys from later products appended at the end
      in discovery order.
    """
    if not products:
        print("No products found; nothing to write.")
        return

    fieldnames = []
    for p in products:
        for key in p.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for product in products:
            writer.writerow(product)

    print(f"Wrote {len(products)} products to {output_file}")


def main():
    # Simple sanity check so you don't forget to set credentials
    if (
        not STORE_HASH
        or not ACCESS_TOKEN
        or "YOUR_STORE_HASH_HERE" in STORE_HASH
        or "YOUR_ACCESS_TOKEN_HERE" in ACCESS_TOKEN
    ):
        raise RuntimeError(
            "Please set STORE_HASH and ACCESS_TOKEN at the top of the script, "
            "or via BC_STORE_HASH / BC_ACCESS_TOKEN environment variables."
        )

    print("Fetching products from BigCommerce V3 Catalog API...")
    products = fetch_all_products()
    print("Exporting to CSV...")
    write_products_to_csv(products, OUTPUT_CSV)
    print("Done.")


if __name__ == "__main__":
    main()
