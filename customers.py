import csv
import json
import re
import time
from typing import Any, Dict, List, Optional

import requests
from tqdm import tqdm  # pip install tqdm

# -----------------------------
# Config
# -----------------------------
LIMIT = 250
OUTPUT_CSV = "bigcommerce_customers.csv"

# Rate limit handling
MAX_429_RETRIES = 20
DEFAULT_RETRY_AFTER = 5  # seconds if Retry-After header missing

# Split columns caps
MAX_ADDRESSES = 50
MAX_ATTRIBUTES = 50

# These should appear at the BACK of the CSV
BACK_COLUMNS_ORDER = [
    "address_count",
    "addresses",          # JSON string of full list (kept for completeness)
    "attribute_count",
    "attributes",         # JSON string of full list (kept for completeness)
    "attribute_values",   # JSON string of full list (optional, only when fetched)
]

# Chunk sizes for customer_id:in filters (keeps URLs reasonable)
ID_IN_CHUNK = 50


# -----------------------------
# Helpers
# -----------------------------
def get_headers(access_token: str) -> dict:
    return {
        "X-Auth-Token": access_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def json_cell(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def chunked_ints(values: List[int], n: int) -> List[List[int]]:
    return [values[i : i + n] for i in range(0, len(values), n)]


def group_by_customer_id(items: List[Dict[str, Any]], key: str = "customer_id") -> Dict[int, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for item in items:
        cid = item.get(key)
        if cid is None:
            continue
        grouped.setdefault(int(cid), []).append(item)
    return grouped


# -----------------------------
# HTTP (429-safe)
# -----------------------------
def get_with_429_retry(url: str, headers: dict, params: dict, timeout: int = 60) -> requests.Response:
    retries = 0
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)

        if resp.status_code != 429:
            return resp

        retries += 1
        if retries > MAX_429_RETRIES:
            raise requests.HTTPError(
                f"Exceeded {MAX_429_RETRIES} retries due to 429 rate limits. "
                f"Last response: {resp.status_code} {resp.text}",
                response=resp,
            )

        retry_after = resp.headers.get("Retry-After")
        if retry_after and retry_after.strip().isdigit():
            sleep_s = int(retry_after.strip())
        else:
            sleep_s = min(DEFAULT_RETRY_AFTER + (retries - 1), 30)

        time.sleep(sleep_s)


def fetch_paginated(base_url: str, headers: dict, params: dict, desc: str, unit: str) -> List[Dict[str, Any]]:
    """
    Fetch all pages for endpoints that return meta.pagination and accept page/limit.
    """
    page = 1
    out: List[Dict[str, Any]] = []
    pbar = None

    while True:
        page_params = dict(params)
        page_params["limit"] = min(int(page_params.get("limit", LIMIT)), LIMIT)
        page_params["page"] = page

        resp = get_with_429_retry(base_url, headers=headers, params=page_params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        data = payload.get("data") or []
        pagination = (payload.get("meta") or {}).get("pagination") or {}

        if not data:
            break

        if pbar is None:
            total = pagination.get("total")
            pbar = tqdm(total=total, desc=desc, unit=unit) if total else tqdm(desc=desc, unit=unit)

        out.extend(data)
        pbar.update(len(data))

        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_pages", page)
        if current_page >= total_pages:
            break

        page += 1

    if pbar is not None:
        pbar.close()

    return out


# -----------------------------
# BigCommerce fetchers
# -----------------------------
def fetch_attribute_definitions(store_hash: str, headers: dict) -> Dict[int, str]:
    """
    Fetch attribute definitions once and build attribute_id -> attribute_name map.
    Endpoint: /v3/customers/attributes
    """
    url = f"https://api.bigcommerce.com/stores/{store_hash}/v3/customers/attributes"
    rows = fetch_paginated(url, headers, params={"limit": LIMIT}, desc="Fetching attribute definitions", unit="attrdef")

    mapping: Dict[int, str] = {}
    for a in rows:
        aid = a.get("id")
        name = a.get("name")
        if aid is not None and name is not None:
            mapping[int(aid)] = str(name)
    return mapping


def fetch_addresses_for_customers(store_hash: str, headers: dict, customer_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Bulk fetch addresses using customer_id:in filter.
    Endpoint: /v3/customers/addresses?customer_id:in=1,2,3
    """
    if not customer_ids:
        return {}

    url = f"https://api.bigcommerce.com/stores/{store_hash}/v3/customers/addresses"
    all_rows: List[Dict[str, Any]] = []

    for id_chunk in chunked_ints(customer_ids, ID_IN_CHUNK):
        params = {"customer_id:in": ",".join(str(x) for x in id_chunk), "limit": LIMIT}
        rows = fetch_paginated(url, headers, params, desc="Fetching addresses", unit="address")
        all_rows.extend(rows)

    return group_by_customer_id(all_rows, key="customer_id")


def fetch_attribute_values_for_customers(store_hash: str, headers: dict, customer_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Bulk fetch attribute values using customer_id:in filter.
    Endpoint: /v3/customers/attribute-values
    """
    if not customer_ids:
        return {}

    url = f"https://api.bigcommerce.com/stores/{store_hash}/v3/customers/attribute-values"
    all_rows: List[Dict[str, Any]] = []

    for id_chunk in chunked_ints(customer_ids, ID_IN_CHUNK):
        params = {"customer_id:in": ",".join(str(x) for x in id_chunk), "limit": LIMIT}
        try:
            rows = fetch_paginated(url, headers, params, desc="Fetching attribute values", unit="attrval")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (403, 404):
                print(
                    "Warning: Could not fetch /v3/customers/attribute-values (403/404). "
                    "Will rely on attributes included on the customer record (if present)."
                )
                return {}
            raise
        all_rows.extend(rows)

    return group_by_customer_id(all_rows, key="customer_id")


# -----------------------------
# Normalization / splitting
# -----------------------------
def split_list_of_dicts(prefix: str, items: List[Dict[str, Any]], max_items: int) -> Dict[str, Any]:
    """
    For items[0], items[1], ... create columns like:
      prefix1_field, prefix2_field, ...
    Nested dict/list values become JSON strings.
    """
    out: Dict[str, Any] = {}
    for i in range(min(len(items), max_items)):
        item = items[i]
        if not isinstance(item, dict):
            continue
        for k, v in item.items():
            col = f"{prefix}{i+1}_{k}"
            out[col] = json_cell(v) if isinstance(v, (dict, list)) else v
    return out


def canonicalize_attribute_items(items: List[Dict[str, Any]], attr_id_to_name: Dict[int, str]) -> List[Dict[str, Any]]:
    """
    Convert attribute-like objects to a consistent shape:
      {attribute_id, attribute_name, value, raw}

    Handles both:
      - attribute-values objects (attribute_id, customer_id, attribute_value, ...)
      - included attributes if they have id/name/value in a different shape
    """
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue

        aid = it.get("attribute_id")
        if aid is None:
            aid = it.get("id")

        name = it.get("name")
        if name is None and aid is not None:
            name = attr_id_to_name.get(int(aid))

        # IMPORTANT: attribute-values uses "attribute_value"
        val = it.get("attribute_value")
        if val is None:
            val = it.get("value")

        out.append(
            {
                "attribute_id": aid,
                "attribute_name": name,
                "value": val,
                "raw": it,
            }
        )
    return out


def split_attributes(items: List[Dict[str, Any]], attr_id_to_name: Dict[int, str], max_items: int) -> Dict[str, Any]:
    """
    Create columns:
      attribute{n}_id
      attribute{n}_name
      attribute{n}_value
      attribute{n}_raw
    """
    canon = canonicalize_attribute_items(items, attr_id_to_name)

    out: Dict[str, Any] = {}
    for i in range(min(len(canon), max_items)):
        a = canon[i]
        out[f"attribute{i+1}_id"] = a.get("attribute_id")
        out[f"attribute{i+1}_name"] = a.get("attribute_name")
        out[f"attribute{i+1}_value"] = a.get("value")
        out[f"attribute{i+1}_raw"] = json_cell(a.get("raw"))
    return out


def normalize_customer_row(customer: Dict[str, Any], attr_id_to_name: Dict[int, str]) -> Dict[str, Any]:
    """
    - Keep customer top-level fields (excluding list subresources)
    - Split addresses up to MAX_ADDRESSES: address{n}_*
    - Split attributes up to MAX_ATTRIBUTES: attribute{n}_id/name/value/raw
      (value comes from attribute_value OR value)
    - Keep full lists as JSON (addresses/attributes/attribute_values) for completeness
      (these get moved to the end by column ordering)
    """
    out: Dict[str, Any] = {}

    addresses = customer.get("addresses") or []
    attributes = customer.get("attributes") or []
    attribute_values = customer.get("attribute_values") or []

    deferred = {"addresses", "attributes", "attribute_values"}
    for k, v in customer.items():
        if k in deferred:
            continue
        out[k] = json_cell(v) if isinstance(v, (dict, list)) else v

    if isinstance(addresses, list) and addresses:
        out.update(split_list_of_dicts("address", addresses, MAX_ADDRESSES))

    # Prefer included `attributes` if present; otherwise use fetched `attribute_values`
    attr_source = attributes if isinstance(attributes, list) and attributes else attribute_values
    if isinstance(attr_source, list) and attr_source:
        out.update(split_attributes(attr_source, attr_id_to_name, MAX_ATTRIBUTES))

    # Keep complete raw lists at end
    if "addresses" in customer:
        out["addresses"] = json_cell(addresses)
    if "attributes" in customer:
        out["attributes"] = json_cell(attributes)
    if "attribute_values" in customer:
        out["attribute_values"] = json_cell(attribute_values)

    return out


# -----------------------------
# CSV column ordering
# -----------------------------
def sort_split_keys(keys: List[str], kind_prefix: str) -> List[str]:
    """
    Sort keys like address12_city, address2_city => numeric order then field name
    """
    pat = re.compile(rf"^{re.escape(kind_prefix)}(\d+)_(.+)$")

    def key_fn(k: str):
        m = pat.match(k)
        if not m:
            return (10**9, k)
        return (int(m.group(1)), m.group(2))

    return sorted(keys, key=key_fn)


def build_fieldnames(rows: List[Dict[str, Any]]) -> List[str]:
    """
    Order:
      (A) normal customer columns (first-seen order), excluding back columns & split columns
      (B) address split columns sorted by index then field
      (C) attribute split columns sorted by index then field
      (D) back columns in BACK_COLUMNS_ORDER
      (E) leftovers
    """
    seen = set()
    all_keys_in_order: List[str] = []
    for row in rows:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                all_keys_in_order.append(k)

    back_set = set(BACK_COLUMNS_ORDER)
    addr_split = [k for k in all_keys_in_order if re.match(r"^address\d+_", k)]
    attr_split = [k for k in all_keys_in_order if re.match(r"^attribute\d+_", k)]

    addr_split_sorted = sort_split_keys(addr_split, "address")
    attr_split_sorted = sort_split_keys(attr_split, "attribute")

    split_set = set(addr_split) | set(attr_split)

    normal_cols = [k for k in all_keys_in_order if k not in back_set and k not in split_set]
    back_cols = [k for k in BACK_COLUMNS_ORDER if k in all_keys_in_order]

    final = normal_cols + addr_split_sorted + attr_split_sorted + back_cols
    final_set = set(final)
    leftovers = [k for k in all_keys_in_order if k not in final_set]
    return final + leftovers


# -----------------------------
# Main export
# -----------------------------
def fetch_all_customers_with_subresources(store_hash: str, access_token: str) -> List[Dict[str, Any]]:
    """
    Efficient approach:
      - Fetch attribute definitions once (id->name)
      - Fetch customers page-by-page with include=addresses,attributes
      - If counts suggest truncation, bulk-fetch missing addresses/attribute-values for that page
      - Normalize into CSV-friendly dicts, including split columns
    """
    customers_url = f"https://api.bigcommerce.com/stores/{store_hash}/v3/customers"
    headers = get_headers(access_token)

    attr_id_to_name = fetch_attribute_definitions(store_hash, headers)

    rows: List[Dict[str, Any]] = []
    page = 1
    pbar = None

    while True:
        params = {"limit": LIMIT, "page": page, "include": "addresses,attributes"}
        resp = get_with_429_retry(customers_url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        customers = payload.get("data") or []
        pagination = (payload.get("meta") or {}).get("pagination") or {}

        if not customers:
            break

        if pbar is None:
            total = pagination.get("total")
            pbar = tqdm(total=total, desc="Exporting customers", unit="customer") if total else tqdm(
                desc="Exporting customers", unit="customer"
            )

        # Determine which customers might have truncated includes
        need_addr: List[int] = []
        need_attr: List[int] = []

        for c in customers:
            cid = c.get("id")
            if cid is None:
                continue
            cid = int(cid)

            addr_count = c.get("address_count")
            attr_count = c.get("attribute_count")

            included_addrs = c.get("addresses") or []
            included_attrs = c.get("attributes") or []

            if isinstance(addr_count, int) and isinstance(included_addrs, list) and addr_count > len(included_addrs):
                need_addr.append(cid)
            if isinstance(attr_count, int) and isinstance(included_attrs, list) and attr_count > len(included_attrs):
                need_attr.append(cid)

        addr_map = fetch_addresses_for_customers(store_hash, headers, need_addr) if need_addr else {}
        attr_map = fetch_attribute_values_for_customers(store_hash, headers, need_attr) if need_attr else {}

        for c in customers:
            cid = c.get("id")
            if cid is None:
                continue
            cid = int(cid)

            if cid in addr_map:
                c["addresses"] = addr_map[cid]
            if cid in attr_map:
                c["attribute_values"] = attr_map[cid]

            rows.append(normalize_customer_row(c, attr_id_to_name))
            pbar.update(1)

        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_pages", page)
        if current_page >= total_pages:
            break

        page += 1

    if pbar is not None:
        pbar.close()

    return rows


def write_csv(rows: List[Dict[str, Any]], output_file: str) -> None:
    if not rows:
        print("No customers found; nothing to write.")
        return

    fieldnames = build_fieldnames(rows)

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"Wrote {len(rows)} customers to {output_file}")


def main() -> None:
    store_hash = input("Enter your BigCommerce Store Hash: ").strip()
    access_token = input("Enter your BigCommerce Access Token: ").strip()

    if not store_hash or not access_token:
        raise SystemExit("Store hash and access token are required.")

    rows = fetch_all_customers_with_subresources(store_hash, access_token)
    write_csv(rows, OUTPUT_CSV)


if __name__ == "__main__":
    main()