import requests
import json
import csv
import time
from tqdm import tqdm

print("\n--BigCommerce graphQL Account User Permissions CSV Export--\n")

# Set your BigCommerce API credentials
ACCOUNT_UUID = input("Enter your BigCommerce Account UUID: ")
ACCESS_TOKEN = input("Enter your BigCommerce Access Token: ")
API_URL = f"https://api.bigcommerce.com/accounts/{ACCOUNT_UUID}/graphql"

# GraphQL query to fetch users and their permissions
def get_users_query(cursor=None):
    after_cursor = f', after: "{cursor}"' if cursor else ""
    return f"""
    query {{
      account {{
        id
        stores {{
          edges {{
            node {{
              id
              name
              storeHash
              users(first: 50{after_cursor}) {{
                pageInfo {{
                    startCursor
                    endCursor
                    hasNextPage
                }}  
                edges {{
                  node {{
                    id
                    email
                    firstName
                    lastName
                    locale
                    lastLoginAt
                    permissions
                    status
                    updatedAt
                  }}
                  cursor
                }}
              }}
            }}
          }}
        }}
      }}
    }}"""

def make_request(query):
    headers = {
        "Content-Type": "application/json",
        "X-Auth-Token": ACCESS_TOKEN
    }
    retries = 0
    while retries < 5:
        response = requests.post(API_URL, headers=headers, json={"query": query})
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            wait_time = (2 ** retries) * 2  # Exponential backoff with increased delay
            print(f"Rate limited. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retries += 1
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None
    print("Max retries reached. Exiting.")
    return None

def fetch_users():
    users = []
    data = make_request(get_users_query())
    if data:
        stores = data.get("data", {}).get("account", {}).get("stores", {}).get("edges", [])
        total_stores = len(stores)
        with tqdm(total=total_stores, desc="Fetching Users", unit="store") as pbar:
            for store in stores:
                store_node = store.get("node", {})
                store_hash = store_node.get("storeHash", "")
                page_info = store_node.get("users", {}).get("pageInfo", {})
                has_next_page = page_info.get("hasNextPage", False)
                end_cursor = page_info.get("endCursor")
                
                while True:
                    store_users = store_node.get("users", {}).get("edges", [])
                    
                    for user in store_users:
                        node = user.get("node", {})
                        node["storeHash"] = store_hash  # Attach storeHash to user node
                        users.append(node)
                    
                    if not has_next_page:
                        break
                    
                    time.sleep(1.5)  # Add delay between requests to prevent rate limiting
                    data = make_request(get_users_query(end_cursor))
                    if data:
                        store_node = data.get("data", {}).get("account", {}).get("stores", {}).get("edges", [])[0].get("node", {})
                        page_info = store_node.get("users", {}).get("pageInfo", {})
                        has_next_page = page_info.get("hasNextPage", False)
                        end_cursor = page_info.get("endCursor")
                    else:
                        break
                pbar.update(1)
    return users

def save_to_csv(users, filename="users_permissions.csv"):
    excluded_permissions = {"No_Permission_Required", "IntegratedApps_InstallApp", "IntegratedApps_LoadApp", "IntegratedApps_UninstallApp"}
    all_permissions = set()
    for user in users:
        all_permissions.update(user.get("permissions", []))
    all_permissions = sorted(all_permissions - excluded_permissions)
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        header = ["Store Hash", "Email", "Last Login At", "Status"] + all_permissions
        writer.writerow(header)
        
        for user in users:
            store_hash = user.get("storeHash", "")
            email = user.get('email', '')
            last_login_at = user.get('lastLoginAt', 'N/A')
            status = user.get('status', 'Unknown')
            user_permissions = set(user.get("permissions", []))
            permission_markers = ["X" if perm in user_permissions else "" for perm in all_permissions]
            writer.writerow([store_hash, email, last_login_at, status] + permission_markers)

def main():
    users = fetch_users()
    
    if users:
        print("Users and Permissions saved to CSV.")
        save_to_csv(users)
    else:
        print("No users found or an error occurred.")

if __name__ == "__main__":
    main()