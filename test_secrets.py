import os

print("=== Environment Variables Test ===")
print(f"NOTION_TOKEN exists: {'NOTION_TOKEN' in os.environ}")
print(f"DATABASE_ID exists: {'DATABASE_ID' in os.environ}")
print(f"DATABASE_ID value: {os.environ.get('DATABASE_ID', 'NOT SET')}")

if 'NOTION_TOKEN' in os.environ:
    token = os.environ['NOTION_TOKEN']
    print(f"Token starts with 'secret_': {token.startswith('secret_')}")
    print(f"Token length: {len(token)}")
