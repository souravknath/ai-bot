# Script to create a properly UTF-8 encoded .env file
with open('.env', 'w', encoding='utf-8') as f:
    f.write('DHAN_API_KEY=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ4NDE5MTIyLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTY1Nzk4NCJ9.mdqgKkG2brOROTZwbmjtgdMylrQ0xtLTKhA23RqrRVSlaiN9uc_VZTPt1Te5DPi7G2GT3QePBgr_kKYsVbbMhw')
print("Created .env file with proper UTF-8 encoding")