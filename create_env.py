# Script to create a properly UTF-8 encoded .env file
with open('.env', 'w', encoding='utf-8') as f:
    f.write('DHAN_API_KEY=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ2NTI2MTY2LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTY1Nzk4NCJ9.ixEQrTtTSJN3bwaiD4wBNw43lmh1TavJDG-cbAoGXnWx1bI9NUo48cplHaMmwd7K6XhVy01wrIVDbyHR-Lbf6g')
print("Created .env file with proper UTF-8 encoding")