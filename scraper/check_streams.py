from curl_cffi import requests
import re
import json

s = requests.Session(impersonate="chrome124")
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0"}

# 1. VOE
voe_url = "https://voe.sx/e/gphvuoqqkr71"
r_voe = s.get(voe_url, headers=headers)
print("VOE final URL:", r_voe.url)
print("VOE status:", r_voe.status_code)

# VOE has an encrypted JSON payload or base64 sources
# Let's see what is inside <script type="application/json">
json_scripts = re.findall(r'<script type="application/json">([\s\S]*?)</script>', r_voe.text)
print("VOE JSON scripts count:", len(json_scripts))
for js in json_scripts:
    print("VOE JSON snippet:", js[:100])

# Let's search for var sources or hls or mp4 in r_voe.text
matches = re.findall(r'(https?://[^\s\"\'<>]+\.(?:m3u8|mp4)[^\s\"\'<>]*)', r_voe.text)
print("VOE URL matches:", matches)

# 2. LuluStream
lulu_url = "https://luminscdn.xyz/e/gytoxg4qml1r"
r_lulu = s.get(lulu_url, headers={"Referer": "https://supjav.com/"})
print("\nLuluStream status with supjav referer:", r_lulu.status_code)
# Search for m3u8 in lulustream
lulu_m3u8 = re.findall(r'(https?://[^\s\"\'<>]+\.m3u8[^\s\"\'<>]*)', r_lulu.text)
print("LuluStream m3u8 matches:", lulu_m3u8)

# 3. What if we extract the packed eval in LuluStream?
for line in r_lulu.text.split("\n"):
    if "eval(function(p,a,c,k,e,d)" in line:
        print("Found packed eval in LuluStream, len:", len(line))
        # Save line for unpacking
        with open("scraper/lulu_eval.txt", "w", encoding="utf-8") as f:
            f.write(line)
