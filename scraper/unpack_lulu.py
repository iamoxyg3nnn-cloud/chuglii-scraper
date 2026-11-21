import re

with open('scraper/lulu_eval.txt', 'r', encoding='utf-8') as f:
    text = f.read()

# Look for Dean Edwards packing pattern
pattern = r"\}\('(.*)',(\d+),(\d+),'(.*)'\.split\('\|'\)"
match = re.search(pattern, text)

if match:
    p, a, c, k = match.groups()
    a = int(a)
    c = int(c)
    k = k.split('|')

    def baseN(num, b):
        digits = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        if num == 0:
            return '0'
        res = []
        while num:
            res.append(digits[num % b])
            num //= b
        return ''.join(reversed(res))

    lookup = {baseN(i, a): (k[i] if i < len(k) and k[i] else baseN(i, a)) for i in range(c)}

    def repl(m):
        w = m.group(0)
        return lookup.get(w, w)

    unpacked = re.sub(r'\b\w+\b', repl, p)
    print("Unpacked successfully! Length:", len(unpacked))
    print("\n--- UNPACKED SNIPPET ---")
    print(unpacked[:1000])

    m3u8s = re.findall(r'https?://[^\s\"\'<>]+\.m3u8[^\s\"\'<>]*', unpacked)
    print("\nDIRECT M3U8 URLS FOUND:", m3u8s)
else:
    print("No pattern match, text length was:", len(text))
    print("Text snippet:", text[:200])
