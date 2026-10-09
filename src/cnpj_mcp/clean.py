"""Validation and normalisation for contacts found on websites. Used by the crawler and by the post-processing of a finished sweep."""
import re

VALID_DDD = {11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 24, 27, 28, 31, 32, 33, 34, 35, 37, 38, 41, 42, 43, 44, 45, 46, 47, 48, 49, 51, 53, 54, 55,
             61, 62, 63, 64, 65, 66, 67, 68, 69, 71, 73, 74, 75, 77, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 91, 92, 93, 94, 95, 96, 97, 98, 99}


def br_number(raw):
    """Return (national_digits, kind) for a valid Brazilian landline or mobile number, else (None, reason).
    Accepts 55 / 0055 / stray leading zeros. kind is 'mobile' or 'landline'."""
    d = re.sub(r"\D", "", str(raw))
    if d.startswith("00"):
        d = d[2:]
    d = d.lstrip("0")
    if len(d) in (12, 13) and d.startswith("55"):
        d = d[2:]
    elif len(d) not in (10, 11):
        return None, "foreign_or_bad_length"
    if int(d[:2]) not in VALID_DDD:
        return None, "bad_ddd"
    sub = d[2:]
    if len(sub) == 9:
        if sub[0] != "9":
            return None, "bad_mobile"
        kind = "mobile"
    else:
        if sub[0] not in "2345":
            return None, "bad_landline"
        kind = "landline"
    if len(set(sub)) <= 2 or re.search(r"(\d)\1{5,}", sub) or sub in "01234567890123456789" or sub in "98765432109876543210" or sub[-8:] in ("12345678", "87654321"):
        return None, "placeholder_pattern"
    return d, kind


SOCIAL_GENERIC = {
    "facebook": {"profile.php", "2008", "sharer.php", "sharer", "share.php", "share", "recover", "help", "https", "http", "pages", "people", "groups", "tr", "plugins",
                 "policies", "policy.php", "dialog", "login", "login.php", "watch", "events", "hashtag", "photo.php", "permalink.php", "story.php", "l.php", "business",
                 "ads", "marketplace", "legal", "about", "privacy", "terms", "gaming", "stories", "reel", "reels", "video.php", "videos", "photo", "photos", "home.php", "facebook"},
    "instagram": {"reel", "reels", "accounts", "explore", "p", "tv", "stories", "about", "whatsapp", "direct", "web", "instagram", "legal", "developer", "privacy", "challenge"},
    "x": {"share", "intent", "home", "whatsapp", "search", "i", "hashtag", "x", "twitter", "login", "privacy", "tos", "explore"},
    "tiktok": {"@https", "@http", "@tiktok"},
    "youtube": {"c/youtube", "@youtube"},
    "linkedin": set(),
}


def social_ok(item):
    kind, _, handle = item.partition(":")
    first = handle.split("/")[0].lower()
    if first in SOCIAL_GENERIC.get(kind, set()) or handle.lower() in SOCIAL_GENERIC.get(kind, set()):
        return False
    if kind == "facebook" and first in ("profile.php",):
        return False
    return True


# parked, expired, "site unavailable" and big-platform landing pages that a dead domain redirects to
PARKED_HOSTS = ("uni5.net", "hugedomains.com", "expireddomains.com", "buscaintegrada.com.br", "a.umbler.com", "umbler.com", "mpitemporario.com.br",
                "combrhost.com.br", "sedoparking.com", "sedo.com", "godaddy.com", "registro.br", "dan.com", "afternic.com", "parkingcrew.net", "bodis.com")
PLATFORM_HOSTS = ("instagram.com", "facebook.com", "accounts.google.com", "google.com", "outlook.live.com", "api.whatsapp.com", "whatsapp.com", "youtube.com",
                  "github.com", "linktr.ee", "wa.me", "x.com", "twitter.com", "linkedin.com")


def _host(u):
    m = re.match(r"https?://([^/:?#]+)", u or "")
    return (m.group(1).lower() if m else "")


def _reg(h):
    p = h.split(".")
    return ".".join(p[-3:]) if len(p) >= 3 and p[-2] in ("com", "net", "org", "gov", "adv", "edu", "ind", "eco", "med") and p[-1] == "br" else ".".join(p[-2:])


def landing_problem(domain, final_url):
    """'parked' when the domain redirects to a parking / hosting-unavailable page, 'platform' when it redirects to a social or login page."""
    h = _host(final_url)
    if not h:
        return None
    d = domain.lower().strip()
    if _reg(h) == _reg(d) or h.endswith("." + d) or d.endswith("." + h):
        return None
    if any(h == p or h.endswith("." + p) for p in PARKED_HOSTS):
        return "parked_or_unavailable"
    if any(h == p or h.endswith("." + p) for p in PLATFORM_HOSTS):
        return "redirects_to_platform"
    return None


def wa_number(raw):
    """WhatsApp variant of br_number: a 10-digit number whose subscriber part starts 6-9 is an old-format mobile; WhatsApp expects the extra 9, so add it."""
    n, kind = br_number(raw)
    if n:
        return n, kind
    if kind == "bad_landline":
        d = re.sub(r"\D", "", str(raw))
        d = d[2:] if d.startswith("00") else d
        d = d.lstrip("0")
        d = d[2:] if len(d) == 12 and d.startswith("55") else d
        if len(d) == 10 and d[2] in "6789":
            return br_number(d[:2] + "9" + d[2:])
    return n, kind
