import re


PLATE_COL_PATTERNS = [
    "رقم اللوحة", "رقم اللوحه", "اللوحة", "اللوحه",
    "لوحة", "لوحه", "رقم اللوح", "full_plate", "plate"
]


def normalize_plate_value(
    letters_raw: str = "",
    numbers_raw: str = "",
    full_raw: str = ""
) -> tuple[str, bool]:
    """
    Normalize plate:
    - strip spaces from letters
    - max 3 letters + max 4 numbers
    Returns (normalized_plate, is_valid)
    """
    letters = re.sub(r"[^A-Za-z\u0600-\u06FF]+", "", str(letters_raw or ""))
    # Remove Arabic tatweel and harakat globally (prevents mismatches like "هـ" vs "ه").
    letters = re.sub(r"[\u0640\u064B-\u065F\u0670]", "", letters)
    numbers = re.sub(r"\D+", "", str(numbers_raw or ""))

    if (not letters or not numbers) and full_raw:
        compact = re.sub(r"[\s\-_/]+", "", str(full_raw or ""))
        if not letters:
            letters = "".join(ch for ch in compact if ch.isalpha())
        if not numbers:
            numbers = "".join(ch for ch in compact if ch.isdigit())

    is_valid = bool(
        letters and numbers and len(letters) <= 3 and len(numbers) <= 4
    )
    return (f"{letters}{numbers}" if is_valid else "", is_valid)


def normalize_plate(s: str) -> str:
    """Normalize plate string for comparison/matching."""
    s = str(s or "").strip()
    s = re.sub(r"[\s\u200b\u200c\u200d\ufeff]+", "", s)
    # Remove Arabic tatweel and harakat globally (ـ َ ً ُ ٌ ِ ٍ ْ ّ ٰ).
    s = re.sub(r"[\u0640\u064B-\u065F\u0670]", "", s)
    s = re.sub(r"[أإآٱ]", "ا", s)
    s = re.sub(r"[ى]", "ي", s)
    s = re.sub(r"[ة]", "ه", s)
    return s.lower()


def auto_detect_plate_col(headers: list[str]) -> str | None:
    """Detect which header column contains plate numbers."""
    for h in headers:
        if h is None:
            continue
        hs = str(h).strip()
        for pat in PLATE_COL_PATTERNS:
            if pat in hs:
                return hs
    return None