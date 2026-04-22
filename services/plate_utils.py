import re


PLATE_COL_PATTERNS = [
    "رقم اللوحة", "رقم اللوحه", "اللوحة", "اللوحه",
    "لوحة", "لوحه", "رقم اللوح", "full_plate", "plate", "plate no"
]

_AR_HARAKAT_TATWEEL = re.compile(r"[\u0640\u064B-\u065F\u0670]")
_SPACE_ZW = re.compile(r"[\s\u200b\u200c\u200d\ufeff]+")
_SEPARATORS = re.compile(r"[\s\u200b\u200c\u200d\ufeff\-._/]+")


def _digits_western_from_chars(s: str) -> str:
    out: list[str] = []
    for ch in s:
        if "0" <= ch <= "9":
            out.append(ch)
        elif "\u0660" <= ch <= "\u0669":
            out.append(str(ord(ch) - 0x0660))
        elif "\u06F0" <= ch <= "\u06F9":
            out.append(str(ord(ch) - 0x06F0))
    return "".join(out)


def _normalize_plate_letters_segment(letters: str) -> str:
    letters = re.sub(r"[^A-Za-z\u0600-\u06FF]+", "", str(letters or ""))
    letters = _AR_HARAKAT_TATWEEL.sub("", letters)
    letters = re.sub(r"[أإآٱ]", "ا", letters)
    letters = letters.replace("\u0649", "\u064a")
    letters = re.sub(r"[ة]", "ه", letters)
    return letters.lower()


def _plate_from_compact(compact: str) -> tuple[str, bool]:
    """
    لوحة سعودية: حروف (بترتيب الظهور في النص بعد إزالة الفواصل) ثم أرقام.
    يقبل مثل: «2636 و ص ر»، «س-ا-ج-3744»، «ساج 3744».
    """
    letters_raw = "".join(ch for ch in compact if ch.isalpha())
    digits_raw = _digits_western_from_chars(compact)
    letters = _normalize_plate_letters_segment(letters_raw)
    numbers = re.sub(r"\D+", "", digits_raw)
    if not letters or not numbers:
        return "", False
    if not (1 <= len(letters) <= 3 and 1 <= len(numbers) <= 4):
        return "", False
    return f"{letters}{numbers}", True


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
    letters = _AR_HARAKAT_TATWEEL.sub("", letters)
    letters = _normalize_plate_letters_segment(letters)
    numbers = re.sub(r"\D+", "", str(numbers_raw or ""))

    if (not letters or not numbers) and full_raw:
        compact = _SEPARATORS.sub("", str(full_raw or ""))
        if not letters:
            letters_raw2 = "".join(ch for ch in compact if ch.isalpha())
            letters = _normalize_plate_letters_segment(letters_raw2)
        if not numbers:
            numbers = re.sub(r"\D+", "", _digits_western_from_chars(compact))

    is_valid = bool(
        letters and numbers and len(letters) <= 3 and len(numbers) <= 4
    )
    return (f"{letters}{numbers}" if is_valid else "", is_valid)


def _normalize_plate_legacy(s: str) -> str:
    """سلوك قديم خفيف عندما لا يُستخرج نمط لوحة واضح."""
    s = str(s or "").strip()
    s = _SPACE_ZW.sub("", s)
    s = _AR_HARAKAT_TATWEEL.sub("", s)
    s = re.sub(r"[أإآٱ]", "ا", s)
    s = s.replace("\u0649", "\u064a")
    s = re.sub(r"[ة]", "ه", s)
    return s.lower()


def normalize_plate(s: str) -> str:
    """
    مفتاح مطابقة: حروف ثم أرقام بدون مسافة (مثلاً وصر2636، ساج3744).
    يُوحّد «مع فاصل / من غير فاصل / شرطات / أرقام قبل الحروف».
    """
    raw = str(s or "").strip()
    if not raw:
        return ""
    compact = _SEPARATORS.sub("", raw)
    key, ok = _plate_from_compact(compact)
    if ok:
        return key
    return _normalize_plate_legacy(raw)


def format_plate_display(s: str) -> str:
    """
    عرض بمسافة بين الحروف والأرقام (مثل: «ساج 3744»).
    يقبل النص الخام أو المفتاح المضغوط من normalize_plate.
    """
    key = normalize_plate(str(s or ""))
    if not key:
        t = str(s or "").strip()
        return t
    first_d = next((i for i, ch in enumerate(key) if ch.isdigit()), None)
    if first_d is None or first_d == 0:
        return key
    letters, nums = key[:first_d], key[first_d:]
    return f"{letters} {nums}".strip()


def auto_detect_plate_col(headers: list[str]) -> str | None:
    """Detect which header column contains plate numbers."""
    for h in headers:
        if h is None:
            continue
        hs = str(h).strip()
        hs_l = hs.lower()
        for pat in PLATE_COL_PATTERNS:
            if pat.lower() in hs_l:
                return hs
    return None


def _looks_like_plate_text(value: str) -> bool:
    """Accept formats like: وصر 3243 or و ص ر 3453 or س-ا-ج-3744."""
    s = str(value or "").strip()
    if not s:
        return False
    compact = _SEPARATORS.sub("", s)
    _, ok = _plate_from_compact(compact)
    if ok:
        return True
    s2 = re.sub(r"[^\u0600-\u06FF0-9\s]", " ", s)
    s2 = re.sub(r"\s+", " ", s2).strip()
    m = re.match(r"^([\u0600-\u06FF ]{1,8})\s+([0-9]{1,4})$", s2)
    if not m:
        return False
    letters = re.sub(r"\s+", "", m.group(1) or "")
    numbers = m.group(2) or ""
    return bool(1 <= len(letters) <= 3 and 1 <= len(numbers) <= 4)


def auto_detect_plate_col_from_row3(headers: list[str], row3: tuple | list | None) -> str | None:
    """Detect plate column by checking Excel row 3 values."""
    if not headers or row3 is None:
        return None
    vals = list(row3)
    for i, h in enumerate(headers):
        if not h:
            continue
        v = vals[i] if i < len(vals) else None
        if _looks_like_plate_text("" if v is None else str(v)):
            return str(h).strip()
    return None
