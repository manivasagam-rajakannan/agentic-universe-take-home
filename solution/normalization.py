"""Validation shared by lead ingestion and event linking."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import hashlib
import hmac
import re


IST = timezone(timedelta(hours=5, minutes=30))
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y")
CURRENCY = re.compile(r"^(?:₹|Rs\.?|INR)\s*", re.IGNORECASE)
DECIMAL = re.compile(
    r"[+-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,2}(?:,[0-9]{2})+,[0-9]{3})"
    r"(?:\.[0-9]*)?|[+-]?\.[0-9]+"
)


def header_key(value):
    return re.sub(r"[_\s]+", " ", value.strip().lower())


def canonical_phone(value):
    if not isinstance(value, str):
        return None
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if re.fullmatch(r"[6-9][0-9]{9}", digits):
        return "+91" + digits
    return None


def phone_token(key, workflow, phone):
    message = f"phone\0{workflow}\0{phone}".encode("utf-8")
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def key_fingerprint(key):
    return hmac.new(key, b"interaction-ledger-key-v1", hashlib.sha256).hexdigest()


def normalize_date(value):
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(value, date_format).date().isoformat()
        except ValueError:
            continue
    return None


def normalize_decimal(value):
    value = CURRENCY.sub("", value, count=1)
    if not DECIMAL.fullmatch(value):
        return None
    try:
        amount = Decimal(value.replace(",", ""))
        if amount >= 0:
            with localcontext() as context:
                context.prec = max(28, len(amount.as_tuple().digits) + 3)
                rounded = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                return format(rounded.copy_abs(), "f")
    except InvalidOperation:
        pass
    return None


def normalize_field(field, value):
    kind = field["type"]
    if kind == "string":
        return value
    if kind == "phone":
        return canonical_phone(value)
    if kind == "date":
        return normalize_date(value)
    if kind == "decimal":
        return normalize_decimal(value)
    if kind == "enum":
        value = value.lower()
        return value if value in field["values"] else None
    raise ValueError(f"Unsupported field type: {kind}")


def ist_timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=IST)
        return stamp.astimezone(IST).isoformat(timespec="microseconds")
    except (ValueError, OverflowError):
        return None
