
def apply_inbank_prefix(INBANK_API_PREFIX: str, original_url: str) -> str:
    """
    Универсальное применение префикса.
    Пример:
      INBANK_API_PREFIX = "https://gate.rshb.ru/ex/cbr"
      original_url      = "https://www.cbr.ru/banking_sector/credit/FullCoList/"
      -> "https://gate.rshb.ru/ex/cbr/banking_sector/credit/FullCoList/"

    Логика:
      - Берём последний сегмент префикса (хвост), например 'cbr' / 'rosstat' / 'moex'.
      - В оригинальной ссылке ищем этот сегмент (регистр игнорируется).
      - Заменяем всю левую часть, включая домен до ближайшего справа '.ru',
        на INBANK_API_PREFIX (без завершающего '/'), и добавляем остаток.
      - Если после '.ru' сразу нет '/', добавим его (кроме случаев '?', '#').

    Ограничение:
      - В оригинальной ссылке после найденного сегмента должен встречаться '.ru'.
        (Например, 'cbr.ru', 'rosstat.gov.ru' — подойдёт. Если домен не '.ru', выбросим исключение.)
    """
    if not isinstance(INBANK_API_PREFIX, str) or not isinstance(original_url, str):
        raise ValueError("INBANK_API_PREFIX и original_url должны быть строками.")

    prefix = INBANK_API_PREFIX.rstrip("/")
    # последний сегмент после '/'
    if "/" not in prefix:
        raise ValueError("INBANK_API_PREFIX должен содержать хотя бы один '/'.")
    tail = prefix.rsplit("/", 1)[-1].strip()
    if not tail:
        raise ValueError("INBANK_API_PREFIX должен оканчиваться непустым сегментом (например, '/cbr').")

    s = original_url.strip()
    lower = s.lower()

    # Начинаем поиск после схемы (если есть)
    scheme_pos = lower.find("://")
    start = scheme_pos + 3 if scheme_pos != -1 else 0

    pos_tail = lower.find(tail.lower(), start)
    if pos_tail == -1:
        raise ValueError(f"В оригинальной ссылке не найден сегмент '{tail}' для замены префиксом.")

    pos_ru = lower.find(".ru", pos_tail)
    if pos_ru == -1:
        raise ValueError("В оригинальной ссылке после найденного сегмента отсутствует '.ru' — не могу применить префикс.")

    # Остаток после '.ru'
    remainder = s[pos_ru + 3:]  # 3 == len(".ru")

    # Нормализуем разделитель между префиксом и остатком
    if not remainder.startswith(("/", "?", "#")):
        remainder = "/" + remainder

    return prefix + remainder
