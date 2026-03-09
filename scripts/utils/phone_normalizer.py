# phone_normalizer.py - Нормализация телефонных номеров
import re


def normalize_phone(phone):
    """
    Приводит телефон к формату +79161234567
    Примеры:
    89161234567 → +79161234567
    +7 (916) 123-45-67 → +79161234567
    8-916-123-45-67 → +79161234567
    9161234567 → +79161234567
    """
    if not phone:
        return None

    # Удаляем всё кроме цифр
    digits = re.sub(r'\D', '', str(phone))

    if len(digits) == 10:
        # 9161234567 → +79161234567
        return f"+7{digits}"
    elif len(digits) == 11:
        if digits.startswith('8'):
            # 89161234567 → +79161234567
            return f"+7{digits[1:]}"
        elif digits.startswith('7'):
            # 79161234567 → +79161234567
            return f"+{digits}"
    elif len(digits) > 11:
        # Берем последние 10 цифр (на случай лишних кодов)
        return f"+7{digits[-10:]}"

    # Если не получилось нормализовать, возвращаем None
    return None