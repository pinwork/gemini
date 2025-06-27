# Website Analyzer - Gemini AI Integration

Аналізатор веб-сайтів з використанням Gemini AI для автоматичного виявлення бізнес-характеристик та сегментації доменів.

## Системні вимоги та встановлення

### Скачати та встановити Python (add path натиснути галочку) та Microsoft Visual C++
- https://www.python.org/ftp/python/3.12.9/python-3.12.9-amd64.exe
- https://aka.ms/vs/17/release/vc_redist.x64.exe

### Налаштування політики виконання скриптів у PowerShell (виконати один раз)
```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

### Перейти в папку скрипта (виконати що разу перед запуском скрипта)
```bash
cd C:\github_projects\gemini
```

### Створення віртуального середовища на Windows (виконати один раз)
```bash
python -m venv venv
```

### Активація віртуального середовища (виконати що разу перед запуском скрипта)
```powershell
.\venv\Scripts\activate
```

### Встановлення залежностей проекту (виконати один раз)
```bash
pip install -r requirements.txt
```

## Контроль виконання

Управління скриптом через `config/script_control.json`:
```json
{
  "enabled": true
}
```
Встановіть `false` для правильної зупинки обробки.


## Структура проекту

```
gemini/
├── src/                              # Основний код проекту
│   ├── __init__.py                   # Python package marker
│   ├── main.py                       # Головний скрипт аналізатора
│   ├── prompts/                      # Модулі генерації промптів
│   │   ├── __init__.py              
│   │   ├── stage1_prompt_generator.py    # Генератор промптів для 1-го етапу
│   │   └── stage2_system_prompt_generator.py  # Генератор системних промптів для 2-го етапу
│   └── utils/                        # Допоміжні утиліти
│       ├── __init__.py              
│       ├── proxy_config.py           # Конфігурація та управління проксі-серверами
│       ├── validation_utils.py       # Валідаційні функції та утиліти
│       ├── logging_config.py         # Конфігурація логування системи
│       ├── network_error_classifier.py  # Класифікація мережевих помилок
│       └── mongo_operations.py       # Операції з MongoDB
├── config/                           # Конфігураційні файли
│   ├── mongo_config.json            # Налаштування MongoDB
│   ├── stage2_schema.json           # JSON схема для Gemini Stage2 API
│   └── script_control.json          # Файл контролю виконання скрипта
├── logs/                             # Логи виконання (створюється автоматично)
│   ├── system_errors.log           
│   ├── success_timing.log           
│   ├── rate_limits.log              
│   ├── ai_segmentation_validation.log
│   └── ... (інші log файли)         
├── venv/                             # Віртуальне середовище Python
├── requirements.txt                  # Python залежності
└── README.md                         # Документація проекту
```

## Модульне тестування

Кожен модуль може бути протестований незалежно:

```bash
# Тестування генератора промптів Stage1
python src/prompts/stage1_prompt_generator.py

# Тестування генератора системних промптів Stage2  
python src/prompts/stage2_system_prompt_generator.py

# Тестування конфігурації проксі
python src/utils/proxy_config.py

# Тестування валідаційних утиліт
python src/utils/validation_utils.py

# Тестування конфігурації логування
python src/utils/logging_config.py

# Тестування класифікатора мережевих помилок
python src/utils/network_error_classifier.py
```

## Моніторинг та логи

### Ключові лог файли:
- `success_timing.log` - успішні операції та час відповіді
- `rate_limits.log` - досягнення лімітів API
- `ai_segmentation_validation.log` - проблеми сегментації
- `proxy_errors.log` - помилки проксі підключень
- `system_errors.log` - загальні системні помилки

### Контроль якості:
```bash
# Моніторинг активності
tail -f logs/success_timing.log

# Перевірка проблем
tail -f logs/ai_segmentation_validation.log
```