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
Встановіть `enabled: false` для правильної зупинки обробки.

## Структура проекту

```
gemini/
├── .gitattributes                    # Git налаштування нормалізації файлів
├── .gitignore                        # Git ігнорування файлів та папок
├── README.md                         # Документація проекту
├── requirements.txt                  # Python залежності
├── config/                           # Конфігураційні файли
│   ├── mongo_config.json            # Налаштування MongoDB підключення
│   ├── script_control.json          # Файл контролю виконання скрипта + retry_model
│   └── stage2_schema.json           # JSON схема для Gemini Stage2 API
├── src/                              # Основний код проекту
│   ├── __init__.py                   # Python package marker
│   ├── main.py                       # Головний скрипт аналізатора з воркерами
│   ├── prompts/                      # Модулі генерації промптів
│   │   ├── __init__.py              
│   │   ├── stage1_prompt_generator.py       # Генератор промптів для 1-го етапу
│   │   └── stage2_system_prompt_generator.py # Генератор системних промптів + retry warnings
│   └── utils/                        # Допоміжні утиліти та модулі
│       ├── __init__.py              
│       ├── gemini_client.py          # Клієнт з підтримкою dual-model system
│       ├── logging_config.py         # Конфігурація системи логування
│       ├── mongo_operations.py       # Операції з MongoDB з спрощеним логуванням
│       ├── network_error_classifier.py  # Класифікація мережевих помилок
│       ├── proxy_config.py           # Конфігурація та управління проксі-серверами
│       └── validation_utils.py       # Валідація та очистка даних
├── logs/                             # Логи виконання (створюється автоматично)
│   ├── system_errors.log           
│   ├── success_timing.log           
│   ├── rate_limits.log              
│   ├── ai_segmentation_validation.log
│   └── ... (інші log файли)         
└── venv/                             # Віртуальне середовище Python
```

## Модульне тестування

Кожен модуль може бути протестований незалежно:

```bash
# Тестування генератора промптів Stage1
python src/prompts/stage1_prompt_generator.py

# Тестування генератора системних промптів Stage2 з retry підтримкою
python src/prompts/stage2_system_prompt_generator.py

# Тестування Gemini клієнта з dual-model підтримкою
python src/utils/gemini_client.py

# Тестування конфігурації проксі
python src/utils/proxy_config.py

# Тестування валідаційних утиліт з новими функціями
python src/utils/validation_utils.py

# Тестування конфігурації логування
python src/utils/logging_config.py

# Тестування класифікатора мережевих помилок
python src/utils/network_error_classifier.py

# Тестування MongoDB операцій (потребує підключення до БД)
python src/utils/mongo_operations.py
```