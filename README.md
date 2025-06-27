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

### Запуск скрипта парсера
```bash
python -m src.main
```

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
│       └── network_error_classifier.py  # Класифікація мережевих помилок
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

## Модульна архітектура

### Stage 1 Prompt Generator (`src/prompts/stage1_prompt_generator.py`)
- **Призначення**: Генерація промптів для першого етапу аналізу веб-сайтів
- **Функціональність**: 
  - Варіації слів для промптів
  - Детектори бізнес-функцій
  - Текстові поля аналізу
- **Тестування**: `python src/prompts/stage1_prompt_generator.py`

### Stage 2 System Prompt Generator (`src/prompts/stage2_system_prompt_generator.py`)
- **Призначення**: Генерація системних промптів для другого етапу з AI сегментацією
- **Функціональність**:
  - Варіації фраз та слів
  - Інструкції для доменної сегментації
  - Підстановка segment_combined параметрів
- **Тестування**: `python src/prompts/stage2_system_prompt_generator.py`

### Proxy Configuration (`src/utils/proxy_config.py`)
- **Призначення**: Управління конфігурацією проксі-серверів з підтримкою session rotation
- **Функціональність**:
  - Підтримка HTTP, HTTPS, SOCKS4, SOCKS5 протоколів
  - Валідація IP адрес, доменів та портів
  - Автоматична генерація нових session ID для ротації
  - Аутентифікація з username/password
  - Утилітарні методи для aiohttp-socks integration
  - Створення ProxyConfig з URL строк
  - Валідація списків проксі конфігурацій
- **Ключові методи**:
  - `has_sessid()` - перевірка наявності session ID
  - `generate_new_sessid()` - генерація нової сесії
  - `get_connection_params()` - параметри для ProxyConnector
  - `test_different_ports()` - тестування різних портів
- **Тестування**: `python src/utils/proxy_config.py`

### Validation Utils (`src/utils/validation_utils.py`)
- **Призначення**: Валідація та очистка даних від Gemini API
- **Функціональність**:
  - Email, телефон, URL валідації
  - Детекція проблем доступу
  - AI сегментація валідація
  - Форматування summary та очистка даних
- **Тестування**: `python src/utils/validation_utils.py`

### Logging Configuration (`src/utils/logging_config.py`)
- **Призначення**: Централізована конфігурація всіх логгерів системи
- **Функціональність**:
  - 14 спеціалізованих логгерів (системні помилки, успішні операції, rate limits, проксі, API, мережа)
  - Функції логування з форматуванням та маскуванням чутливих даних
  - Ротація логів з налаштуваними розмірами та кількістю backup файлів
  - Централізована функція setup_all_loggers() для ініціалізації
- **Тестування**: `python src/utils/logging_config.py`

### Network Error Classifier (`src/utils/network_error_classifier.py`)
- **Призначення**: Класифікація та обробка всіх типів мережевих помилок
- **Функціональність**:
  - Класифікація 8 типів помилок (HTTP, проксі, мережа, SSL, DNS, timeout, payload, невідомі)
  - ErrorDetails dataclass з інформацією про помилку та рекомендованими діями
  - Утилітарні функції (is_proxy_error, should_retry_request, was_api_key_consumed)
  - Підтримка aiohttp та proxy винятків
- **Тестування**: `python src/utils/network_error_classifier.py`

### Main Script (`src/main.py`)
- **Призначення**: Основний скрипт координації аналізу
- **Функціональність**:
  - Керування worker'ами
  - Інтеграція з MongoDB
  - Логування та error handling
  - Проксі та API ключі менеджмент
  - Використання модульних компонентів

## Конфігураційні файли

### `config/mongo_config.json`
Налаштування підключення до MongoDB з параметрами баз даних та клієнта.

### `config/stage2_schema.json`
JSON схема для Gemini API Stage2, що визначає структуру відповіді з усіма полями аналізу веб-сайту.

### `config/script_control.json`
Файл контролю виконання скрипта - дозволяє зупиняти/запускати обробку доменів.

## Основні функції

- **Двоетапний аналіз веб-сайтів** через Gemini AI
- **Автоматична сегментація доменів** з валідацією
- **Детекція бізнес-моделей** (B2B/B2C, SaaS, eCommerce)
- **Витягування контактної інформації** (email, телефони, адреси)
- **Географічний та віковий таргетинг**
- **Валідація та очистка даних**
- **Логування всіх процесів**
- **Проксі rotation з session management**

## Технології

- **Python 3.12+**
- **Gemini AI API** (gemini-2.5-flash, gemini-2.0-flash)
- **MongoDB** для зберігання результатів
- **aiohttp** для асинхронних HTTP запитів
- **aiohttp-socks** для проксі підтримки
- **Проксі-сервери** для масштабування з session rotation
- **phonenumbers** для валідації телефонів

## Налаштування та запуск

1. Створіть та налаштуйте `config/mongo_config.json`
2. Переконайтесь що MongoDB доступна
3. Налаштуйте API ключі Gemini в базі даних
4. Запустіть: `python -m src.main`

## Логування

Система створює детальні логи в папці `logs/`:
- Успішні операції та час відповіді
- Rate limits та помилки API
- Валідація AI сегментації
- Помилки проксі та мережі
- Debug інформація коротких відповідей
- IP usage та proxy rotation

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

Кожен тест показує візуальні приклади роботи функцій для контролю якості.

## Архітектурні переваги модульного підходу

### 🔧 Модульність та масштабованість
- **Окремі модулі** для кожної функціональності
- **Незалежне тестування** кожного компонента
- **Легке розширення** функціональності

### 🛡️ Надійність
- **Централізоване логування** з 14 спеціалізованими логгерами
- **Інтелектуальна класифікація помилок** з автоматичними рекомендаціями
- **Проксі rotation** з session management для уникнення блокувань

### 🔍 Зручність розробки
- **Кожен модуль самотестований** з візуальним виводом
- **Чітка структура проекту** з логічною організацією
- **Документована архітектура** для швидкого розуміння

### ⚡ Продуктивність
- **40 concurrent worker'ів** для паралельної обробки
- **Асинхронна архітектура** з aiohttp
- **Оптимізована обробка помилок** без блокування процесу