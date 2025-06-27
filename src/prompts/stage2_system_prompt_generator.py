#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import re

# Словник варіацій слів для stage2 системного промпту
word_variations = {
    "analyzer": ["analyzer", "evaluator", "specialist", "expert", "reviewer"],
    "intelligence": ["intelligence", "insights", "information"],
    "business": ["business", "commercial", "enterprise"],
    "optimized": ["optimized", "designed", "configured", "tailored"],
    "extracting": ["extracting", "gathering", "collecting", "obtaining"],
    "Analyze": ["Analyze", "Examine", "Review", "Study", "Evaluate", "Assess"],
    "provide": ["provide", "deliver", "generate", "produce", "supply", "offer"],
    "Focus": ["Focus", "Concentrate", "Emphasize", "Prioritize"],
    "fill": ["fill", "populate", "complete", "enter"],
    "add": ["add", "include", "incorporate"],
    "use": ["use", "utilize", "employ"],
    "avoid": ["avoid", "exclude", "omit"],
    "comprehensive": ["comprehensive", "detailed", "thorough", "complete", "extensive"],
    "structured": ["structured", "organized", "systematic", "formatted"],
    "meaningful": ["meaningful", "valuable", "significant", "important", "relevant", "useful"],
    "appropriate": ["appropriate", "suitable", "fitting", "proper", "relevant"],
    "neutral": ["neutral", "generic", "general"],
    "clearly": ["clearly", "explicitly", "evidently", "obviously"],
    "professional": ["professional", "industry", "business", "standard"],
    "characteristics": ["characteristics", "attributes", "properties", "features", "traits"],
    "website": ["website", "site", "web page", "platform"],
    "content": ["content", "information", "material"],
    "fields": ["fields", "parameters", "entries"],
    "responses": ["responses", "outputs", "results"],
    "understanding": ["understanding", "comprehension", "interpretation", "analysis"],
    "functionality": ["functionality", "function", "purpose", "operation"],
    "essence": ["essence", "core", "nature", "substance"],
    "semantic": ["semantic", "meaning-based", "contextual"],
    "regardless": ["regardless", "irrespective"],
    "only": ["only", "exclusively"],
    "instead": ["instead", "rather than"]
}

# Фразові варіації для stage2
phrase_variations = {
    "You are a website content analyzer": [
        "You are a website content analyzer",
        "You function as a website content analyzer",
        "You serve as a website content analyzer"
    ],
    "optimized for extracting structured business intelligence": [
        "optimized for extracting structured business intelligence",
        "designed for gathering organized business insights",
        "configured for collecting systematic commercial data",
        "specialized in obtaining formatted enterprise information"
    ],
    "Analyze website content and provide comprehensive business characteristics": [
        "Analyze website content and provide comprehensive business characteristics",
        "Examine web page material and deliver detailed commercial attributes",
        "Review site information and generate complete business features",
        "Evaluate online content and produce thorough business properties"
    ],
    "RESPONSE STANDARDS:": [
        "RESPONSE STANDARDS:", "OUTPUT REQUIREMENTS:", "RESPONSE CRITERIA:"
    ],
    "Focus on semantic understanding": [
        "Focus on semantic understanding",
        "Emphasize contextual comprehension",
        "Prioritize meaning-based analysis"
    ],
    "For optional URL fields:": [
        "For optional URL fields:",
        "For URL parameters that are optional:",
        "For non-required URL elements:"
    ],
    "provide valid URL if detected, otherwise omit the field entirely from response": [
        "provide valid URL if detected, otherwise omit the field entirely from response",
        "supply valid URL when found, otherwise exclude the field completely from output",
        "include valid URL if present, otherwise remove the field from response entirely"
    ],
    "CONTENT VALIDATION:": [
        "CONTENT VALIDATION:", "INPUT VERIFICATION:", "DATA VALIDATION:"
    ],
    "The input content provided for analysis may contain errors": [
        "The input content provided for analysis may contain errors",
        "The source material given for review may include mistakes",
        "The provided data for examination may have inaccuracies"
    ],
    "Always double-check and replace or restructure content": [
        "Always double-check and replace or restructure content",
        "Constantly verify and substitute or reorganize material",
        "Continuously validate and modify or reformat information"
    ],
    "SPECIAL ATTENTION for summary, similarity_search_phrases and vector_search_phrase fields:": [
        "SPECIAL ATTENTION for summary, similarity_search_phrases and vector_search_phrase fields:",
        "SPECIFIC EMPHASIS for summary, similarity_search_phrases and vector_search_phrase parameters:",
        "PARTICULAR FOCUS for summary, similarity_search_phrases and vector_search_phrase elements:"
    ],
    "Optimize responses for vectorization": [
        "Optimize responses for vectorization",
        "Configure output for vector processing"
    ],
    "by avoiding meaningless noise words": [
        "by avoiding meaningless noise words",
        "by excluding redundant filler terms"
    ],
    "Do not use site names, brand names, proper names, or numbers": [
        "Do not use site names, brand names, proper names, or numbers",
        "Avoid website names, brand identifiers, proper nouns, or numerical values"
    ],
    "Replace brand names or website names with neutral terms": [
        "Replace brand names or website names with neutral terms",
        "Substitute company names or site names with generic terms",
        "Exchange brand identifiers or web names with neutral expressions"
    ],
    "Focus on describing the core essence and functionality directly": [
        "Focus on describing the core essence and functionality directly",
        "Emphasize describing the central purpose and capability directly"
    ],
    "Instead of primitively starting descriptions with basic phrases": [
        "Instead of primitively starting descriptions with basic phrases",
        "Rather than simply beginning descriptions with elementary terms",
        "Avoid starting summaries with rudimentary expressions"
    ]
}

# Базові секції для системного промпту
base_sections = [
    "You are a website content analyzer optimized for extracting structured business intelligence.",
    "Analyze website content and provide comprehensive business characteristics.",
    
    "RESPONSE STANDARDS:",
    "Return empty string instead of placeholder values like 'unknown', 'unclear', 'unavailable', etc.",
    "Focus on semantic understanding to extract meaningful business insights.",
    "For optional URL fields: provide valid URL if detected, otherwise omit the field entirely from response.",
    
    "CONTENT VALIDATION:",
    "The input content provided for analysis may contain errors with brand names instead of neutral terms and verbose descriptions with marketing filler words. Always double-check and replace or restructure content according to the requirements described in this system prompt during your generation.",
    
    "SPECIAL ATTENTION for summary and similarity_search_phrases fields:",
    "Optimize responses for vectorization by avoiding meaningless noise words.",
    "Do not use site names, brand names, proper names, or numbers.",
    "Replace brand names or website names with neutral terms like website, platform, service, tool.",
    "Focus on describing the core essence and functionality directly.",
    "Every word should carry meaningful semantic value related to website content.",
    
    "For SUMMARY field:",
    "Instead of primitively starting descriptions with basic phrases like 'this is', 'the website', 'it provides', company names, brand mentions, immediately highlight the platform's useful features using format: [Category/Adjective] + [Core Function] + [Target/Scope].",
    "Examples: 'Payment aggregator providing...', 'French news publication covering...', 'High-performance tensor library for...', etc.",
    "Focus on business model and functional category for effective similarity matching.",
    
    "For SIMILARITY_SEARCH_PHRASES: Build 3-4 keyword phrases for finding similar websites. Focus on core business function + industry/technology. Use specific terms, avoid brand names and generic words. Example: project management software, team collaboration, task tracking platform, agile workflow tools, etc.",
    
    "DOMAIN FORMATION ANALYSIS:",
    "For this website, automatic segmentation algorithms suggested: \"{segment_combined}\"",
    "However, I am not confident this segmentation is correct. These algorithms are based on English dictionary patterns and ignore business context, domain owner's logic, and industry-specific or non-English terminology.",
    "Your task is to provide semantically correct segmentation by understanding the website owner's perspective. Analyze the website content to determine what words the owner likely intended when creating this domain name. Return properly segmented words separated by spaces that reconstruct the domain core when joined without spaces. Ignore any hyphens in the domain core for segmentation purposes. Preserve non-English words as-is.",
    "CRITICAL: Domain Formation Classification. This data will be used for generating similar domain names, so professional analysis approach is essential. From your corrected segmentation, classify the domain formation pattern using one of these types:",
    "FORMATION PATTERNS - dictionary_word (existing word), compound_words (real words combined), coined_word (invented word), portmanteau (word parts merged), truncated_word (shortened word), acronym (pronounceable initials), initialism (spelled-out letters), personal_name (person's name), geographic_name (place name), numeric_hybrid (includes numbers), transliterated (from other language), hybrid_formation (mixed patterns), unclear_formation (cannot determine).",
    "THEMATIC PARTS - Words directly related to the website's industry, business theme, or service specifics. These parts must be tightly thematically connected to the site's topic or industry and carry the core business meaning.",
    "GENERIC PARTS - Common words frequently used across different domain themes to help secure available domain names. These are theme-neutral but add naming variety. Examples: smart, pro, hub, global, new, plus, max, etc.",
    "EXCLUDE from both categories: Brand names, abbreviations (unless industry-standard), random letter/number combinations, meaningless sequences, proper names. If no words fit either category, leave respective fields empty."
]

# Критичні слова які не підлягають заміні
_CRIT = {"must", "should", "need", "always", "double-check",
         "NOT", "geo_scope", "ALL", "Focus", "VALIDATION"}

# Попередньо скомпільовані регулярні вирази для продуктивності
compiled_words = {w: re.compile(rf'\b{re.escape(w)}\b', re.I)
                  for w in word_variations if w not in _CRIT}

# Попередньо скомпільовані фікси для типових помилок
compiled_fixes = [
    (re.compile(r'\bspecialist specialized\b', re.I), 'specialist'),
    (re.compile(r'standard industry-standard', re.I), 'industry-standard'),
    (re.compile(r'\bmeaningful meaning-based\b', re.I), 'meaningful'),
    (re.compile(r'\brather than of\b', re.I), 'rather than'),
    (re.compile(r'\bcore and (functionality|function|purpose|operation|capability)\b', re.I),
     r'core essence and \1'),
    (re.compile(r'\b(Emphasize|Prioritize) on\b', re.I), r'\1'),
    (re.compile(r'\bConcentrate\s+(?!on\b)(\w+ing)\b', re.I), r'Concentrate on \1'),
    (re.compile(r'\bFocus\s+(?!on\b)(\w+ing)\b', re.I), r'Focus on \1'),
    (re.compile(r'\bthe the\b', re.I), 'the'),
    (re.compile(r'\bcore core\b', re.I), 'core'),
    (re.compile(r'\bindustry industry-standard\b', re.I), 'industry-standard'),
    (
        re.compile(r'(this\s+\w+)(?:,\s*this\s+\w+)*,\s*this\s+(\w+)', re.I),
        lambda m: _fix_this_list(m.group(0))
    )
]


def _pc(src, rep):
    """Зберігає регістр оригінального слова"""
    return rep.upper() if src.isupper() else rep.capitalize() if src[0].isupper() else rep


def _fix_this_list(segment: str) -> str:
    """Виправляє повторювані 'this' в списках"""
    words = re.findall(r'this\s+(\w+)', segment, flags=re.I)
    seen = set()
    out = []
    for w in words:
        wl = w.lower()
        if wl in seen:
            out.append('resource')
        else:
            out.append(w)
            seen.add(wl)
    return ", ".join(f"this {w}" for w in out)


def _apply_words(text: str) -> str:
    """Застосовує варіації слів до тексту"""
    for w, rx in compiled_words.items():
        text = rx.sub(lambda m: _pc(m.group(0), random.choice(word_variations[w])), text)
    return text


def generate_system_prompt(segment_combined: str = "") -> str:
    """
    Генерує системний промпт для другого етапу аналізу веб-сайту
    
    Args:
        segment_combined: Сегментований домен для аналізу (наприклад, "book store")
        
    Returns:
        Згенерований системний промпт для stage2
    """
    # Підставляємо segment_combined в базові секції
    sections_with_segment = []
    for section in base_sections:
        if "{segment_combined}" in section:
            sections_with_segment.append(section.format(segment_combined=segment_combined))
        else:
            sections_with_segment.append(section)
    
    # Застосовуємо варіації фраз і слів
    txt = " ".join(
        _apply_words(random.choice(phrase_variations[s]) if s in phrase_variations else s)
        for s in sections_with_segment
    )
    
    # Застосовуємо фікси для типових помилок
    for pat, rep in compiled_fixes:
        txt = pat.sub(rep, txt)
    
    # Нормалізуємо пробіли
    txt = re.sub(r'\s*,\s*', ', ', txt)
    txt = re.sub(r'\s{2,}', ' ', txt)
    
    return txt.strip()


if __name__ == "__main__":
    # Тестування модуля з прикладом сегментації
    test_segment = "example test domain"
    system_prompt = generate_system_prompt(test_segment)
    print("Generated Stage2 System Prompt:")
    print("=" * 50)
    print(f"Test segment: '{test_segment}'")
    print("=" * 50)
    print(system_prompt)
    print("=" * 50)
    print(f"System prompt length: {len(system_prompt)} characters")