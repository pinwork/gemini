#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import re

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
    
    "DOMAIN SEGMENTATION:",
    "Website: {domain_full}",
    "Domain core: {domain_core}",
    "Split domain core into space-separated semantic words for segments_full field. If domain cannot be meaningfully segmented, return original as single segment. When joined without spaces, segments must recreate original domain core exactly.",
    "EXAMPLES:",
    "bookstore.com (domain_core 'bookstore' we split into 'book store')",
    "web24market.com (domain_core 'web24market' we split into 'web 24 market')",
    "preshopify.com (domain_core 'preshopify' we split into 'pre shop ify')",
    "IMPORTANT: Even compound/blended brand names have underlying segment logic - identify and split it. Examples: 'pastebin' = 'paste bin', 'freepik' = 'free pic'. Always analyze word construction, not brand identity.",
    "SEGMENT CATEGORIZATION:",
    "Distribute only the previously split segments from segments_full into appropriate categories. Do not invent new segments. A segment can appear in multiple fields if it logically fits their definitions. If multiple segments fit one category, separate with spaces. Leave fields empty if no segments fit clearly.",
    "STRUCTURAL CATEGORIES:",
    "Primary segments: Main nouns carrying core business meaning (shop, market, food, web)",
    "Descriptive segments: Adjectives that modify main words (fast, smart, quick)",
    "Prefix segments: Word prefixes that could be reused (pre, super, mega)",
    "Suffix segments: Word suffixes that could be reused (ify, ly, er)",
    "SEMANTIC CATEGORIES:",
    "Additionally, segments can be classified as thematic vs common:",
    "Thematic segments: Words that specifically match website's topic or industry",
    "Common segments: Popular word components that could fit any domain as universal parts",
    "SEGMENT LANGUAGE:",
    "Identify the language of the actual words used in domain segments. Use ISO 639-1 codes (en, de, fr), 'mixed' for multiple languages, 'unknown' for unidentifiable terms."
]

_CRIT = {"must", "should", "need", "always", "double-check",
         "NOT", "geo_scope", "ALL", "Focus", "VALIDATION"}

_PRECOMPILED_WORD_PATTERNS = {}
_PRECOMPILED_PHRASE_LOOKUP = {}

def _precompile_patterns():
    """Компілює всі regex patterns один раз при імпорті модуля"""
    global _PRECOMPILED_WORD_PATTERNS, _PRECOMPILED_PHRASE_LOOKUP
    
    for word in word_variations:
        if word not in _CRIT:
            _PRECOMPILED_WORD_PATTERNS[word] = re.compile(rf'\b{re.escape(word)}\b', re.I)
    
    for phrase, variations in phrase_variations.items():
        _PRECOMPILED_PHRASE_LOOKUP[phrase] = variations

_precompile_patterns()

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
    return rep.upper() if src.isupper() else rep.capitalize() if src[0].isupper() else rep


def _fix_this_list(segment: str) -> str:
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


def _apply_words_optimized(text: str) -> str:
    """Оптимізована версія з pre-compiled regex patterns"""
    for word, pattern in _PRECOMPILED_WORD_PATTERNS.items():
        text = pattern.sub(lambda m: _pc(m.group(0), random.choice(word_variations[word])), text)
    return text


def generate_system_prompt(segment_combined: str = "", domain_full: str = "", failed_segments_full: str = "") -> str:
    domain_core = segment_combined.replace(" ", "") if segment_combined else ""
    
    sections_with_variables = []
    for section in base_sections:
        if "{domain_full}" in section:
            sections_with_variables.append(section.format(domain_full=domain_full))
        elif "{domain_core}" in section:
            sections_with_variables.append(section.format(domain_core=domain_core))
        else:
            sections_with_variables.append(section)
    
    if failed_segments_full:
        failed_joined = failed_segments_full.replace(' ', '')
        sections_with_variables.append(f"RETRY WARNING: Your previous segments_full '{failed_segments_full}' failed validation. Result joins to: '{failed_joined}' These do NOT match! Please provide segments that join exactly to '{domain_core}'. CRITICAL: Do NOT add or remove any letters - split only existing characters. Keep hyphens as separate segments, do not expand abbreviations or fix spelling.")
    
    txt = " ".join(
        _apply_words_optimized(random.choice(_PRECOMPILED_PHRASE_LOOKUP.get(s, [s])))
        for s in sections_with_variables
    )
    
    for pat, rep in compiled_fixes:
        txt = pat.sub(rep, txt)
    
    txt = re.sub(r'\s*,\s*', ', ', txt)
    txt = re.sub(r'\s{2,}', ' ', txt)
    
    return txt.strip()


if __name__ == "__main__":
    test_segment = "book store"
    test_domain = "bookstore.com"
    test_failed = "global multimedia protocols group"
    
    print("=== Optimized Stage2 Prompt Generator Test ===")
    print(f"Pre-compiled word patterns: {len(_PRECOMPILED_WORD_PATTERNS)}")
    print(f"Pre-compiled phrase variations: {len(_PRECOMPILED_PHRASE_LOOKUP)}")
    
    print("\n=== Normal prompt ===")
    import time
    start_time = time.time()
    for i in range(100):
        system_prompt = generate_system_prompt(test_segment, test_domain)
    end_time = time.time()
    print(f"100 prompt generations took: {(end_time - start_time):.4f}s")
    print(system_prompt[-200:])
    
    print("\n=== Retry prompt with failed segments ===")
    retry_prompt = generate_system_prompt(test_segment, test_domain, test_failed)
    print(retry_prompt[-200:])
    
    print(f"\n🚀 OPTIMIZED with pre-compiled regex patterns!")