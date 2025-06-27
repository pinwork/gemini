#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random

# Словник варіацій слів для stage1 промпта
stage1_word_variations = {
    "analyzer": ["analyzer", "evaluator", "specialist", "expert", "reviewer"],
    "intelligence": ["intelligence", "insights", "information"],
    "business": ["business", "commercial", "enterprise"],
    "optimized": ["optimized", "designed", "configured", "tailored"],
    "extracting": ["extracting", "gathering", "collecting", "obtaining"],
    "Analyze": ["Analyze", "Examine", "Review", "Study", "Evaluate", "Assess"],
    "provide": ["provide", "deliver", "generate", "produce", "supply", "offer"],
    "comprehensive": ["comprehensive", "detailed", "thorough", "complete", "extensive"],
    "meaningful": ["meaningful", "valuable", "significant", "important", "relevant", "useful"],
    "appropriate": ["appropriate", "suitable", "fitting", "proper", "relevant"],
    "professional": ["professional", "industry", "business", "standard"],
    "website": ["website", "site", "web page", "platform"],
    "content": ["content", "information", "material"],
    "understanding": ["understanding", "comprehension", "interpretation", "analysis"],
    "clearly": ["clearly", "explicitly", "evidently", "obviously"],
    "regardless": ["regardless", "irrespective"],
    "only": ["only", "exclusively"]
}

# Базове введення для stage1 промпта
stage1_base_intro = [
    "You are a website content {analyzer} {optimized} for {extracting} structured {business} {intelligence}.",
    "{Analyze} {website} {content} and {provide} {comprehensive} {business} characteristics.",
    "All responses must be in English {only}, {regardless} of original {content} language.",
    "If urlContext tool fails to access website, {clearly} state 'Website inaccessible' and stop analysis. If website shows coming soon page, maintenance page, parked domain page, hosting placeholder, suspended page, domain for sale page, etc., {clearly} state 'Placeholder page' and stop analysis. If website is functional and suitable for analysis, proceed with analysis according to the prompt below.",
    "Focus on semantic {understanding} to extract {meaningful} {business} insights.",
    "For fields requiring URL detection, return the exact URL including subdomains if present (e.g., blog.example.com, support.company.org).",
    "When analyzing contact information, thoroughly examine contact pages, legal/privacy pages, about pages, footer sections, and any other accessible pages to extract all available emails, phone numbers, and physical addresses.",
    "IMPORTANT: Provide responses for ALL fields and indicators below. If not found, write 'Not detected'. If found, describe in one sentence what exactly was detected."
]

# Всі детектори для stage1
stage1_all_detectors = {
    "b2c_detected": "True only if core offering targets individual consumers (B2C)",
    "b2b_detected": "True only if core offering targets businesses (B2B)",
    "personal_project_detected": "Personal project indicators (individual portfolio, personal blog, hobby project, single person effort)",
    "local_business_detected": "Physical brick-and-mortar business where customers can visit in person (restaurants, retail stores, salons, medical offices, repair shops, etc.). Indicates physical location for customer service delivery, not just geographic targeting",
    "pricing_page_detected": "Pricing information present",
    "blog_detected": "Blog or content updates present", 
    "ecommerce_detected": "Single-vendor online store functionality (company sells its own products directly to customers)",
    "hiring_detected": "True only if the site hosts its own career/apply pages or embeds a jobs widget for its staff",
    "api_available_detected": "True only if the site exposes its own public or partner API (docs link, /api page, swagger file)",
    "contact_page_detected": "Contact us, get in touch, or contact information page present",
    "payment_methods_detected": "True only if site's own checkout advertises Stripe, PayPal, crypto, or similar payment options",
    "analytics_tools_detected": "True only if embedded tracking script for Google Analytics, Hotjar, Mixpanel, etc. is present",
    "knowledge_base_detected": "True only if site hosts its own help/FAQ hub",
    "subscription_detected": "True only if the site charges recurring fees for its own product or content",
    "monetizes_via_ads_detected": "True only if display ads are served on this site (banner code, AdSense, sponsor slots)",
    "saas_detected": "True only if the site itself offers SaaS (login/signup to hosted software)",
    "recruits_affiliates_detected": "True only if the company runs its own affiliate/partner programme (signup form, terms page)",
    "community_platform_detected": "True only if the site hosts its own forum, user board, or social layer. Embedding generic comment widgets does not count",
    "funding_received_detected": "True only if the company running this site reports its own funding round (press-release, 'Investors' page)",
    "disposable_site_detected": "Low-quality or deceptive website indicators: thin content, excessive ads, push notifications, instant redirects, poor design, minimal navigation, or lacking trust signals",
    "mobile_first_detected": "True only if layout is clearly built mobile-first"
}

# Текстові поля для stage1
stage1_text_fields = {
    "website_summary": "Generate a comprehensive 5-7 sentence summary showcasing the website's core functionality, business purpose, and value proposition. Transform any brand names or site identifiers into neutral terms (platform, service, website, tool, system). Lead with descriptive business category or key differentiator using format: [Category/Adjective] + [Core Function] + [Target/Scope]. Examples: 'Payment aggregator providing...', 'French news publication covering...', 'High-performance tensor library for...'. Deliver immediate value by highlighting business model, target audience, main features/services, geographic scope, and revenue approach when identifiable. Craft each sentence to carry meaningful semantic weight for effective similarity matching and vectorization.",
    "similarity_search_phrases": "Build 3-4 keyword phrases for finding similar websites. Focus on core business function + industry/technology. Use specific terms, avoid brand names and generic words. Example: project management software, team collaboration, task tracking platform, agile workflow tools, etc. Use comma to separate multiple phrases.",
    "vector_search_phrase": "Create one precise 4-5 word phrase that captures the absolute essence of this website's business for finding the most similar websites. This is the single most important phrase for vector similarity matching. Focus on the core value proposition using the most specific industry terms. Example: agile project management platform, etc. Avoid generic words like 'service', 'platform', 'website'.",
    "cms_platform": "Name of CMS or site generator identified in source code",
    "primary_language": "ISO 639-1 two-letter code of dominant site language. For English use 'en', Spanish 'es', etc.",
    "external_links_count": "Total count of all external outbound links (including duplicates to same URLs)",
    "external_domains_count": "Count of unique external domains being linked to",
    "internal_links_count": "Total count of all internal links (including duplicates to same URLs)",
    "internal_pages_count": "Count of unique internal page URLs being linked to",
    "target_age_group": "Primary age group that the website/product targets based on content, design, language, and product offerings. Analyze visual design, content complexity, product types, marketing messages, and user interface patterns. Options: children (0-12), teens (13-17), young_adults (18-25), adults (26-40), middle_aged (41-55), seniors (56-65), elderly (65+), all_ages (suitable for all age groups), unspecified",
    "target_gender": "Primary gender targeting based on design, content and products. Options: male, female, unspecified",
    "geo_scope": "Geographic scope of this specific website's target audience, not the parent company. Global companies may have localized websites for specific regions. Options: local, regional (state/province within country), national, EU, North America, Asia Pacific, DACH (Germany+Austria+Switzerland), Nordic, CIS (post-Soviet states), Latin America, MENA (Middle East & North Africa), global. Select the most specific option based on content analysis. If scope unclear, analyze website language and TLD domain - non-English language with national TLD likely indicates 'national' scope. Default to 'global' if cannot determine",
    "blog_url": "Blog section URL in canonical format (RFC 3986) - provide only if blog detected",
    "recruits_affiliates_url": "Affiliate recruitment page URL in canonical format (RFC 3986) - provide only if affiliate program detected", 
    "contact_page_url": "Contact page URL in canonical format (RFC 3986) - provide only if contact page detected",
    "api_documentation_url": "API documentation page URL in canonical format (RFC 3986) - provide only if API documentation section detected",
    "app_platforms": "Software platforms developed by website. Examples: 'windows, macos, linux, ios, android, chrome, firefox, edge, safari, electron', etc. Use comma for multiple platforms.",
    "geo_country": "ISO country code where business operates (US, GB, CA, DE, etc.) - provide if determinable",
    "geo_region": "State/province/region using standard abbreviations where countries have them (CA/NY/TX for USA, ON/QC for Canada, BY/NW for Germany) or standard English names from Google Maps for other countries - provide if determinable",
    "geo_city": "City using standard English names without diacritics (e.g., Munich not München, Cologne not Köln) - provide if determinable",
    "email_list": "Extract all email addresses found on website for administration or support contact. Return as array with format: contact_email (RFC 5322 standard, convert obfuscated emails like 'john [at] example.com' to 'john@example.com'), contact_type (single industry-standard word: general, support, sales, info, admin, etc.), corporate (true if company domain, false for gmail/yahoo/etc.)",
    "phone_list": "Extract all phone numbers found on website for administration or service ordering. Return as array with format: phone_number (E164 international format like +1234567890), whatsapp (true/false), contact_type (single industry-standard word: general, support, sales, office, mobile, etc.)",
    "address_list": "Extract all physical addresses found on website for office visits or mailing. Return as array with format: full_address (complete address as found), address_type (single industry-standard word: headquarters, office, branch, warehouse, store, etc.), country (ISO 2-letter code)"
}


def apply_stage1_word_variations(text: str) -> str:
    """
    Застосовує варіації слів до тексту stage1 промпта
    
    Args:
        text: Текст для обробки
        
    Returns:
        Оброблений текст з застосованими варіаціями
    """
    for word, variations in stage1_word_variations.items():
        if word in text:
            text = text.replace(f"{{{word}}}", random.choice(variations))
    return text


def generate_stage1_prompt() -> str:
    """
    Генерує промпт для першого етапу аналізу веб-сайту
    
    Returns:
        Згенерований промпт для stage1
    """
    # Застосовуємо варіації до введення
    intro_text = " ".join(stage1_base_intro)
    intro_with_variations = apply_stage1_word_variations(intro_text)
    
    # Перемішуємо детектори для варіативності
    detector_items = list(stage1_all_detectors.items())
    random.shuffle(detector_items)
    
    # Перемішуємо текстові поля для варіативності
    text_items = list(stage1_text_fields.items())
    random.shuffle(text_items)
    
    # Будуємо секції аналізу
    analysis_sections = []
    
    # Додаємо секцію загального аналізу
    analysis_sections.append("\n=== GENERAL ANALYSIS ===")
    for field_name, description in text_items:
        analysis_sections.append(f"**{field_name.upper().replace('_', ' ')}**: {description}")
    
    # Додаємо секцію виявлення функцій
    analysis_sections.append("\n=== FEATURE DETECTION ===") 
    
    for detector_name, description in detector_items:
        clean_name = detector_name.replace('_detected', '').replace('_', ' ').title()
        analysis_sections.append(f"**{clean_name}**: {description}")
    
    # Фінальні інструкції
    final_instructions = [
        "\n=== OUTPUT FORMAT ===",
        "Structure your response clearly with headers for each section OR return the short exception message mentioned above for access/functionality issues.",
        "Be thorough but concise in your analysis.", 
        "MANDATORY: Address every single field and indicator listed above - do not skip any items.",
        "For SIMILARITY_SEARCH_PHRASES: Build 3-4 keyword phrases for finding similar websites. Focus on core business function + industry/technology. Use specific terms, avoid brand names and generic words. Example: project management software, team collaboration, task tracking platform, agile workflow tools, etc.",
        "For VECTOR_SEARCH_PHRASE: Create one precise 4-5 word phrase that captures the absolute essence of this website's business for finding the most similar websites. This is the single most important phrase for vector similarity matching. Focus on the core value proposition using the most specific industry terms. Example: agile project management platform, etc. Avoid generic words like 'service', 'platform', 'website'.",
        "If summary is the most detailed description and similarity_search_phrases break down all summary details into compact form ideal for vector search, then vector_search_phrase is perfectly distilled essence from summary."
    ]
    
    # Складаємо фінальний промпт
    full_prompt = "\n".join([
        intro_with_variations,
        *analysis_sections,
        *final_instructions
    ])
    
    return full_prompt


if __name__ == "__main__":
    # Тестування модуля
    prompt = generate_stage1_prompt()
    print("Generated Stage1 Prompt:")
    print("=" * 50)
    print(prompt)
    print("=" * 50)
    print(f"Prompt length: {len(prompt)} characters")