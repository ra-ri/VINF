from pyspark import SparkContext, SparkConf
import os
import re
import glob
import shutil
from datetime import datetime

# Spark context
conf = SparkConf().setAppName("ExtractCountryTVSeries")
sc = SparkContext(conf=conf)

# Dump
input_path = r"C:\Users\JLR\Documents\FIIT_ING\VINF2\enwiki-latest-pages-articles.xml"

# Output path
output_path = rf"C:\Users\JLR\Documents\FIIT_ING\VINF2\tv_series_{datetime.now().strftime('%Y%m%d%H%M%S')}.tsv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Countries to match in categories
countries = [
    "South Korean", "Chinese", "Japanese", "Hong Kong", "Philippine",
    "Taiwanese", "Thai", "Singaporean"
]

# Regex for categories like "drama" or "television series"
tv_keywords = r"(?:television series|tv series|drama)"

# Dynamically build regex to capture different orders and years
tvseries_pattern = re.compile(
    rf"\[\[\s*Category:(?:\d{{4}}\s+)?(?:{'|'.join(countries)})(?:\s+\d{{4}})?\s+{tv_keywords}.*?\]\]",
    flags=re.IGNORECASE)

# Blacklist for filtering
blacklist = [
    "dramatists", "playwrights", "actor", "actress", "actors", "actresses",
    "film", "films", "movie", "movies", "animation", "anime", "manga",
    "director", "screenwriter", "voice actor"
]


def extract_year(page):
    # Searching for template {{start date|YYYY|...}}
    start_match = re.search(r"\|\s*first_aired\s*=\s*\{\{\s*start date\|(\d{4})", page, flags=re.IGNORECASE)
    if start_match:
        return start_match.group(1)

    # If template is not present, directly look for year in text
    fallback = re.search(r"\|\s*first_aired\s*=\s*[^\d]*(\d{4})", page, flags=re.IGNORECASE)
    if fallback:
        return fallback.group(1)
    return "\t"


def extract_genres(page):
    genre_pattern = re.compile(r"\|\s*genre\s*=\s*(.+?)(?=\n\s*\|\s*[a-zA-Z_]+\s*=|\n\}\}|$)",
                               flags=re.IGNORECASE | re.DOTALL)

    genre_match = genre_pattern.search(page)
    if not genre_match:
        return []

    genre_block = genre_match.group(1).strip()

    # Removal of HTML tags ( &lt;br&gt; )
    genre_block = re.sub(r"&lt;\s*br\s*/?\s*&gt;", " ", genre_block, flags=re.IGNORECASE)

    # Remove all tags
    genre_block = re.sub(r"<[^>]+>", " ", genre_block)

    # Cleaning  {{ubl|...}}, {{plainlist|...}}, {{hlist|...}}
    genre_block = re.sub(r"\{\{(?:ubl|plainlist|hlist)\|", "", genre_block, flags=re.IGNORECASE)
    genre_block = genre_block.replace("}}", "")
    genre_block = genre_block.replace("*", "")

    # Extracting [[...]]
    genres = re.findall(r"\[\[([^|\]]+)", genre_block)

    # If there are no [[...]], split by delimiters
    if not genres:
        raw = re.split(r"[,/|]", genre_block)
        genres = [g.strip() for g in raw if g.strip()]

    # Removal of empty and numeric values
    genres = [g for g in genres if len(g.split()) <= 4 and not re.search(r"\d", g)]

    return genres


def extract_network(page):
    network_match = re.search(r"\|\s*network\s*=\s*(.+?)(?=\n\s*\|\s*[a-zA-Z_]+\s*=|\n\}\}|$)", page,
                              flags=re.IGNORECASE | re.DOTALL)

    if not network_match:
        return "\t"

    network_block = network_match.group(1).strip()

    network_block = re.sub(r"&lt;!--.*?--&gt;", " ", network_block, flags=re.DOTALL)
    network_block = re.sub(r"&lt;\s*br\s*/?\s*&gt;", " ", network_block, flags=re.IGNORECASE)
    network_block = re.sub(r"<[^>]+>", " ", network_block)

    networks = []

    # If plainlist, extract items marked with *
    plainlist_match = re.search(r"\{\{plainlist\|(.+?)\}\}", network_block, flags=re.IGNORECASE | re.DOTALL)
    if plainlist_match:
        items_block = plainlist_match.group(1)
        networks += re.findall(r"\*\s*([^\n*]+)", items_block)

    links = re.findall(r"\[\[([^|\]]+)", network_block)
    networks += links

    # Remove years in parentheses and empty items
    networks = [re.sub(r"\(.*?\)", "", n).strip() for n in networks if n.strip()]

    if networks:
        # Remove duplicates and join with comma
        return ", ".join(sorted(set(networks)))

    return "\t"


def clean_person_block(block: str) -> str:
    # Stop at the next field like "| xyz ="
    block = re.split(r"\n\s*\|[a-zA-Z_]+\s*=", block)[0]

    # Remove HTML and comments
    block = re.sub(r"&lt;!--.*?--&gt;", " ", block)
    block = re.sub(r"&lt;\s*br\s*/?\s*&gt;", ", ", block)
    block = re.sub(r"<[^>]+>", " ", block)

    # Remove templates {{plainlist| ... }}, {{ubl|...}}, {{unbulleted list|...}}
    block = re.sub(r"\{\{(?:plainlist|ubl|unbulleted list|hlist)\|", "", block, flags=re.IGNORECASE)
    block = block.replace("{{", "").replace("}}", "")
    block = block.replace("*", " ")

    # Cleaning unnecessary pipes and tabs
    block = block.replace("\t", " ").replace("|", " ")

    # Removal of extra spaces and lines
    block = re.sub(r"\s+", " ", block).strip()

    return block


def extract_names(block: str) -> list:
    #  [[Name]] alebo [[Name|...]]
    names = re.findall(r"\[\[([^|\]]+)", block)

    # If there are no [[...]], split by delimiters
    if not names:
        raw = re.split(r"[,/&]", block)
        names = [n.strip() for n in raw if n.strip()]

    cleaned = []
    for n in names:
        # Remove parentheses, years, numbers
        n = re.sub(r"\(.*?\)", "", n)
        n = re.sub(r"\d{4}–\d{2,4}", "", n)
        n = re.sub(r"\d{4}", "", n)

        # Replace hyphens and dots with space (Kang Byung-taek to Kang Byung Taek)
        n = re.sub(r"[-.]", " ", n)

        # Remove special characters
        n = re.sub(r"[^A-Za-zÀ-ž' ]", "", n)

        # Remove multiple spaces
        n = re.sub(r"\s+", " ", n).strip()

        # Capitalize each word
        n = " ".join(w.capitalize() for w in n.split())

        if n:
            cleaned.append(n)

    # Remove duplicates
    seen = set()
    result = []
    for n in cleaned:
        if n.lower() not in seen:
            seen.add(n.lower())
            result.append(n)

    return result


def extract_directors(page):
    match = re.search(r"\|\s*director\s*=\s*(.+?)(?=\n\s*\|\s*[a-zA-Z_]+\s*=|\n\}\}|$)", page,
                      flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return "\t"

    block = match.group(1).strip()

    # If empty or contains only other fields (| producer, | writer, etc.)
    if not block or re.fullmatch(r"[\s|=_]*", block):
        return "\t"

    # Remove part where e.g. "| writer =" appears
    block = re.split(r"\|\s*(writer|producer|editor|creator)\s*=", block, flags=re.IGNORECASE)[0]

    block = clean_person_block(block)
    if not block or re.fullmatch(r"(?i)\s*(writer|producer|editor|creator)\s*", block):
        return "\t"

    names = extract_names(block)
    return ", ".join(names) if names else "\t"


def extract_producers(page):
    match = re.search(r"\|\s*producer\s*=\s*(.+?)(?=\n\s*\|\s*[a-zA-Z_]+\s*=|\n\}\}|$)", page,
                      flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return "\t"

    block = match.group(1).strip()

    # If empty or contains only other fields
    if not block or re.fullmatch(r"[\s|=_]*", block):
        return "\t"

    # Remove part where e.g. "| writer =" appears
    block = re.split(r"\|\s*(writer|director|editor|creator)\s*=", block, flags=re.IGNORECASE)[0]

    block = clean_person_block(block)
    if not block or re.fullmatch(r"(?i)\s*(writer|director|editor|creator)\s*", block):
        return "\t"

    names = extract_names(block)
    return ", ".join(names) if names else "\t"


def extract_num_episodes(page):
    match = re.search(r"\|\s*num_episodes\s*=\s*(.+?)(?=\n\s*\|\s*[a-zA-Z_]+\s*=|\n\}\}|$)", page,
                      flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return "\t"

    block = match.group(1).strip()

    # Remove HTML tags, comments, wiki templates
    block = re.sub(r"&lt;!--.*?--&gt;", " ", block)
    block = re.sub(r"<[^>]+>", " ", block)
    block = re.sub(r"\{\{[^{}]+\}\}", " ", block)
    block = re.sub(r"[^0-9]", " ", block)

    # Extract the first number
    num_match = re.search(r"\b(\d{1,4})\b", block)
    if num_match:
        return num_match.group(1)

    return "\t"


def extract_maintext_info(page):
    # First, find and remove the entire {{Infobox }}
    infobox_start = page.find("{{Infobox")

    if infobox_start == -1:
        # If there is no infobox, try to find text from the beginning up to ==
        main_text_match = re.search(r"^(.+?)(?:==|$)", page, flags=re.DOTALL)
        if main_text_match:
            text_after_infobox = main_text_match.group(1)
        else:
            return "\t"
    else:
        # Count brackets to find the end of the infobox
        bracket_count = 0
        i = infobox_start
        infobox_end = -1

        while i < len(page):
            if page[i:i + 2] == "{{":
                bracket_count += 1
                i += 2
            elif page[i:i + 2] == "}}":
                bracket_count -= 1
                if bracket_count == 0:
                    infobox_end = i + 2
                    break
                i += 2
            else:
                i += 1

        if infobox_end == -1:
            return "\t"

        # Take text after infobox up to the first section ==
        text_after_infobox = page[infobox_end:]

    # Find text after the end of the infobox up to ==
    main_text_match = re.search(r"^\s*(.+?)(?:==|$)", text_after_infobox, flags=re.DOTALL)

    if not main_text_match:
        return "\t"

    main_text = main_text_match.group(1).strip()

    # Remove remaining }} at the beginning
    main_text = re.sub(r"^\s*\}+\s*", "", main_text)

    # Remove HTML
    main_text = re.sub(r"&lt;", "<", main_text)
    main_text = re.sub(r"&gt;", ">", main_text)
    main_text = re.sub(r"&amp;", "&", main_text)
    main_text = re.sub(r"&quot;", '"', main_text)

    # Odstráenie referencie <ref>...</ref>
    main_text = re.sub(r"<ref[^>]*>.*?</ref>", "", main_text, flags=re.DOTALL | re.IGNORECASE)

    # Remove HTML comments
    main_text = re.sub(r"<!--.*?-->", "", main_text, flags=re.DOTALL)

    # Remove other tags
    main_text = re.sub(r"<[^>]+>", "", main_text)

    # Remove templates {{...}} and nested
    for _ in range(5):
        main_text = re.sub(r"\{\{[^{}]*\}\}", "", main_text)

    # Remove wiki links but keep text: [[link|text]] > text, [[link]] -> link
    main_text = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", main_text)

    # Remove URLs
    main_text = re.sub(r"\[https?://[^\]]+\]", "", main_text)

    # Remove bold and italics (''' and '')
    main_text = re.sub(r"'{2,}", "", main_text)

    # Remove multiple spaces and newlines
    main_text = re.sub(r"\s+", " ", main_text)

    # Take only the first 500 characters
    main_text = main_text.strip()
    if len(main_text) > 500:
        main_text = main_text[:500] + "..."

    return main_text if main_text else "\t"


def extract_cast(page):
    # Find Cast section
    cast_match = re.search(r"==+\s*Cast\s*==+(.+?)(?:==+[^=]|$)", page, flags=re.IGNORECASE | re.DOTALL)

    if not cast_match:
        return "\t"

    cast_section = cast_match.group(1)

    # Remove subheadings
    cast_section = re.sub(r"===+[^=]+===+", "", cast_section)

    actors = []

    # Format 1: Lists with * [[Actor Name]]
    bullet_actors = re.findall(r"\*+\s*\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", cast_section)
    actors.extend(bullet_actors)

    # Format 2: Without wiki links
    plain_actors = re.findall(r"\*+\s*([A-Z][A-Za-z\s'-]+?)(?:\s+[-–—]|\s+as\s+|\s*<br|\n)", cast_section)
    actors.extend(plain_actors)

    # Format 3: HTML <br /> separators
    br_actors = re.findall(r"(?:'''|\*)\s*\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", cast_section)
    actors.extend(br_actors)

    # Clean actor names
    cleaned_actors = []
    for actor in actors:
        actor = actor.strip()

        # Remove notes in parentheses
        actor = re.sub(r"\([^)]*\)", "", actor)

        # Remove numbers and years
        actor = re.sub(r"\d{4}", "", actor)

        # Replace hyphens and dots with space
        actor = re.sub(r"[-.]", " ", actor)

        # Remove multiple spaces
        actor = re.sub(r"\s+", " ", actor).strip()

        # Capitalize each word
        actor = " ".join(w.capitalize() for w in actor.split())

        # Filter
        if actor and len(actor) > 2 and len(actor) < 50 and re.search(r"[A-Za-z]", actor):
            cleaned_actors.append(actor)

    # Remove duplicates
    seen = set()
    unique_actors = []
    for actor in cleaned_actors:
        actor_lower = actor.lower()
        if actor_lower not in seen and actor_lower not in ["cast", "main", "supporting", "guest"]:
            seen.add(actor_lower)
            unique_actors.append(actor)

    # Limit to the first 20 actors
    unique_actors = unique_actors[:20]

    return ", ".join(unique_actors) if unique_actors else "\t"


def extract_awards(page):
    awards_list = []

    # Combine all sections with possible names
    award_sections = re.findall(r"==+\s*(?:Accolades|Awards and nominations)\s*==+(.+?)(?==+[^=]|$)", page,
                                flags=re.IGNORECASE | re.DOTALL)

    if not award_sections:
        return []

    for section in award_sections:
        text = section.strip()

        # Remove comments, HTML, templates {{...}}
        text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
        for _ in range(5):
            text = re.sub(r"\{\{[^{}]*\}\}", "", text)
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"\s+", " ", text).strip()

        # First format: wikitable with !Year !Award !Category !Recipient !Result
        rows = re.findall(r"\|-\s*(.+?)(?=\|-\s*|$)", text, flags=re.DOTALL)
        for row in rows:
            cols = re.findall(r"\| ?(.+?)(?=\||$)", row)
            cols = [c.strip() for c in cols if c.strip()]
            if not cols:
                continue

            # Try to heuristically identify year and result
            year = next((c for c in cols if re.match(r"\d{4}", c)), "\t")
            result = next((c for c in cols if re.search(r"won|nominated", c, re.IGNORECASE)), "\t")

            # Combine the rest as Award, Category, Nominee
            others = [c for c in cols if c != year and c != result]
            while len(others) < 3:
                others.append("\t")  # doplniť prázdne

            award, category, nominee = others[:3]
            awards_list.append(f"{year}\t{award}\t{category}\t{nominee}\t{result.capitalize()}")

        # Second format: lists ;Year Award\n*Category - Nominee - Result
        lines = text.split("\n")
        current_year_award = ["\t", "\t"]
        for line in lines:
            line = line.strip()
            if line.startswith(";"):
                year_match = re.search(r"(\d{4})", line)
                award_match = re.sub(r"\d{4}", "", line).strip("; ").strip()
                current_year_award = [year_match.group(1) if year_match else "\t",
                                      award_match if award_match else "\t"]
            elif line.startswith("*"):
                parts = re.split(r"-|:", line[1:], maxsplit=1)
                category = parts[0].strip() if len(parts) > 0 else "\t"
                nominee = parts[1].strip() if len(parts) > 1 else "\t"
                year, award = current_year_award
                result = "Nominated" if "nom" in line.lower() else "Won" if "win" in line.lower() else "\t"
                awards_list.append(f"{year}\t{award}\t{category}\t{nominee}\t{result}")

    return awards_list if awards_list else []


def extract_reception(page):
    reception_match = re.search(r"==+\s*Reception\s*==+(.+?)(?:==+[^=]|$)", page, flags=re.IGNORECASE | re.DOTALL)

    if not reception_match:
        return "\t"

    reception_text = reception_match.group(1).strip()

    # Remove nested subheadings
    reception_text = re.sub(r"===+[^=]+===+", "", reception_text)

    # Remove templates {{...}}
    for _ in range(5):
        reception_text = re.sub(r"\{\{[^{}]*\}\}", "", reception_text)

    # Remove HTML comments
    reception_text = re.sub(r"<!--.*?-->", "", reception_text, flags=re.DOTALL)

    # Remove HTML tags
    reception_text = re.sub(r"<[^>]+>", "", reception_text)

    # Remove wiki links, keep text [[link|text]] -> text, [[link]] -> link
    reception_text = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", reception_text)

    # Remove URLs
    reception_text = re.sub(r"\[https?://[^\]]+\]", "", reception_text)

    # Remove bold and italics (''' and '')
    reception_text = re.sub(r"'{2,}", "", reception_text)

    # Remove multiple spaces and newlines
    reception_text = re.sub(r"\s+", " ", reception_text).strip()

    # Take the first 500 characters
    if len(reception_text) > 500:
        reception_text = reception_text[:500] + "..."

    return reception_text if reception_text else "\t"


def extract_tv_series_info(page):
    page = page.strip()
    if not page:
        return []

    # Extraction of title
    title_match = re.search(r"<title>(.*?)</title>", page)
    title = title_match.group(1) if title_match else "UNKNOWN"

    # Remove parentheses with expressions like (TV series), (TV program), (television series), etc.
    title = re.sub(
        r"\s*\([^)]*(?:TV|television|series|program|show|drama|South Korean|Chinese|Japanese|Hong Kong|Philippine|Taiwanese|Thai|Singaporean|\d{4})[^)]*\)\s*",
        "", title,
        flags=re.IGNORECASE).strip()

    # Extraction of categories
    categories = re.findall(r"\[\[Category:(.*?)\]\]", page)

    # Check if at least one category matches the pattern
    matched_countries = []
    for cat in categories:
        if re.search(tvseries_pattern, f"[[Category:{cat}]]"):
            for country in countries:
                if country.lower() in cat.lower():
                    matched_countries.append(country)

    if not matched_countries:
        return []

    # Check blacklist
    for cat in categories:
        cat_lower = cat.lower()
        if any(bad in cat_lower for bad in blacklist):
            return []

    # Helper functions
    first_aired_year = extract_year(page)
    genres = extract_genres(page)
    network = extract_network(page)
    directors = extract_directors(page)
    producers = extract_producers(page)
    num_episodes = extract_num_episodes(page)
    cast = extract_cast(page)
    info = extract_maintext_info(page)
    reception = extract_reception(page)
    awards = extract_awards(page)

    # Formatting output
    country_str = ", ".join(sorted(set(matched_countries)))
    genre_str = ", ".join(sorted(set(genres))) if genres else "\t"
    awards_str = "; ".join(awards) if awards else "\t"

    # Cleaning text fields
    def sanitize(text):
        if text is None:
            return ""
        return str(text).replace("\t", " ").replace("\n", " ").strip()

    fields = [
        title,
        country_str,
        first_aired_year,
        genre_str,
        network,
        directors,
        producers,
        num_episodes,
        cast,
        info,
        reception,
        awards_str
    ]

    sanitized = [sanitize(f) for f in fields]
    return ["\t".join(sanitized)]


# Načítanie  dumpu
lines = sc.textFile(input_path)

# Processing pages
pages = lines.mapPartitions(lambda part: ["\n".join(part)]).flatMap(lambda x: x.split("<page>"))

# Extracting TV series
extracted = pages.flatMap(extract_tv_series_info)

# Writing to temporary folder
temp_output = output_path + "_temp"
extracted.saveAsTextFile(temp_output)

# TSV header
header = "title\tcountry\tyear\tgenre\tpublisher\tdirector\tproducer\tepisodes\tactors\tinfo\treception\tawards"

# Merging parts into a single file
part_files = sorted(glob.glob(os.path.join(temp_output, "part-*")))

if part_files:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for part_file in part_files:
            with open(part_file, "r", encoding="utf-8") as pf:
                content = pf.read()
                if content.strip():
                    f.write(content)
                    if not content.endswith("\n"):
                        f.write("\n")

    # Deleting temporary files
    shutil.rmtree(temp_output)

    # Counting results
    total_count = extracted.count()

    print(f"DONE! RESULT SAVED IN: {output_path}")
    print(f"NUMBER OF SERIES FOUND: {total_count}")
else:
    print("NOTHING FOUND")

sc.stop()