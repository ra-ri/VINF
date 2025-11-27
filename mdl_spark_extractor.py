import os
import re
import html
import glob
import shutil
from pyspark import SparkContext, SparkConf

# Subory
PAGES_DIR = r"C:\Users\JLR\Documents\FIIT_ING\VINF2\P1\pages"
OUTPUT_FILE = "extracted.tsv"

# Regexy na extrahovanie informacii
RE_TITLE = re.compile(r'<title>\s*(.*?)\s*</title>', re.IGNORECASE | re.DOTALL)
RE_YEAR = re.compile(r'Aired:.*?(\d{4})', re.IGNORECASE | re.DOTALL)
RE_GENRES = re.compile(r'"genre"\s*:\s*\[(.*?)\]', re.IGNORECASE | re.DOTALL)
RE_PUBLISHER = re.compile(r'"publisher"\s*:\s*\{[^}]*"name"\s*:\s*"(.*?)"', re.IGNORECASE | re.DOTALL)
RE_ACTORS = re.compile(r'"actor"\s*:\s*\[(.*?)\]', re.DOTALL)
RE_EPISODES = re.compile(r'<b class="inline">Episodes:</b>\s*(\d+)', re.IGNORECASE)
RE_DURATION = re.compile(r'<b class="inline">Duration:</b>\s*([\d\w\s\.]+)', re.IGNORECASE)
RE_DIRECTOR = re.compile(r'<b class="inline">Director:</b>\s*<a[^>]*>(.*?)</a>', re.IGNORECASE)
RE_SCREENWRITER = re.compile(r'<b class="inline">Screenwriter:</b>\s*<a[^>]*>(.*?)</a>', re.IGNORECASE)
RE_SCORE = re.compile(r'<b class="inline">Score:</b>\s*([\d\.]+)', re.IGNORECASE)
RE_DESCRIPTION = re.compile(r'<div class="show-synopsis"[^>]*>(.*?)</div>', re.DOTALL | re.IGNORECASE)


def clean_text(value):
    #Cleaning text from HTML tags and whitespace
    value = re.sub(r'<.*?>', '', value)
    value = re.sub(r'\s+', ' ', value).strip()
    return html.unescape(value)


def extract_field(pattern, text, cleanup_func=None):
    #Extract field using regex pattern
    m = pattern.search(text)
    if not m:
        return ""
    value = m.group(1).strip()
    value = clean_text(value)
    if cleanup_func:
        value = cleanup_func(value)
    return value


def cleanup_title(title):
    #Remove '- MyDramaList' from title
    return re.sub(r'\s*-\s*MyDramaList\s*$', '', title, flags=re.IGNORECASE)


def extract_genres(html_text):
    #Extract genres
    m = RE_GENRES.search(html_text)
    if not m:
        return ""
    raw = m.group(1)
    genres = [g.strip().strip('"') for g in raw.split(",")]
    return ", ".join(genres)


def extract_actors(html_text):
    #Extract actors
    m = RE_ACTORS.search(html_text)
    if not m:
        return ""
    raw = m.group(1)
    names = re.findall(r'"name"\s*:\s*"(.*?)"', raw)
    return ", ".join(html.unescape(name) for name in names)


def extract_description(html_text):
    #Extract description
    m = RE_DESCRIPTION.search(html_text)
    if not m:
        return ""
    desc = m.group(1)
    desc = re.sub(r'<.*?>', '', desc)
    desc = html.unescape(desc)
    desc = re.sub(r'\s+', ' ', desc).strip()
    return desc


def extract_page(file_tuple):
    #Extract all information from HTML page
    doc_id, file_path = file_tuple

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            html_text = f.read()

        title = extract_field(RE_TITLE, html_text, cleanup_func=cleanup_title)
        year = extract_field(RE_YEAR, html_text)
        genres = extract_genres(html_text)
        publisher = extract_field(RE_PUBLISHER, html_text)
        actors = extract_actors(html_text)
        episodes = extract_field(RE_EPISODES, html_text)
        duration = extract_field(RE_DURATION, html_text)
        director = extract_field(RE_DIRECTOR, html_text)
        screenwriter = extract_field(RE_SCREENWRITER, html_text)
        score = extract_field(RE_SCORE, html_text)
        description = extract_description(html_text)

        return (doc_id, [title, year, genres, publisher, actors, episodes,
                         duration, director, screenwriter, score, description])

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        return (doc_id, [""] * 11)


def format_tsv_line(record):
    #Format extracted data into TSV line
    doc_id, fields = record
    all_fields = [str(doc_id)] + fields
    return "\t".join(all_fields)


# Initialize Spark
conf = SparkConf().setAppName("MyDramaList HTML Extractor")
sc = SparkContext(conf=conf)

# Get list of HTML files
files = [os.path.join(PAGES_DIR, f) for f in os.listdir(PAGES_DIR)
         if f.endswith(".html")]
files.sort()
print(f"Found {len(files)} HTML files in '{PAGES_DIR}'")

# Create RDD from files with doc_id
files_rdd = sc.parallelize(enumerate(files))

# Extract data using map transformation
extracted_rdd = files_rdd.map(extract_page)

# Format extracted data into TSV lines
tsv_rdd = extracted_rdd.map(format_tsv_line)

# Save RDD to file 
temp_output = OUTPUT_FILE + "_temp"
tsv_rdd.saveAsTextFile(temp_output)

# TSV file header
header = "doc_id\ttitle\tyear\tgenres\tpublisher\tactors\tepisodes\tduration\tdirector\tscreenwriter\tscore\tdescription"

# Merge parts into single file with header
part_files = sorted(glob.glob(os.path.join(temp_output, "part-*")))

with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
    out.write(header + "\n")
    for part_file in part_files:
        with open(part_file, "r", encoding="utf-8") as pf:
            out.write(pf.read())

# Delete temporary files
shutil.rmtree(temp_output)

# Print statistics
total_count = files_rdd.count()
print(f"\nExtraction complete. Processed {total_count} files.")
print(f"Saved to {OUTPUT_FILE}")

# Print sample extracted data
sample = extracted_rdd.take(3)
print("\nSample extracted data:")
for doc_id, fields in sample:
    title = fields[0] or "Unknown title"
    year = fields[1] or "Unknown year"
    print(f"  [{doc_id + 1}] {title}, {year}")

sc.stop()